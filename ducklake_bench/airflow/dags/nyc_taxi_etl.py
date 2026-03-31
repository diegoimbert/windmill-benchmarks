from datetime import datetime

from airflow import DAG
from airflow.decorators import task

SNOWFLAKE_CONN_ID = "snowflake_default"


def _run_sql(sql: str):
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()
    cur.execute("USE DATABASE BENCHMARK")
    cur.execute("USE SCHEMA PUBLIC")
    cur.execute(sql)
    cur.close()
    conn.close()


with DAG(
    dag_id="nyc_taxi_etl",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["benchmark", "ducklake-bench"],
) as dag:

    # Step 1: Download parquet, PUT to internal stage, COPY INTO table
    @task(task_id="ingest")
    def ingest():
        import requests as req
        import tempfile
        import os
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
        tmp_dir = tempfile.mkdtemp()
        local_path = os.path.join(tmp_dir, "yellow_tripdata_2024-01.parquet")
        with req.get(url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("USE DATABASE BENCHMARK")
        cur.execute("USE SCHEMA PUBLIC")
        cur.execute("CREATE OR REPLACE FILE FORMAT parquet_fmt TYPE = PARQUET")
        cur.execute("CREATE OR REPLACE STAGE nyc_taxi_stage FILE_FORMAT = parquet_fmt")
        cur.execute(f"PUT 'file://{local_path}' @nyc_taxi_stage AUTO_COMPRESS=FALSE")
        cur.execute("""
            CREATE OR REPLACE TABLE raw_trips AS
            SELECT
              $1:VendorID::INT AS VendorID,
              $1:tpep_pickup_datetime::TIMESTAMP AS tpep_pickup_datetime,
              $1:tpep_dropoff_datetime::TIMESTAMP AS tpep_dropoff_datetime,
              $1:passenger_count::INT AS passenger_count,
              $1:trip_distance::FLOAT AS trip_distance,
              $1:RatecodeID::INT AS RatecodeID,
              $1:store_and_fwd_flag::STRING AS store_and_fwd_flag,
              $1:PULocationID::INT AS PULocationID,
              $1:DOLocationID::INT AS DOLocationID,
              $1:payment_type::INT AS payment_type,
              $1:fare_amount::FLOAT AS fare_amount,
              $1:extra::FLOAT AS extra,
              $1:mta_tax::FLOAT AS mta_tax,
              $1:tip_amount::FLOAT AS tip_amount,
              $1:tolls_amount::FLOAT AS tolls_amount,
              $1:improvement_surcharge::FLOAT AS improvement_surcharge,
              $1:total_amount::FLOAT AS total_amount,
              $1:congestion_surcharge::FLOAT AS congestion_surcharge,
              $1:Airport_fee::FLOAT AS Airport_fee
            FROM @nyc_taxi_stage/yellow_tripdata_2024-01.parquet
            (FILE_FORMAT => 'parquet_fmt')
        """)
        cur.execute("REMOVE @nyc_taxi_stage")
        cur.close()
        conn.close()
        os.remove(local_path)
        os.rmdir(tmp_dir)

    # Step 2: Clean invalid rows
    @task(task_id="clean")
    def clean():
        _run_sql("""
            CREATE OR REPLACE TABLE clean_trips AS
            SELECT *
            FROM raw_trips
            WHERE passenger_count > 0
              AND fare_amount >= 0
              AND trip_distance > 0
              AND PULocationID IS NOT NULL
              AND DOLocationID IS NOT NULL
        """)

    # Step 3: Enrich with computed columns
    @task(task_id="enrich")
    def enrich():
        _run_sql("""
            CREATE OR REPLACE TABLE enriched_trips AS
            SELECT
              *,
              DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration_minutes,
              CASE
                WHEN DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) > 0
                THEN trip_distance / (DATEDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0)
                ELSE NULL
              END AS speed_mph,
              CASE
                WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 0 AND 5 THEN 'night'
                WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 6 AND 11 THEN 'morning'
                WHEN EXTRACT(HOUR FROM tpep_pickup_datetime) BETWEEN 12 AND 17 THEN 'afternoon'
                ELSE 'evening'
              END AS time_of_day_bucket,
              CASE
                WHEN DAYOFWEEK(tpep_pickup_datetime) IN (0, 6) THEN true
                ELSE false
              END AS is_weekend
            FROM clean_trips
        """)

    # Step 4: Aggregate by hour of day
    @task(task_id="aggregate_hourly")
    def aggregate_hourly():
        _run_sql("""
            CREATE OR REPLACE TABLE hourly_stats AS
            SELECT
              EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour_of_day,
              COUNT(*) AS trip_count,
              AVG(fare_amount) AS avg_fare,
              AVG(trip_distance) AS avg_distance,
              AVG(trip_duration_minutes) AS avg_duration_minutes
            FROM enriched_trips
            GROUP BY EXTRACT(HOUR FROM tpep_pickup_datetime)
            ORDER BY hour_of_day
        """)

    # Step 5: Aggregate by pickup zone
    @task(task_id="aggregate_by_zone")
    def aggregate_by_zone():
        _run_sql("""
            CREATE OR REPLACE TABLE zone_stats AS
            SELECT
              PULocationID AS pickup_location_id,
              COUNT(*) AS trip_count,
              AVG(fare_amount) AS avg_fare,
              AVG(tip_amount / NULLIF(fare_amount, 0)) * 100 AS avg_tip_pct
            FROM enriched_trips
            GROUP BY PULocationID
            ORDER BY trip_count DESC
        """)

    # Step 6: Finalize - verify row counts
    @task(task_id="finalize")
    def finalize():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute("USE DATABASE BENCHMARK")
        cur.execute("USE SCHEMA PUBLIC")
        tables = ["raw_trips", "clean_trips", "enriched_trips", "hourly_stats", "zone_stats"]
        counts = {}
        for table in tables:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            result = cur.fetchone()
            counts[table] = result[0]
        cur.close()
        conn.close()
        return counts

    ingest() >> clean() >> enrich() >> aggregate_hourly() >> aggregate_by_zone() >> finalize()
