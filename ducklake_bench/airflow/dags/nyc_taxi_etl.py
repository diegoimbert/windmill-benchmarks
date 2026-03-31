from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.operators.sql import SQLExecuteQueryOperator

SNOWFLAKE_CONN_ID = "snowflake_default"

with DAG(
    dag_id="nyc_taxi_etl",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["benchmark", "ducklake-bench"],
) as dag:

    # Step 1: Ingest raw parquet from external stage into staging table
    ingest = SQLExecuteQueryOperator(
        task_id="ingest",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            CREATE OR REPLACE TABLE raw_trips AS
            SELECT *
            FROM @nyc_taxi_stage/yellow_tripdata_2024-01.parquet
            (FILE_FORMAT => (TYPE = PARQUET));
        """,
    )

    # Step 2: Clean invalid rows
    clean = SQLExecuteQueryOperator(
        task_id="clean",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            CREATE OR REPLACE TABLE clean_trips AS
            SELECT *
            FROM raw_trips
            WHERE passenger_count > 0
              AND fare_amount >= 0
              AND trip_distance > 0
              AND PULocationID IS NOT NULL
              AND DOLocationID IS NOT NULL;
        """,
    )

    # Step 3: Enrich with computed columns
    enrich = SQLExecuteQueryOperator(
        task_id="enrich",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
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
            FROM clean_trips;
        """,
    )

    # Step 4: Aggregate by hour of day
    aggregate_hourly = SQLExecuteQueryOperator(
        task_id="aggregate_hourly",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            CREATE OR REPLACE TABLE hourly_stats AS
            SELECT
              EXTRACT(HOUR FROM tpep_pickup_datetime) AS hour_of_day,
              COUNT(*) AS trip_count,
              AVG(fare_amount) AS avg_fare,
              AVG(trip_distance) AS avg_distance,
              AVG(trip_duration_minutes) AS avg_duration_minutes
            FROM enriched_trips
            GROUP BY EXTRACT(HOUR FROM tpep_pickup_datetime)
            ORDER BY hour_of_day;
        """,
    )

    # Step 5: Aggregate by pickup zone
    aggregate_by_zone = SQLExecuteQueryOperator(
        task_id="aggregate_by_zone",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            CREATE OR REPLACE TABLE zone_stats AS
            SELECT
              PULocationID AS pickup_location_id,
              COUNT(*) AS trip_count,
              AVG(fare_amount) AS avg_fare,
              AVG(tip_amount / NULLIF(fare_amount, 0)) * 100 AS avg_tip_pct
            FROM enriched_trips
            GROUP BY PULocationID
            ORDER BY trip_count DESC;
        """,
    )

    # Step 6: Finalize - verify row counts
    @task(task_id="finalize")
    def finalize():
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)

        tables = [
            "raw_trips",
            "clean_trips",
            "enriched_trips",
            "hourly_stats",
            "zone_stats",
        ]
        counts = {}
        for table in tables:
            result = hook.get_first(f"SELECT COUNT(*) FROM {table}")
            counts[table] = result[0]

        return counts

    ingest >> clean >> enrich >> aggregate_hourly >> aggregate_by_zone >> finalize()
