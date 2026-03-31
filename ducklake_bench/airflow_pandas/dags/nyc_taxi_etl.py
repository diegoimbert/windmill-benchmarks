from datetime import datetime

import pandas as pd
from airflow import DAG
from airflow.decorators import task

DATA_DIR = "/opt/airflow/data"

with DAG(
    dag_id="nyc_taxi_etl",
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["benchmark", "ducklake-bench"],
) as dag:

    # Step 1: Download parquet into local file
    @task(task_id="ingest")
    def ingest():
        url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
        df = pd.read_parquet(url)
        df.to_parquet(f"{DATA_DIR}/raw_trips.parquet", index=False)
        return len(df)

    # Step 2: Clean invalid rows
    @task(task_id="clean")
    def clean():
        df = pd.read_parquet(f"{DATA_DIR}/raw_trips.parquet")
        df = df[
            (df["passenger_count"] > 0)
            & (df["fare_amount"] >= 0)
            & (df["trip_distance"] > 0)
            & (df["PULocationID"].notna())
            & (df["DOLocationID"].notna())
        ]
        df.to_parquet(f"{DATA_DIR}/clean_trips.parquet", index=False)
        return len(df)

    # Step 3: Enrich with computed columns
    @task(task_id="enrich")
    def enrich():
        df = pd.read_parquet(f"{DATA_DIR}/clean_trips.parquet")

        pickup = pd.to_datetime(df["tpep_pickup_datetime"])
        dropoff = pd.to_datetime(df["tpep_dropoff_datetime"])

        df["trip_duration_minutes"] = (dropoff - pickup).dt.total_seconds() / 60.0

        df["speed_mph"] = None
        mask = df["trip_duration_minutes"] > 0
        df.loc[mask, "speed_mph"] = (
            df.loc[mask, "trip_distance"]
            / (df.loc[mask, "trip_duration_minutes"] / 60.0)
        )

        hour = pickup.dt.hour
        df["time_of_day_bucket"] = pd.cut(
            hour,
            bins=[-1, 5, 11, 17, 24],
            labels=["night", "morning", "afternoon", "evening"],
        )

        df["is_weekend"] = pickup.dt.dayofweek >= 5

        df.to_parquet(f"{DATA_DIR}/enriched_trips.parquet", index=False)
        return len(df)

    # Step 4: Aggregate by hour of day
    @task(task_id="aggregate_hourly")
    def aggregate_hourly():
        df = pd.read_parquet(f"{DATA_DIR}/enriched_trips.parquet")

        pickup = pd.to_datetime(df["tpep_pickup_datetime"])
        stats = (
            df.assign(hour_of_day=pickup.dt.hour)
            .groupby("hour_of_day")
            .agg(
                trip_count=("fare_amount", "count"),
                avg_fare=("fare_amount", "mean"),
                avg_distance=("trip_distance", "mean"),
                avg_duration_minutes=("trip_duration_minutes", "mean"),
            )
            .reset_index()
            .sort_values("hour_of_day")
        )
        stats.to_parquet(f"{DATA_DIR}/hourly_stats.parquet", index=False)
        return len(stats)

    # Step 5: Aggregate by pickup zone
    @task(task_id="aggregate_by_zone")
    def aggregate_by_zone():
        df = pd.read_parquet(f"{DATA_DIR}/enriched_trips.parquet")

        df["tip_pct"] = df["tip_amount"] / df["fare_amount"].replace(0, float("nan")) * 100

        stats = (
            df.groupby("PULocationID")
            .agg(
                trip_count=("fare_amount", "count"),
                avg_fare=("fare_amount", "mean"),
                avg_tip_pct=("tip_pct", "mean"),
            )
            .reset_index()
            .rename(columns={"PULocationID": "pickup_location_id"})
            .sort_values("trip_count", ascending=False)
        )
        stats.to_parquet(f"{DATA_DIR}/zone_stats.parquet", index=False)
        return len(stats)

    # Step 6: Finalize - verify row counts
    @task(task_id="finalize")
    def finalize():
        tables = ["raw_trips", "clean_trips", "enriched_trips", "hourly_stats", "zone_stats"]
        counts = {}
        for table in tables:
            df = pd.read_parquet(f"{DATA_DIR}/{table}.parquet")
            counts[table] = len(df)
        return counts

    ingest() >> clean() >> enrich() >> aggregate_hourly() >> aggregate_by_zone() >> finalize()
