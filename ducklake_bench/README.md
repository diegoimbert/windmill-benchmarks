# Benchmark: Windmill+DuckLake vs Airflow+Snowflake

ETL benchmark comparing **Windmill with DuckLake** against **Airflow with Snowflake** using the NYC yellow taxi dataset (January 2024, ~3M rows).

## ETL Pipeline (6 steps, semantically identical)

| Step | Name | Description |
|------|------|-------------|
| 1 | Ingest | Load raw parquet into `raw_trips` |
| 2 | Clean | Filter invalid rows → `clean_trips` |
| 3 | Enrich | Add computed columns (duration, speed, time bucket, weekend flag) → `enriched_trips` |
| 4 | Aggregate Hourly | Stats by hour of day → `hourly_stats` |
| 5 | Aggregate by Zone | Stats by pickup location → `zone_stats` |
| 6 | Finalize | Verify row counts |

Data source: `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet`

## Running the Windmill+DuckLake Benchmark

```bash
cd windmill
docker compose up -d

# Wait for Windmill to be ready at http://localhost:80
# Complete initial setup (create workspace, etc.)

# Deploy the flow
wmill sync push

# Trigger the flow via UI or API, note the job ID
# Then analyze:
cd ../scripts
pip install -r requirements.txt
python analyze_windmill.py <flow_job_id> \
    --conn-string "postgres://postgres:changeme@localhost:5432/windmill" \
    -o windmill_timing.json
```

## Running the Airflow+Snowflake Benchmark

### Prerequisites

1. Fill in `airflow/.env` with your Snowflake credentials

No Snowflake-side setup required — the ingest step creates an internal stage, downloads the parquet, PUTs it, and COPY INTOs automatically.

```bash
cd airflow
echo "AIRFLOW_UID=$(id -u)" >> .env
docker compose up -d

# Wait for Airflow at http://localhost:8080 (airflow/airflow)
# Unpause and trigger the nyc_taxi_etl DAG, note the run ID
# Then analyze:
cd ../scripts
python analyze_airflow.py <dag_run_id> \
    --airflow-url http://localhost:8080 \
    -o airflow_timing.json
```

## Comparing Results

```bash
cd scripts
python compare.py windmill_timing.json airflow_timing.json -o report.md
```

This produces a markdown report with per-step timing comparison and overall wall-clock delta.

## Timing Measurement

- **Windmill**: Queries the Windmill PostgreSQL database (`v2_as_completed_job` table) for `created_at`, `started_at`, and `duration_ms` per flow step.
- **Airflow**: Uses the Airflow REST API (`GET /api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances`) which returns `queued_when`, `start_date`, `end_date`, and `duration` per task.

Both produce a common JSON format consumed by `compare.py`.
