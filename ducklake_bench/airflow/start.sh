#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

if [ ! -f .env ]; then
  echo "ERROR: .env file not found."
  echo "Copy .env.example to .env and fill in your Snowflake credentials:"
  echo "  cp .env.example .env"
  exit 1
fi

echo "Starting Airflow (LocalExecutor + Postgres)…"
docker compose up airflow-init --build
docker compose up -d airflow-webserver airflow-scheduler

echo ""
echo "Airflow is starting up at http://localhost:8080"
echo "  Username: admin"
echo "  Password: admin"
echo ""
echo "Trigger the DAG 'nyc_taxi_etl_airflow_snowflake' from the UI or run:"
echo "  docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_etl_airflow_snowflake"
