#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

echo "Starting Airflow (LocalExecutor + Postgres)…"
docker compose up airflow-init --build
docker compose up -d airflow-webserver airflow-scheduler

echo ""
echo "Airflow is starting up at http://localhost:8080"
echo "  Username: admin"
echo "  Password: admin"
echo ""
echo "Trigger the DAG 'nyc_taxi_etl' from the UI or run:"
echo "  docker compose exec airflow-scheduler airflow dags trigger nyc_taxi_etl"
