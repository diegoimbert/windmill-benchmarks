#!/usr/bin/env python3
"""Analyze Airflow DAG run timing for the NYC taxi ETL benchmark.

Uses the Airflow REST API to fetch per-task timing data.

Usage:
    python analyze_airflow.py <dag_run_id> \
        --airflow-url http://localhost:8080 \
        --user airflow --password airflow
"""

import argparse
import json

import requests
from datetime import datetime


DAG_ID = "nyc_taxi_etl"


def fetch_task_instances(dag_run_id: str, base_url: str, user: str, password: str):
    url = f"{base_url}/api/v1/dags/{DAG_ID}/dagRuns/{dag_run_id}/taskInstances"
    resp = requests.get(url, auth=(user, password))
    resp.raise_for_status()
    return resp.json()["task_instances"]


def build_timing_report(dag_run_id: str, task_instances: list):
    # Sort tasks by start_date to get execution order
    tasks = sorted(task_instances, key=lambda t: t["start_date"] or "")

    # Use the earliest queued time as reference
    ref_time = None
    for t in tasks:
        if t.get("queued_when"):
            dt = datetime.fromisoformat(t["queued_when"].replace("Z", "+00:00"))
            if ref_time is None or dt < ref_time:
                ref_time = dt

    steps = []
    for t in tasks:
        queued = datetime.fromisoformat(t["queued_when"].replace("Z", "+00:00"))
        started = datetime.fromisoformat(t["start_date"].replace("Z", "+00:00"))
        ended = datetime.fromisoformat(t["end_date"].replace("Z", "+00:00"))

        queue_s = (started - queued).total_seconds()
        execution_s = (ended - started).total_seconds()
        started_rel = (started - ref_time).total_seconds()
        completed_rel = (ended - ref_time).total_seconds()

        steps.append(
            {
                "name": t["task_id"],
                "queue_seconds": round(queue_s, 3),
                "execution_seconds": round(execution_s, 3),
                "started_at_relative": round(started_rel, 3),
                "completed_at_relative": round(completed_rel, 3),
            }
        )

    total_wall = steps[-1]["completed_at_relative"] if steps else 0

    return {
        "platform": "airflow_snowflake",
        "run_id": dag_run_id,
        "total_wall_clock_seconds": round(total_wall, 3),
        "steps": steps,
    }


def main():
    parser = argparse.ArgumentParser(description="Analyze Airflow ETL DAG timing")
    parser.add_argument("dag_run_id", help="Airflow DAG run ID")
    parser.add_argument(
        "--airflow-url", default="http://localhost:8080", help="Airflow webserver URL"
    )
    parser.add_argument("--user", default="airflow", help="Airflow API user")
    parser.add_argument("--password", default="airflow", help="Airflow API password")
    parser.add_argument(
        "-o", "--output", default=None, help="Output JSON file (default: stdout)"
    )
    args = parser.parse_args()

    tasks = fetch_task_instances(
        args.dag_run_id, args.airflow_url, args.user, args.password
    )
    report = build_timing_report(args.dag_run_id, tasks)

    output = json.dumps(report, indent=2)
    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        print(f"Written to {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()
