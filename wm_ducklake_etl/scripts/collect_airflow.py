#!/usr/bin/env python3
"""
Collect timing data from an Airflow DAG run via the REST API.

Usage:
  python collect_airflow.py --dag-id tpcds_etl --run-id <run_id> [--base-url http://localhost:8080]
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import requests


def parse_ts(ts_str):
    """Parse an ISO timestamp string to a UTC datetime."""
    if ts_str is None:
        return None
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def classify_stage(task_id: str) -> str:
    task_lower = task_id.lower()
    for prefix, stage in [
        ("ingest", "ingest"),
        ("validate", "validate"),
        ("denorm", "denormalize"),
        ("agg", "aggregate"),
        ("q", "query"),
        ("verify", "verify"),
    ]:
        if task_lower.startswith(prefix):
            return stage
    return "unknown"


def main():
    parser = argparse.ArgumentParser(description="Collect Airflow benchmark timings")
    parser.add_argument("--dag-id", default="tpcds_etl", help="Airflow DAG ID")
    parser.add_argument("--run-id", required=True, help="DAG run ID")
    parser.add_argument("--base-url", default=os.environ.get("AIRFLOW_BASE_URL", "http://localhost:8080"))
    parser.add_argument("--username", default=os.environ.get("AIRFLOW_USER", "airflow"))
    parser.add_argument("--password", default=os.environ.get("AIRFLOW_PASSWORD", "airflow"))
    parser.add_argument("--output", "-o", default=None)
    args = parser.parse_args()

    session = requests.Session()
    session.auth = (args.username, args.password)

    url = f"{args.base_url}/api/v1/dags/{args.dag_id}/dagRuns/{args.run_id}/taskInstances"
    resp = session.get(url)
    if resp.status_code != 200:
        print(f"ERROR: {resp.status_code} {resp.text}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    task_instances = data.get("task_instances", [])

    if not task_instances:
        print(f"ERROR: no task instances found for {args.dag_id}/{args.run_id}", file=sys.stderr)
        sys.exit(1)

    # Determine the earliest queued time as origin
    all_queued = [parse_ts(t["queued_when"]) for t in task_instances if t.get("queued_when")]
    if not all_queued:
        print("ERROR: no queued timestamps found", file=sys.stderr)
        sys.exit(1)
    origin = min(all_queued)

    # Get wall-clock from DAG run
    run_url = f"{args.base_url}/api/v1/dags/{args.dag_id}/dagRuns/{args.run_id}"
    run_resp = session.get(run_url)
    run_data = run_resp.json() if run_resp.status_code == 200 else {}
    dag_start = parse_ts(run_data.get("start_date"))
    dag_end = parse_ts(run_data.get("end_date"))

    total_wall_clock_s = 0.0
    if dag_start and dag_end:
        total_wall_clock_s = (dag_end - dag_start).total_seconds()

    tasks = []
    for ti in task_instances:
        task_id = ti["task_id"]
        queued_when = parse_ts(ti.get("queued_when"))
        start_date = parse_ts(ti.get("start_date"))
        end_date = parse_ts(ti.get("end_date"))
        duration = ti.get("duration", 0) or 0

        queued_at = (queued_when - origin).total_seconds() if queued_when else 0.0
        started_at = (start_date - origin).total_seconds() if start_date else 0.0
        completed_at = (end_date - origin).total_seconds() if end_date else started_at + duration

        queue_time_s = started_at - queued_at
        execution_time_s = duration if duration else (completed_at - started_at)

        tasks.append({
            "id": task_id,
            "stage": classify_stage(task_id),
            "queued_at": round(queued_at, 3),
            "started_at": round(started_at, 3),
            "completed_at": round(completed_at, 3),
            "queue_time_s": round(max(queue_time_s, 0), 3),
            "execution_time_s": round(execution_time_s, 3),
        })

    result = {
        "competitor": "airflow",
        "timestamp": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "tasks": tasks,
        "total_wall_clock_s": round(total_wall_clock_s, 3),
    }

    output = json.dumps(result, indent=2)
    if args.output:
        with open(args.output, "w") as f:
            f.write(output + "\n")
        print(f"Saved to {args.output}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
