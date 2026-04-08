#!/usr/bin/env python3
"""
Collect timing data from Dagster via the GraphQL API.

Usage:
  python collect_dagster.py --run-id <run_id> [--base-url http://localhost:3000]
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import requests


GRAPHQL_QUERY = """
query RunSteps($runId: ID!) {
  runOrError(runId: $runId) {
    ... on Run {
      runId
      startTime
      endTime
      stepStats {
        stepKey
        startTime
        endTime
        status
      }
    }
    ... on RunNotFoundError {
      message
    }
    ... on PythonError {
      message
    }
  }
}
"""


def classify_stage(step_key: str) -> str:
    key = step_key.lower()
    for prefix, stage in [
        ("ingest", "ingest"),
        ("validate", "validate"),
        ("denorm", "denormalize"),
        ("agg", "aggregate"),
        ("q", "query"),
        ("verify", "verify"),
    ]:
        if key.startswith(prefix):
            return stage
    return "unknown"


def main():
    parser = argparse.ArgumentParser(description="Collect Dagster benchmark timings")
    parser.add_argument("--run-id", required=True, help="Dagster run ID")
    parser.add_argument("--base-url", default=os.environ.get("DAGSTER_BASE_URL", "http://localhost:3000"))
    parser.add_argument("--output", "-o", default=None)
    args = parser.parse_args()

    url = f"{args.base_url}/graphql"
    resp = requests.post(url, json={
        "query": GRAPHQL_QUERY,
        "variables": {"runId": args.run_id},
    })
    if resp.status_code != 200:
        print(f"ERROR: {resp.status_code} {resp.text}", file=sys.stderr)
        sys.exit(1)

    data = resp.json()
    run_data = data.get("data", {}).get("runOrError", {})

    if "runId" not in run_data:
        msg = run_data.get("message", "Unknown error")
        print(f"ERROR: {msg}", file=sys.stderr)
        sys.exit(1)

    run_start = run_data.get("startTime", 0)
    run_end = run_data.get("endTime", 0)
    origin = run_start

    total_wall_clock_s = (run_end - run_start) if (run_start and run_end) else 0.0

    step_stats = run_data.get("stepStats", [])
    if not step_stats:
        print("ERROR: no step stats found", file=sys.stderr)
        sys.exit(1)

    tasks = []
    for step in step_stats:
        step_key = step["stepKey"]
        start_time = step.get("startTime", 0) or 0
        end_time = step.get("endTime", 0) or 0

        started_at = start_time - origin if origin else 0
        completed_at = end_time - origin if origin else 0
        execution_time_s = end_time - start_time if (start_time and end_time) else 0

        tasks.append({
            "id": step_key,
            "stage": classify_stage(step_key),
            "queued_at": 0.0,  # Dagster does not expose per-step queue time
            "started_at": round(started_at, 3),
            "completed_at": round(completed_at, 3),
            "queue_time_s": round(max(started_at, 0), 3),
            "execution_time_s": round(execution_time_s, 3),
        })

    result = {
        "competitor": "dagster",
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
