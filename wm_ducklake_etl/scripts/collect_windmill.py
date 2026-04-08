#!/usr/bin/env python3
"""
Collect timing data from Windmill's postgres database for a completed flow job.

Usage:
  python collect_windmill.py --job-id <flow_uuid> [--pg-host localhost] [--pg-port 5432]
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import psycopg2


STAGE_MAP = {
    "ingest": "ingest",
    "validate": "validate",
    "denorm": "denormalize",
    "agg": "aggregate",
    "q": "query",
    "verify": "verify",
}


def classify_stage(task_id: str) -> str:
    for prefix, stage in STAGE_MAP.items():
        if task_id.startswith(prefix):
            return stage
    return "unknown"


def main():
    parser = argparse.ArgumentParser(description="Collect Windmill benchmark timings")
    parser.add_argument("--job-id", required=True, help="Flow job UUID")
    parser.add_argument("--pg-host", default=os.environ.get("WM_PG_HOST", "localhost"))
    parser.add_argument("--pg-port", type=int, default=int(os.environ.get("WM_PG_PORT", "5432")))
    parser.add_argument("--pg-db", default=os.environ.get("WM_PG_DB", "windmill"))
    parser.add_argument("--pg-user", default=os.environ.get("WM_PG_USER", "windmill"))
    parser.add_argument("--pg-password", default=os.environ.get("WM_PG_PASSWORD", "windmill"))
    parser.add_argument("--output", "-o", default=None, help="Output JSON file (default: stdout)")
    args = parser.parse_args()

    conn = psycopg2.connect(
        host=args.pg_host,
        port=args.pg_port,
        dbname=args.pg_db,
        user=args.pg_user,
        password=args.pg_password,
    )

    # Query completed jobs that belong to the given flow
    query = """
        SELECT id, created_at, started_at, duration_ms
        FROM v2_as_completed_job
        WHERE root_job = %s
          AND job_kind != 'flow'
        ORDER BY created_at
    """

    with conn.cursor() as cur:
        cur.execute(query, (args.job_id,))
        rows = cur.fetchall()

    if not rows:
        print(f"ERROR: no tasks found for job {args.job_id}", file=sys.stderr)
        sys.exit(1)

    # Get flow-level timestamps for wall-clock calculation
    with conn.cursor() as cur:
        cur.execute(
            "SELECT created_at, started_at, duration_ms FROM v2_as_completed_job WHERE id = %s",
            (args.job_id,),
        )
        flow_row = cur.fetchone()

    conn.close()

    if flow_row is None:
        print(f"ERROR: flow job {args.job_id} not found", file=sys.stderr)
        sys.exit(1)

    flow_created, flow_started, flow_duration_ms = flow_row
    flow_origin = flow_created.replace(tzinfo=timezone.utc)
    total_wall_clock_s = flow_duration_ms / 1000.0

    tasks = []
    for row in rows:
        job_id, created_at, started_at, duration_ms = row
        created_at = created_at.replace(tzinfo=timezone.utc)
        started_at = started_at.replace(tzinfo=timezone.utc)

        queued_at = (created_at - flow_origin).total_seconds()
        started_rel = (started_at - flow_origin).total_seconds()
        execution_time_s = duration_ms / 1000.0
        completed_rel = started_rel + execution_time_s
        queue_time_s = started_rel - queued_at

        task_id = str(job_id)
        tasks.append({
            "id": task_id,
            "stage": classify_stage(task_id),
            "queued_at": round(queued_at, 3),
            "started_at": round(started_rel, 3),
            "completed_at": round(completed_rel, 3),
            "queue_time_s": round(queue_time_s, 3),
            "execution_time_s": round(execution_time_s, 3),
        })

    result = {
        "competitor": "windmill",
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
