#!/usr/bin/env python3
"""Analyze Windmill flow run timing for the NYC taxi ETL benchmark.

Adapted from competitors/windmill/job_timing_analysis.py.
Queries Windmill's PostgreSQL database for per-step timing data.

Usage:
    python analyze_windmill.py <flow_job_id> \
        --conn-string "postgres://postgres:changeme@localhost:5432/windmill"
"""

import argparse
import json
from datetime import datetime

import psycopg2
from psycopg2.extras import RealDictCursor

STEP_NAMES = [
    "ingest",
    "clean",
    "enrich",
    "aggregate_hourly",
    "aggregate_by_zone",
    "finalize",
]


def query_flow_steps(flow_job_id: str, conn_string: str):
    with psycopg2.connect(conn_string) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Get parent flow job
            cur.execute(
                "SELECT id, created_at, started_at FROM v2_as_completed_job WHERE id = %s",
                (flow_job_id,),
            )
            parent = cur.fetchone()
            if not parent:
                raise ValueError(f"Flow job {flow_job_id} not found")

            # Get child step jobs ordered by creation time
            cur.execute(
                """
                SELECT id, created_at, started_at, duration_ms
                FROM v2_as_completed_job
                WHERE parent_job = %s
                ORDER BY created_at
                """,
                (flow_job_id,),
            )
            steps = cur.fetchall()

    return parent, steps


def build_timing_report(parent, steps):
    ref_time = parent["started_at"]

    total_steps = []
    for i, step in enumerate(steps):
        name = STEP_NAMES[i] if i < len(STEP_NAMES) else f"step_{i}"
        created_rel = (step["created_at"] - ref_time).total_seconds()
        started_rel = (step["started_at"] - ref_time).total_seconds()
        execution_s = step["duration_ms"] / 1000.0
        queue_s = (step["started_at"] - step["created_at"]).total_seconds()

        total_steps.append(
            {
                "name": name,
                "queue_seconds": round(queue_s, 3),
                "execution_seconds": round(execution_s, 3),
                "started_at_relative": round(started_rel, 3),
                "completed_at_relative": round(started_rel + execution_s, 3),
            }
        )

    last = total_steps[-1]
    total_wall = last["completed_at_relative"]

    return {
        "platform": "windmill_ducklake",
        "run_id": str(parent["id"]),
        "total_wall_clock_seconds": round(total_wall, 3),
        "steps": total_steps,
    }


def main():
    parser = argparse.ArgumentParser(description="Analyze Windmill ETL flow timing")
    parser.add_argument("flow_job_id", help="Flow (parent) job ID")
    parser.add_argument(
        "--conn-string",
        required=True,
        help="PostgreSQL connection string for Windmill DB",
    )
    parser.add_argument(
        "-o", "--output", default=None, help="Output JSON file (default: stdout)"
    )
    args = parser.parse_args()

    parent, steps = query_flow_steps(args.flow_job_id, args.conn_string)
    report = build_timing_report(parent, steps)

    output = json.dumps(report, indent=2)
    if args.output:
        with open(args.output, "w") as f:
            f.write(output)
        print(f"Written to {args.output}")
    else:
        print(output)


if __name__ == "__main__":
    main()
