#!/usr/bin/env python3
"""
Collect timing data from dbt's run_results.json.

Usage:
  python collect_dbt.py --results-file /path/to/target/run_results.json
"""

import argparse
import json
import sys
from datetime import datetime, timezone


def parse_ts(ts_str):
    if ts_str is None:
        return None
    dt = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def classify_stage(unique_id: str) -> str:
    name = unique_id.split(".")[-1].lower() if "." in unique_id else unique_id.lower()
    for prefix, stage in [
        ("ingest", "ingest"),
        ("validate", "validate"),
        ("denorm", "denormalize"),
        ("agg", "aggregate"),
        ("q", "query"),
        ("verify", "verify"),
    ]:
        if name.startswith(prefix):
            return stage
    return "unknown"


def main():
    parser = argparse.ArgumentParser(description="Collect dbt benchmark timings")
    parser.add_argument("--results-file", required=True, help="Path to run_results.json")
    parser.add_argument("--output", "-o", default=None)
    args = parser.parse_args()

    with open(args.results_file) as f:
        run_results = json.load(f)

    results = run_results.get("results", [])
    if not results:
        print("ERROR: no results found in run_results.json", file=sys.stderr)
        sys.exit(1)

    # Find the earliest and latest timestamps for wall-clock calculation
    all_times = []
    for r in results:
        for t in r.get("timing", []):
            for key in ("started_at", "completed_at"):
                ts = parse_ts(t.get(key))
                if ts:
                    all_times.append(ts)

    if not all_times:
        print("ERROR: no timing data found", file=sys.stderr)
        sys.exit(1)

    origin = min(all_times)
    end = max(all_times)
    total_wall_clock_s = (end - origin).total_seconds()

    tasks = []
    for r in results:
        unique_id = r.get("unique_id", "unknown")
        timing = r.get("timing", [])

        compile_started = None
        execute_started = None
        execute_completed = None

        for t in timing:
            name = t.get("name", "")
            if name == "compile":
                compile_started = parse_ts(t.get("started_at"))
            elif name == "execute":
                execute_started = parse_ts(t.get("started_at"))
                execute_completed = parse_ts(t.get("completed_at"))

        # Use compile start as queue time, execute start as actual start
        queued_at = (compile_started - origin).total_seconds() if compile_started else 0.0
        started_at = (execute_started - origin).total_seconds() if execute_started else queued_at
        completed_at = (execute_completed - origin).total_seconds() if execute_completed else started_at

        queue_time_s = started_at - queued_at
        execution_time_s = completed_at - started_at

        tasks.append({
            "id": unique_id,
            "stage": classify_stage(unique_id),
            "queued_at": round(queued_at, 3),
            "started_at": round(started_at, 3),
            "completed_at": round(completed_at, 3),
            "queue_time_s": round(max(queue_time_s, 0), 3),
            "execution_time_s": round(max(execution_time_s, 0), 3),
        })

    result = {
        "competitor": "dbt",
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
