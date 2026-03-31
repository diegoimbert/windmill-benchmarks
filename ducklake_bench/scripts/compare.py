#!/usr/bin/env python3
"""Compare Windmill+DuckLake vs Airflow+Snowflake timing reports.

Reads two JSON timing files and produces a markdown comparison table.

Usage:
    python compare.py windmill_timing.json airflow_timing.json [-o report.md]
"""

import argparse
import json
import sys


def load_report(path: str) -> dict:
    with open(path) as f:
        return json.load(f)


def generate_comparison(wm: dict, af: dict) -> str:
    lines = []
    lines.append("# ETL Benchmark: Windmill+DuckLake vs Airflow+Snowflake\n")

    lines.append("## Summary\n")
    lines.append(f"| Metric | Windmill+DuckLake | Airflow+Snowflake |")
    lines.append(f"|--------|------------------:|------------------:|")
    lines.append(
        f"| **Total wall clock** | {wm['total_wall_clock_seconds']:.1f}s | {af['total_wall_clock_seconds']:.1f}s |"
    )

    wm_queue = sum(s["queue_seconds"] for s in wm["steps"])
    af_queue = sum(s["queue_seconds"] for s in af["steps"])
    wm_exec = sum(s["execution_seconds"] for s in wm["steps"])
    af_exec = sum(s["execution_seconds"] for s in af["steps"])

    lines.append(f"| Total queue/scheduling | {wm_queue:.1f}s | {af_queue:.1f}s |")
    lines.append(f"| Total execution | {wm_exec:.1f}s | {af_exec:.1f}s |")
    lines.append("")

    # Per-step comparison
    lines.append("## Per-Step Breakdown\n")
    lines.append(
        "| Step | WM Queue | WM Exec | AF Queue | AF Exec | Exec Delta |"
    )
    lines.append(
        "|------|--------:|--------:|--------:|--------:|-----------:|"
    )

    wm_steps = {s["name"]: s for s in wm["steps"]}
    af_steps = {s["name"]: s for s in af["steps"]}

    all_names = [s["name"] for s in wm["steps"]]
    for name in all_names:
        ws = wm_steps.get(name, {})
        afs = af_steps.get(name, {})
        wq = ws.get("queue_seconds", 0)
        we = ws.get("execution_seconds", 0)
        aq = afs.get("queue_seconds", 0)
        ae = afs.get("execution_seconds", 0)
        delta = ae - we
        sign = "+" if delta > 0 else ""
        lines.append(
            f"| {name} | {wq:.2f}s | {we:.2f}s | {aq:.2f}s | {ae:.2f}s | {sign}{delta:.2f}s |"
        )

    lines.append("")

    # Winner
    if wm["total_wall_clock_seconds"] < af["total_wall_clock_seconds"]:
        pct = (
            (af["total_wall_clock_seconds"] - wm["total_wall_clock_seconds"])
            / af["total_wall_clock_seconds"]
            * 100
        )
        lines.append(
            f"**Windmill+DuckLake** was {pct:.1f}% faster overall.\n"
        )
    else:
        pct = (
            (wm["total_wall_clock_seconds"] - af["total_wall_clock_seconds"])
            / wm["total_wall_clock_seconds"]
            * 100
        )
        lines.append(
            f"**Airflow+Snowflake** was {pct:.1f}% faster overall.\n"
        )

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(description="Compare ETL benchmark results")
    parser.add_argument("windmill_json", help="Windmill timing JSON file")
    parser.add_argument("airflow_json", help="Airflow timing JSON file")
    parser.add_argument(
        "-o", "--output", default=None, help="Output markdown file (default: stdout)"
    )
    args = parser.parse_args()

    wm = load_report(args.windmill_json)
    af = load_report(args.airflow_json)
    report = generate_comparison(wm, af)

    if args.output:
        with open(args.output, "w") as f:
            f.write(report)
        print(f"Written to {args.output}")
    else:
        print(report)


if __name__ == "__main__":
    main()
