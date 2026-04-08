#!/usr/bin/env python3
"""
Compare benchmark results across competitors.

Reads all *_result.json files from a directory, computes per-competitor and
per-stage breakdowns, prints a Markdown report, and generates a stacked bar chart.

Usage:
  python compare.py --results-dir ../results/
"""

import argparse
import json
import os
import sys
from collections import defaultdict

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt


def load_results(results_dir):
    """Load all JSON result files from the given directory."""
    results = []
    for fname in sorted(os.listdir(results_dir)):
        if fname.endswith(".json"):
            path = os.path.join(results_dir, fname)
            with open(path) as f:
                data = json.load(f)
            results.append(data)
    return results


def compute_metrics(data):
    """Compute summary metrics for a single competitor result."""
    tasks = data.get("tasks", [])
    total_wall_clock_s = data.get("total_wall_clock_s", 0)
    total_compute = sum(t["execution_time_s"] for t in tasks)
    total_overhead = max(total_wall_clock_s - total_compute, 0)

    # Per-stage breakdown
    stages = defaultdict(lambda: {"compute": 0.0, "count": 0})
    for t in tasks:
        stage = t.get("stage", "unknown")
        stages[stage]["compute"] += t["execution_time_s"]
        stages[stage]["count"] += 1

    return {
        "competitor": data.get("competitor", "unknown"),
        "wall_clock_s": round(total_wall_clock_s, 2),
        "compute_s": round(total_compute, 2),
        "overhead_s": round(total_overhead, 2),
        "task_count": len(tasks),
        "stages": dict(stages),
    }


def generate_report(all_metrics):
    """Generate a Markdown report to stdout."""
    lines = []
    lines.append("# TPC-DS ETL Benchmark Results\n")

    # Summary table
    lines.append("## Summary\n")
    lines.append("| Competitor | Wall Clock (s) | Compute (s) | Overhead (s) | Tasks |")
    lines.append("|------------|---------------:|------------:|-------------:|------:|")
    for m in sorted(all_metrics, key=lambda x: x["wall_clock_s"]):
        lines.append(
            f"| {m['competitor']:10s} | {m['wall_clock_s']:14.2f} | "
            f"{m['compute_s']:11.2f} | {m['overhead_s']:12.2f} | {m['task_count']:5d} |"
        )

    # Per-stage breakdown for each competitor
    lines.append("\n## Per-Stage Breakdown\n")
    stage_order = ["ingest", "validate", "denormalize", "aggregate", "query", "verify", "unknown"]

    for m in sorted(all_metrics, key=lambda x: x["wall_clock_s"]):
        lines.append(f"### {m['competitor']}\n")
        lines.append("| Stage | Compute (s) | Tasks |")
        lines.append("|-------|------------:|------:|")
        for stage in stage_order:
            if stage in m["stages"]:
                s = m["stages"][stage]
                lines.append(f"| {stage:12s} | {s['compute']:11.2f} | {s['count']:5d} |")

    return "\n".join(lines)


def generate_chart(all_metrics, output_path):
    """Generate a stacked bar chart: compute vs overhead."""
    competitors = [m["competitor"] for m in sorted(all_metrics, key=lambda x: x["wall_clock_s"])]
    compute = [m["compute_s"] for m in sorted(all_metrics, key=lambda x: x["wall_clock_s"])]
    overhead = [m["overhead_s"] for m in sorted(all_metrics, key=lambda x: x["wall_clock_s"])]

    fig, ax = plt.subplots(figsize=(10, 6))
    x = range(len(competitors))

    bars_compute = ax.bar(x, compute, label="Compute Time", color="#2196F3")
    bars_overhead = ax.bar(x, overhead, bottom=compute, label="Orchestration Overhead", color="#FF9800")

    ax.set_xlabel("Competitor")
    ax.set_ylabel("Time (seconds)")
    ax.set_title("TPC-DS ETL Benchmark: Compute vs Orchestration Overhead")
    ax.set_xticks(x)
    ax.set_xticklabels(competitors)
    ax.legend()

    # Add value labels
    for i, (c, o) in enumerate(zip(compute, overhead)):
        total = c + o
        ax.text(i, total + total * 0.01, f"{total:.0f}s", ha="center", va="bottom", fontsize=9)

    plt.tight_layout()
    plt.savefig(output_path, dpi=150)
    print(f"Chart saved to {output_path}", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Compare benchmark results")
    parser.add_argument("--results-dir", default="results", help="Directory containing result JSON files")
    parser.add_argument("--chart", default=None, help="Output path for PNG chart (default: <results-dir>/comparison.png)")
    args = parser.parse_args()

    if not os.path.isdir(args.results_dir):
        print(f"ERROR: {args.results_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    results = load_results(args.results_dir)
    if not results:
        print(f"ERROR: no JSON result files found in {args.results_dir}", file=sys.stderr)
        sys.exit(1)

    all_metrics = [compute_metrics(r) for r in results]

    report = generate_report(all_metrics)
    print(report)

    chart_path = args.chart or os.path.join(args.results_dir, "comparison.png")
    generate_chart(all_metrics, chart_path)


if __name__ == "__main__":
    main()
