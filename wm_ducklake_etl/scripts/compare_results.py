#!/usr/bin/env python3
"""Compare benchmark results across competitors."""

import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd

RESULTS_DIR = Path(__file__).parent.parent / "results"

STAGE_ORDER = ["ingest", "validate", "denormalize", "aggregate", "query", "verify", "unknown"]
STAGE_COLORS = {
    "ingest": "#4CAF50",
    "validate": "#2196F3",
    "denormalize": "#FF9800",
    "aggregate": "#9C27B0",
    "query": "#F44336",
    "verify": "#607D8B",
    "unknown": "#607D8B",
}
COMPETITOR_DISPLAY = {
    "windmill": "Windmill + DuckLake",
    "dagster": "Dagster + DuckDB",
    "snowflake": "Snowflake",
    "airflow": "Airflow + Pandas",
}


def load_results():
    results = {}
    for f in sorted(RESULTS_DIR.glob("*.json")):
        data = json.loads(f.read_text())
        results[data["competitor"]] = data
    return results


def normalize_task_id(task_id: str) -> str:
    """Strip stage prefixes to get comparable task names."""
    parts = task_id.split(".")
    name = parts[-1]
    # strip leading stage prefix like "stage1_ingest." or "queries."
    for prefix in ("ingest_", "validate_", "denorm_", "agg_", "query_", "q"):
        if name.startswith(prefix):
            return name
    return name


def stage_summary(data):
    """Return {stage: {exec_time, wall_time}} for a competitor."""
    stages = {}
    for t in data["tasks"]:
        s = t["stage"]
        if s not in stages:
            stages[s] = {"exec_sum": 0, "wall_start": float("inf"), "wall_end": 0}
        stages[s]["exec_sum"] += t["execution_time_s"]
        stages[s]["wall_start"] = min(stages[s]["wall_start"], t["started_at"])
        stages[s]["wall_end"] = max(stages[s]["wall_end"], t["completed_at"])
    for s in stages:
        stages[s]["wall_time"] = stages[s]["wall_end"] - stages[s]["wall_start"]
    return stages


def print_summary(results):
    print("=" * 80)
    print("BENCHMARK COMPARISON — TPC-DS ETL Pipeline")
    print("=" * 80)

    # Total wall clock
    rows = []
    for comp, data in results.items():
        total = data["total_wall_clock_s"]
        rows.append({
            "Competitor": COMPETITOR_DISPLAY.get(comp, comp),
            "Total (s)": f"{total:.1f}",
            "Total (min)": f"{total / 60:.1f}",
        })
    rows.sort(key=lambda r: float(r["Total (s)"]))
    df = pd.DataFrame(rows)
    print("\n## Total Wall-Clock Time\n")
    print(df.to_string(index=False))

    # Per-stage wall time
    print("\n\n## Per-Stage Wall Time (seconds)\n")
    stage_rows = []
    for comp, data in results.items():
        ss = stage_summary(data)
        row = {"Competitor": COMPETITOR_DISPLAY.get(comp, comp)}
        for stage in STAGE_ORDER:
            if stage in ss:
                row[stage.capitalize()] = f"{ss[stage]['wall_time']:.1f}"
        stage_rows.append(row)
    stage_rows.sort(key=lambda r: r["Competitor"])
    df_stage = pd.DataFrame(stage_rows).fillna("-")
    print(df_stage.to_string(index=False))

    # Per-stage execution time (sum of all tasks)
    print("\n\n## Per-Stage Sum of Execution Time (seconds)\n")
    exec_rows = []
    for comp, data in results.items():
        ss = stage_summary(data)
        row = {"Competitor": COMPETITOR_DISPLAY.get(comp, comp)}
        for stage in STAGE_ORDER:
            if stage in ss:
                row[stage.capitalize()] = f"{ss[stage]['exec_sum']:.1f}"
        exec_rows.append(row)
    exec_rows.sort(key=lambda r: r["Competitor"])
    df_exec = pd.DataFrame(exec_rows).fillna("-")
    print(df_exec.to_string(index=False))


def plot_total_comparison(results, ax):
    competitors = sorted(results.keys(), key=lambda c: results[c]["total_wall_clock_s"])
    names = [COMPETITOR_DISPLAY.get(c, c) for c in competitors]
    totals = [results[c]["total_wall_clock_s"] for c in competitors]

    bars = ax.barh(names, totals, color=["#4CAF50" if c == "windmill" else "#78909C" for c in competitors])
    ax.set_xlabel("Wall-Clock Time (seconds)")
    ax.set_title("Total Pipeline Duration")
    for bar, val in zip(bars, totals):
        ax.text(bar.get_width() + 10, bar.get_y() + bar.get_height() / 2,
                f"{val:.0f}s ({val / 60:.1f}m)", va="center", fontsize=9)
    ax.set_xlim(0, max(totals) * 1.25)


def plot_stage_breakdown(results, ax):
    competitors = sorted(results.keys(), key=lambda c: results[c]["total_wall_clock_s"])
    names = [COMPETITOR_DISPLAY.get(c, c) for c in competitors]
    y = np.arange(len(competitors))

    stage_data = {}
    for stage in STAGE_ORDER:
        vals = []
        for c in competitors:
            ss = stage_summary(results[c])
            vals.append(ss.get(stage, {}).get("wall_time", 0))
        stage_data[stage] = vals

    lefts = np.zeros(len(competitors))
    for stage in STAGE_ORDER:
        vals = np.array(stage_data[stage])
        if vals.sum() == 0:
            continue
        ax.barh(y, vals, left=lefts, color=STAGE_COLORS.get(stage, "#999"),
                label=stage.capitalize(), edgecolor="white", linewidth=0.5)
        lefts += vals

    ax.set_yticks(y)
    ax.set_yticklabels(names)
    ax.set_xlabel("Wall-Clock Time (seconds)")
    ax.set_title("Stage Breakdown (wall time)")
    ax.legend(loc="lower right", fontsize=8)


def plot_gantt(results, ax):
    """Gantt chart for each competitor showing task execution over time."""
    competitors = sorted(results.keys(), key=lambda c: results[c]["total_wall_clock_s"])
    y_offset = 0
    y_ticks = []
    y_labels = []

    for comp in competitors:
        tasks = sorted(results[comp]["tasks"], key=lambda t: t["started_at"])
        name = COMPETITOR_DISPLAY.get(comp, comp)
        y_ticks.append(y_offset + len(tasks) / 2)
        y_labels.append(name)
        for i, t in enumerate(tasks):
            color = STAGE_COLORS.get(t["stage"], "#999")
            ax.barh(y_offset + i, t["execution_time_s"], left=t["started_at"],
                    color=color, height=0.8, edgecolor="none", alpha=0.85)
        y_offset += len(tasks) + 3  # gap between competitors

    ax.set_yticks(y_ticks)
    ax.set_yticklabels(y_labels)
    ax.set_xlabel("Time (seconds)")
    ax.set_title("Task Execution Timeline (Gantt)")

    patches = [mpatches.Patch(color=STAGE_COLORS[s], label=s.capitalize())
               for s in STAGE_ORDER if s in STAGE_COLORS]
    ax.legend(handles=patches, loc="lower right", fontsize=8)


def main():
    results = load_results()
    if not results:
        print("No result files found in", RESULTS_DIR)
        sys.exit(1)

    print_summary(results)

    fig, axes = plt.subplots(3, 1, figsize=(14, 16))
    fig.suptitle("TPC-DS ETL Pipeline Benchmark", fontsize=14, fontweight="bold")

    plot_total_comparison(results, axes[0])
    plot_stage_breakdown(results, axes[1])
    plot_gantt(results, axes[2])

    plt.tight_layout(rect=[0, 0, 1, 0.97])
    out = RESULTS_DIR / "comparison.png"
    plt.savefig(out, dpi=150)
    print(f"\nChart saved to {out}")

    if "--show" in sys.argv:
        plt.show()


if __name__ == "__main__":
    main()
