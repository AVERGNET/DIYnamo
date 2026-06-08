#!/usr/bin/env python3
"""Generate success-rate graphs from eval_failure_summary.csv.

Usage:
    pip install pandas matplotlib
    python eval/plot_failure.py [path/to/eval_failure_summary.csv]

Outputs (next to the CSV):
    eval_failure_alive4.pdf / .png
    eval_failure_alive3.pdf / .png
    eval_failure_alive2.pdf / .png
"""

import sys
import pathlib
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

CLUSTER_LABEL = "5 nodes · N=3 · W=2 · R=2 · spread kills"

CSV_PATH = pathlib.Path(
    sys.argv[1] if len(sys.argv) > 1 else "eval_failure_summary.csv"
)
OUT_DIR = CSV_PATH.parent

df = pd.read_csv(CSV_PATH)

for alive in [4, 3, 2]:
    subset = df[df["alive"] == alive].sort_values("concurrency")
    if subset.empty:
        print(f"Skip alive={alive}: no data")
        continue

    fig, ax = plt.subplots(figsize=(7, 5))
    ax.plot(
        subset["concurrency"],
        subset["put_success_pct"],
        marker="o",
        linewidth=2,
        markersize=7,
        color="steelblue",
        label="PUT",
    )
    ax.plot(
        subset["concurrency"],
        subset["get_success_pct"],
        marker="s",
        linewidth=2,
        markersize=7,
        color="darkorange",
        label="GET",
    )

    dead = 5 - alive
    ax.set_xlabel("Client concurrency")
    ax.set_ylabel("Success rate (%)")
    ax.set_title(
        f"Experiment 2 — Success under failure ({alive} alive, {dead} dead)\n"
        f"({CLUSTER_LABEL})"
    )
    ax.set_ylim(0, 105)
    ax.legend()
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
    ax.set_xticks(sorted(subset["concurrency"].unique()))
    ax.grid(True, alpha=0.35)
    fig.tight_layout()

    for ext in ("pdf", "png"):
        out = OUT_DIR / f"eval_failure_alive{alive}.{ext}"
        fig.savefig(out, dpi=150)
        print(f"Saved {out}")
    plt.close(fig)
