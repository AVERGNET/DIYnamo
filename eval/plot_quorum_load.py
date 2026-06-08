#!/usr/bin/env python3
"""Generate quorum-load comparison graphs from eval_quorum_load_summary.csv.

Usage:
    pip install pandas matplotlib
    python eval/plot_quorum_load.py [path/to/eval_quorum_load_summary.csv]

Outputs (next to the CSV):
    eval_quorum_load_success.pdf / .png
    eval_quorum_load_p99.pdf / .png
"""

import sys
import pathlib
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

CLUSTER_LABEL = "5 nodes · N=3 · healthy cluster"
LOOSE = (2, 2)
STRICT = (3, 3)

CSV_PATH = pathlib.Path(
    sys.argv[1] if len(sys.argv) > 1 else "eval_quorum_load_summary.csv"
)
OUT_DIR = CSV_PATH.parent

df = pd.read_csv(CSV_PATH)


def subset(w, r, op):
    return df[(df["w"] == w) & (df["r"] == r) & (df["op"] == op)].sort_values(
        "concurrency"
    )


def conc_ticks(frame):
    return sorted(frame["concurrency"].unique())


def plot_metric(metric_col, ylabel, title_suffix, stem):
    fig, axes = plt.subplots(1, 2, figsize=(12, 5), sharey=True)

    for ax, op, color_loose, color_strict in [
        (axes[0], "put", "steelblue", "navy"),
        (axes[1], "get", "darkorange", "saddlebrown"),
    ]:
        loose = subset(*LOOSE, op)
        strict = subset(*STRICT, op)
        if not loose.empty:
            ax.plot(
                loose["concurrency"],
                loose[metric_col],
                marker="o",
                linewidth=2,
                markersize=7,
                color=color_loose,
                label="W=2, R=2 (loose)",
            )
        if not strict.empty:
            ax.plot(
                strict["concurrency"],
                strict[metric_col],
                marker="s",
                linewidth=2,
                markersize=7,
                color=color_strict,
                label="W=3, R=3 (strict)",
            )
        ax.set_xlabel("Client concurrency")
        ax.set_ylabel(ylabel)
        ax.set_title(f"{op.upper()} {title_suffix}")
        ax.legend()
        ax.set_xscale("log", base=2)
        ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
        ticks = conc_ticks(loose if not loose.empty else strict)
        if ticks:
            ax.set_xticks(ticks)
        ax.grid(True, alpha=0.35)

    fig.suptitle(f"Experiment 2b — Quorum strictness under load\n({CLUSTER_LABEL})")
    fig.tight_layout()
    for ext in ("pdf", "png"):
        out = OUT_DIR / f"{stem}.{ext}"
        fig.savefig(out, dpi=150)
        print(f"Saved {out}")
    plt.close(fig)


plot_metric("success_pct", "Success rate (%)", "success vs concurrency", "eval_quorum_load_success")
plot_metric("p99_ms", "p99 latency (ms)", "p99 latency vs concurrency", "eval_quorum_load_p99")
