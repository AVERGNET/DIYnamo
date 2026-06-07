#!/usr/bin/env python3
"""Generate recovery-time plots from eval_recovery_3a.csv and eval_recovery_3b.csv.

Usage:
    pip install pandas matplotlib
    python eval/plot_recovery.py [directory]

Outputs in the given directory (default: workspace root):
    eval_recovery_3a.pdf / .png
    eval_recovery_3b.pdf / .png
"""

import sys
import pathlib
import pandas as pd
import matplotlib.pyplot as plt

OUT_DIR = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else ".")
CLUSTER_LABEL = "5 nodes · N=3 · W=2 · R=2"


def plot_3a():
    path = OUT_DIR / "eval_recovery_3a.csv"
    if not path.exists():
        print(f"Skip 3a: {path} not found")
        return
    df = pd.read_csv(path)
    fig, ax = plt.subplots(figsize=(7, 5))
    ok = df[df["success"] == 1]
    ax.plot(
        ok["hint_count"],
        ok["recovery_ms"] / 1000.0,
        marker="o",
        linewidth=2,
        markersize=8,
        color="steelblue",
        label="hint delivery",
    )
    ax.set_xlabel("Hints stored during outage")
    ax.set_ylabel("Recovery time (s)")
    ax.set_title(f"Experiment 3a — Hint recovery (data intact)\n({CLUSTER_LABEL})")
    ax.grid(True, alpha=0.35)
    ax.legend()
    fig.tight_layout()
    for ext in ("pdf", "png"):
        out = OUT_DIR / f"eval_recovery_3a.{ext}"
        fig.savefig(out, dpi=150)
        print(f"Saved {out}")
    plt.close(fig)


def plot_3b():
    path = OUT_DIR / "eval_recovery_3b.csv"
    if not path.exists():
        print(f"Skip 3b: {path} not found")
        return
    df = pd.read_csv(path)
    fig, ax = plt.subplots(figsize=(7, 5))
    ok = df[df["success"] == 1]
    ax.plot(
        ok["key_count"],
        ok["recovery_ms"] / 1000.0,
        marker="s",
        linewidth=2,
        markersize=8,
        color="darkorange",
        label="reconciliation",
    )
    ax.set_xlabel("Keys to migrate")
    ax.set_ylabel("Recovery time (s)")
    ax.set_title(f"Experiment 3b — Reconciliation (data loss)\n({CLUSTER_LABEL})")
    # ax.set_xscale("log", base=10)
    ax.grid(True, alpha=0.35)
    ax.legend()
    fig.tight_layout()
    for ext in ("pdf", "png"):
        out = OUT_DIR / f"eval_recovery_3b.{ext}"
        fig.savefig(out, dpi=150)
        print(f"Saved {out}")
    plt.close(fig)


plot_3a()
plot_3b()
