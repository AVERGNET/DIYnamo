#!/usr/bin/env python3
"""Generate latency heatmaps from eval_quorum_samples.csv.

Usage:
    pip install pandas matplotlib numpy
    python eval/plot_quorum.py [path/to/eval_quorum_samples.csv]

Outputs (next to the CSV file):
    eval_quorum_put_latency.pdf / .png
    eval_quorum_get_latency.pdf / .png
    eval_quorum_write_success.pdf / .png
"""

import sys
import pathlib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# Must match tests/eval_quorum.rs
MEASURE_SECS = 10
N = 5
NODE_COUNT = 9
CONCURRENCY = 48
CLUSTER_LABEL = f"{NODE_COUNT} nodes · N={N} · healthy · concurrency={CONCURRENCY}"

CSV_PATH = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "eval_quorum_samples.csv")
OUT_DIR = CSV_PATH.parent

df = pd.read_csv(CSV_PATH)
success_df = df[df["success"] == 1]

w_vals = list(range(1, N + 1))
r_vals = list(range(1, N + 1))


def percentile(series, p):
    if len(series) == 0:
        return np.nan
    return float(np.percentile(series, p))


def build_heatmap(op, stat_fn):
    grid = np.full((len(w_vals), len(r_vals)), np.nan)
    for i, w in enumerate(w_vals):
        for j, r in enumerate(r_vals):
            lat = success_df[(success_df["w"] == w) & (success_df["r"] == r) & (success_df["op"] == op)][
                "latency_us"
            ] / 1_000.0
            if len(lat):
                grid[i, j] = stat_fn(lat)
    return grid


def plot_heatmap(op, stat_name, stat_fn, cmap, fname_stem):
    grid = build_heatmap(op, stat_fn)
    fig, ax = plt.subplots(figsize=(7, 6))
    im = ax.imshow(grid, cmap=cmap, aspect="auto", origin="lower")
    ax.set_xticks(range(len(r_vals)), labels=[str(r) for r in r_vals])
    ax.set_yticks(range(len(w_vals)), labels=[str(w) for w in w_vals])
    ax.set_xlabel("R (read quorum)")
    ax.set_ylabel("W (write quorum)")
    ax.set_title(f"DIYnamo — {op.upper()} {stat_name}\n({CLUSTER_LABEL})")
    for i, w in enumerate(w_vals):
        for j, r in enumerate(r_vals):
            val = grid[i, j]
            if not np.isnan(val):
                ax.text(j, i, f"{val:.2f}", ha="center", va="center", fontsize=9, color="white")
            elif w + r > N:
                ax.text(j, i, "·", ha="center", va="center", fontsize=9, color="lightgray")
            else:
                ax.text(j, i, "—", ha="center", va="center", fontsize=9, color="gray")
    fig.colorbar(im, ax=ax, label=f"{stat_name} (ms)")
    fig.tight_layout()
    for ext in ("pdf", "png"):
        p = OUT_DIR / f"{fname_stem}.{ext}"
        fig.savefig(p, dpi=150)
        print(f"Saved {p}")
    plt.close(fig)


plot_heatmap("put", "p50 latency", lambda s: percentile(s, 50), "Blues", "eval_quorum_put_latency")
plot_heatmap("get", "p50 latency", lambda s: percentile(s, 50), "Oranges", "eval_quorum_get_latency")

# Write success rate heatmap (PUT only).
put_df = df[df["op"] == "put"]
grid = np.full((len(w_vals), len(r_vals)), np.nan)
for i, w in enumerate(w_vals):
    for j, r in enumerate(r_vals):
        subset = put_df[(put_df["w"] == w) & (put_df["r"] == r)]
        if len(subset):
            grid[i, j] = 100.0 * subset["success"].mean()

fig, ax = plt.subplots(figsize=(7, 6))
im = ax.imshow(grid, cmap="RdYlGn", vmin=0, vmax=100, aspect="auto", origin="lower")
ax.set_xticks(range(len(r_vals)), labels=[str(r) for r in r_vals])
ax.set_yticks(range(len(w_vals)), labels=[str(w) for w in w_vals])
ax.set_xlabel("R (read quorum)")
ax.set_ylabel("W (write quorum)")
ax.set_title(f"DIYnamo — PUT write success rate\n({CLUSTER_LABEL})")
for i, w in enumerate(w_vals):
    for j, r in enumerate(r_vals):
        val = grid[i, j]
        if not np.isnan(val):
            ax.text(j, i, f"{val:.0f}%", ha="center", va="center", fontsize=9)
fig.colorbar(im, ax=ax, label="success %")
fig.tight_layout()
for ext in ("pdf", "png"):
    p = OUT_DIR / f"eval_quorum_write_success.{ext}"
    fig.savefig(p, dpi=150)
    print(f"Saved {p}")
plt.close(fig)
