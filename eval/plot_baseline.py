#!/usr/bin/env python3
"""Generate throughput and latency graphs from eval_baseline_samples.csv.

Usage:
    pip install pandas matplotlib numpy
    python eval/plot_baseline.py [path/to/eval_baseline_samples.csv]

Outputs (next to the CSV file, or in the current directory):
    eval_baseline_throughput.pdf / .png
    eval_baseline_latency.pdf    / .png
"""

import sys
import pathlib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

# ---------------------------------------------------------------------------
# Config — must match the Rust constants in tests/eval_baseline.rs
# ---------------------------------------------------------------------------
MEASURE_SECS = 30          # Duration::from_secs(30)
CLUSTER_LABEL = "5 nodes · N=3 · W=2 · R=2 · 3 vnodes"

CSV_PATH = pathlib.Path(sys.argv[1] if len(sys.argv) > 1 else "eval_baseline_samples.csv")
OUT_DIR  = CSV_PATH.parent

# ---------------------------------------------------------------------------
# Load data
# ---------------------------------------------------------------------------
df = pd.read_csv(CSV_PATH)
df["latency_ms"] = df["latency_us"] / 1_000.0
success_df = df[df["success"] == 1]

concurrencies = sorted(df["concurrency"].unique())
ops           = ["put", "get"]
colors        = {"put": "steelblue", "get": "darkorange"}
markers       = {"put": "o",         "get": "s"}

# ---------------------------------------------------------------------------
# Figure 1: Throughput vs. concurrency
# ---------------------------------------------------------------------------
fig1, ax1 = plt.subplots(figsize=(7, 4))

for op in ops:
    tput = [
        len(df[(df["op"] == op) & (df["concurrency"] == c)]) / MEASURE_SECS
        for c in concurrencies
    ]
    ax1.plot(concurrencies, tput,
             marker=markers[op], label=op.upper(),
             color=colors[op], linewidth=2, markersize=7)

ax1.set_xlabel("Client concurrency")
ax1.set_ylabel("Throughput (ops / sec)")
ax1.set_title(f"DIYnamo — Baseline Throughput\n({CLUSTER_LABEL})")
ax1.legend()
ax1.set_xscale("log", base=2)
ax1.xaxis.set_major_formatter(ticker.ScalarFormatter())
ax1.set_xticks(concurrencies)
ax1.set_xlim(concurrencies[0] * 0.8, concurrencies[-1] * 1.25)
ax1.grid(True, alpha=0.35)
fig1.tight_layout()

for ext in ("pdf", "png"):
    p = OUT_DIR / f"eval_baseline_throughput.{ext}"
    fig1.savefig(p, dpi=150)
    print(f"Saved {p}")

# ---------------------------------------------------------------------------
# Figure 2: Latency percentiles vs. concurrency (side-by-side: PUT / GET)
# ---------------------------------------------------------------------------
fig2, axes = plt.subplots(1, 2, figsize=(12, 4), sharey=False)

percentile_specs = [
    (50,  "p50",  "green",      "o"),
    (95,  "p95",  "darkorange", "s"),
    (99,  "p99",  "red",        "^"),
]

for ax, op in zip(axes, ops):
    series = {label: [] for _, label, _, _ in percentile_specs}
    for c in concurrencies:
        lat = success_df[(success_df["op"] == op) & (success_df["concurrency"] == c)]["latency_ms"]
        for pct, label, _, _ in percentile_specs:
            series[label].append(np.percentile(lat, pct) if len(lat) else 0)

    for pct, label, color, marker in percentile_specs:
        ax.plot(concurrencies, series[label],
                marker=marker, label=label, color=color,
                linewidth=2, markersize=7)

    ax.set_xlabel("Client concurrency")
    ax.set_ylabel("Latency (ms)")
    ax.set_title(f"{op.upper()} Latency Percentiles")
    ax.legend()
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(ticker.ScalarFormatter())
    ax.set_xticks(concurrencies)
    ax.set_xlim(concurrencies[0] * 0.8, concurrencies[-1] * 1.25)
    ax.grid(True, alpha=0.35)

fig2.suptitle(f"DIYnamo — Baseline Latency\n({CLUSTER_LABEL})")
fig2.tight_layout()

for ext in ("pdf", "png"):
    p = OUT_DIR / f"eval_baseline_latency.{ext}"
    fig2.savefig(p, dpi=150)
    print(f"Saved {p}")

# ---------------------------------------------------------------------------
# Figure 3: CDF of latency at max concurrency (one subplot per op)
# ---------------------------------------------------------------------------
fig3, axes3 = plt.subplots(1, 2, figsize=(12, 4), sharey=True)
max_c = max(concurrencies)

for ax, op in zip(axes3, ops):
    lat = success_df[(success_df["op"] == op) & (success_df["concurrency"] == max_c)]["latency_ms"]
    if len(lat):
        sorted_lat = np.sort(lat)
        cdf = np.arange(1, len(sorted_lat) + 1) / len(sorted_lat)
        ax.plot(sorted_lat, cdf, color=colors[op], linewidth=2)
        ax.axvline(np.percentile(sorted_lat, 50), color="green",      linestyle="--", linewidth=1, label="p50")
        ax.axvline(np.percentile(sorted_lat, 95), color="darkorange", linestyle="--", linewidth=1, label="p95")
        ax.axvline(np.percentile(sorted_lat, 99), color="red",        linestyle="--", linewidth=1, label="p99")
        ax.legend()

    ax.set_xlabel("Latency (ms)")
    ax.set_ylabel("CDF")
    ax.set_title(f"{op.upper()} Latency CDF (concurrency={max_c})")
    ax.grid(True, alpha=0.35)

fig3.suptitle(f"DIYnamo — Latency CDF at max concurrency={max_c}\n({CLUSTER_LABEL})")
fig3.tight_layout()

for ext in ("pdf", "png"):
    p = OUT_DIR / f"eval_baseline_latency_cdf.{ext}"
    fig3.savefig(p, dpi=150)
    print(f"Saved {p}")
