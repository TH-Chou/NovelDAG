#!/usr/bin/env python3
"""
Generate comparison plots from rtt_sweep_results.csv
  1. Throughput vs Latency (scatter, colored by protocol+delay)
  2. Injection Rate vs Throughput (line chart, grouped by protocol)
  3. Latency vs Throughput (scatter, grouped by fault count)
"""

import csv
import sys
import statistics
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

CSV_PATH = Path("/Users/apple/Documents/NovelDAG/benchmark/rtt_sweep_results.csv")
OUT_DIR = Path("/Users/apple/Documents/NovelDAG/benchmark/rtt_plots")
OUT_DIR.mkdir(exist_ok=True)

# ── Load data ──────────────────────────────────────────────────
# Group: (proto, faults, delay_ms, rate) -> [tps], [lat]
raw = defaultdict(lambda: {"tps": [], "lat": []})

with open(CSV_PATH) as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["run"] == "mean":
            continue  # skip aggregate rows
        key = (row["protocol"], int(row["faults"]), int(row["delay_ms"]), int(row["rate"]))
        raw[key]["tps"].append(float(row["tps"]))
        raw[key]["lat"].append(float(row["latency_ms"]))

# Compute stable mean (middle 3 of 5)
data = {}  # (proto, faults, delay, rate) -> {"tps": mean, "lat": mean}
for key, vals in raw.items():
    proto, faults, delay, rate = key
    tps_vals = sorted([v for v in vals["tps"] if v > 0])
    lat_vals = sorted([v for v in vals["lat"] if v > 0])
    if len(tps_vals) >= 3 and len(lat_vals) >= 3:
        mid_tps = tps_vals[1:4] if len(tps_vals) >= 5 else tps_vals
        mid_lat = lat_vals[1:4] if len(lat_vals) >= 5 else lat_vals
        data[key] = {
            "tps": statistics.mean(mid_tps),
            "lat": statistics.mean(mid_lat),
        }

PROTOS = ["narwhal", "noveldag"]
FAULTS = [0, 1, 3]
DELAYS = [0, 50, 100]
RATES = list(range(60000, 331000, 30000))

COLORS = {
    ("narwhal", 0): "#FF5722",
    ("narwhal", 50): "#FF9800",
    ("narwhal", 100): "#FFC107",
    ("noveldag", 0): "#2196F3",
    ("noveldag", 50): "#1976D2",
    ("noveldag", 100): "#0D47A1",
}
MARKERS = {"narwhal": "s", "noveldag": "o"}
LINE_STYLES = {0: "-", 50: "--", 100: ":"}

# ── Figure 1: TPS vs Latency scatter ───────────────────────────
fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
for idx, faults in enumerate(FAULTS):
    ax = axes[idx]
    for proto in PROTOS:
        for delay in DELAYS:
            x, y = [], []
            for rate in RATES:
                key = (proto, faults, delay, rate)
                if key in data:
                    x.append(data[key]["tps"])
                    y.append(data[key]["lat"])
            if x:
                ax.plot(x, y, marker=MARKERS[proto], linestyle=LINE_STYLES.get(delay, "-"),
                        color=COLORS[(proto, delay)], markersize=7, linewidth=1.8,
                        label=f"{proto} {delay}ms",
                        markerfacecolor="white" if delay > 0 else COLORS[(proto, delay)])
    ax.set_xlabel("Consensus TPS (tx/s)", fontsize=10)
    ax.set_ylabel("Consensus Latency (ms)", fontsize=10)
    ax.set_title(f"f = {faults}", fontsize=12, fontweight="bold")
    ax.legend(fontsize=7)
    ax.grid(True, alpha=0.2)
fig.suptitle("NovelDAG vs Narwhal: TPS–Latency Under Network Delay (n=10)",
             fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "tps_vs_latency.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: tps_vs_latency.png")

# ── Figure 2: Injection Rate vs TPS ────────────────────────────
fig, axes = plt.subplots(3, 3, figsize=(18, 15), sharex=True)
for col, delay in enumerate(DELAYS):
    for row, faults in enumerate(FAULTS):
        ax = axes[row][col]
        for proto in PROTOS:
            x, y = [], []
            for rate in RATES:
                key = (proto, faults, delay, rate)
                if key in data:
                    x.append(rate // 1000)
                    y.append(data[key]["tps"])
            if x:
                ax.plot(x, y, marker=MARKERS[proto], linestyle="-", linewidth=2,
                        markersize=6, color=COLORS.get((proto, 0), "#333"),
                        label=proto)
        ax.set_title(f"f={faults}, delay={delay}ms", fontsize=10)
        ax.set_ylabel("TPS (tx/s)", fontsize=9)
        ax.legend(fontsize=7)
        ax.grid(True, alpha=0.2)
        if row == 1:
            ax.set_xlabel("Injection Rate (K tx/s)", fontsize=9)
fig.suptitle("Injection Rate vs Consensus Throughput by Delay and Fault Tolerance",
             fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "rate_vs_tps.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: rate_vs_tps.png")

# ── Figure 3: Latency vs TPS, grouped by f ─────────────────────
fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
for col, delay in enumerate(DELAYS):
    ax = axes[col]
    for faults in FAULTS:
        for proto in PROTOS:
            x, y = [], []
            for rate in RATES:
                key = (proto, faults, delay, rate)
                if key in data:
                    x.append(data[key]["tps"])
                    y.append(data[key]["lat"])
            if x:
                style = MARKERS[proto]
                ax.plot(x, y, marker=style, linestyle="-", linewidth=1.5,
                        markersize=7,
                        color=COLORS.get((proto, delay), COLORS.get((proto, 0), "#333")),
                        alpha=0.8,
                        label=f"{proto} f={faults}")
    ax.set_xlabel("Consensus TPS (tx/s)", fontsize=10)
    ax.set_ylabel("Consensus Latency (ms)", fontsize=10)
    ax.set_title(f"delay = {delay}ms one-way ({delay*2}ms RTT)", fontsize=11, fontweight="bold")
    ax.legend(fontsize=6.5)
    ax.grid(True, alpha=0.2)
fig.suptitle("Latency vs Throughput by Fault Count Under Network Delay (n=10)",
             fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "latency_by_fault.png", dpi=150, bbox_inches="tight")
plt.close()
print("Saved: latency_by_fault.png")

print(f"\nAll plots saved to {OUT_DIR}/")
