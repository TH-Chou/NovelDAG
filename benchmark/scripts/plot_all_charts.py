#!/usr/bin/env python3
"""
Comprehensive chart generation from rtt_sweep_results.csv (180 configs).
Generates ~15 chart types covering all analytical dimensions.
"""

import csv
import sys
import statistics
import numpy as np
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.colors import LinearSegmentedColormap
from mpl_toolkits.axes_grid1 import make_axes_locatable

CSV_PATH = Path("/Users/apple/Documents/NovelDAG/benchmark/csv_plots/rtt_sweep_results.csv")
OUT_DIR = Path("/Users/apple/Documents/NovelDAG/benchmark/rtt_plots")
OUT_DIR.mkdir(exist_ok=True)

# ── Load data ──────────────────────────────────────────────────
raw = defaultdict(list)
with open(CSV_PATH) as f:
    reader = csv.DictReader(f)
    for row in reader:
        if row["run"] == "mean":
            continue
        key = (row["protocol"], int(row["faults"]), int(row["delay_ms"]), int(row["rate"]))
        raw[key].append((float(row["tps"]), float(row["latency_ms"])))

data = {}  # (proto, faults, delay, rate) -> {"tps": mean, "lat": mean, "tps_std": ..., "lat_std": ...}
for key, vals in raw.items():
    proto, faults, delay, rate = key
    tps_vals = sorted([v for vv in vals for v in [vv[0]] if v > 0])
    lat_vals = sorted([v for vv in vals for v in [vv[1]] if v > 0])
    if len(tps_vals) >= 3:
        mid_tps = tps_vals[1:4] if len(tps_vals) >= 5 else tps_vals
        mid_lat = lat_vals[1:4] if len(lat_vals) >= 5 else lat_vals
        data[key] = {
            "tps": statistics.mean(mid_tps),
            "lat": statistics.mean(mid_lat),
            "tps_std": statistics.stdev(mid_tps) if len(mid_tps) > 1 else 0,
            "lat_std": statistics.stdev(mid_lat) if len(mid_lat) > 1 else 0,
            "all_tps": [v[0] for v in vals],
            "all_lat": [v[1] for v in vals],
        }

PROTOS = ["narwhal", "noveldag", "wahoo"]
HIGHLIGHTED = "noveldag"  # the protocol we showcase; pairwise charts compute its advantage over each entry in COMPARED
COMPARED = ["narwhal", "wahoo"]  # opponents (one panel row each)
FAULTS = [0, 1, 3]
DELAYS = [0, 50, 100]
RATES = list(range(60000, 331000, 30000))

# Color palette - distinct per (proto, fault)
PROTO_COLORS = {
    ("narwhal", 0): "#FF5722",
    ("narwhal", 1): "#FF9800",
    ("narwhal", 3): "#FFC107",
    ("noveldag", 0): "#2196F3",
    ("noveldag", 1): "#1976D2",
    ("noveldag", 3): "#0D47A1",
    ("wahoo", 0): "#9C27B0",
    ("wahoo", 1): "#7B1FA2",
    ("wahoo", 3): "#4A148C",
}
DELAY_COLORS = {0: "#4CAF50", 50: "#FF9800", 100: "#F44336"}
MARKERS = {"narwhal": "s", "noveldag": "o", "wahoo": "^"}
FAULT_MARKERS = {0: "o", 1: "s", 3: "D"}

# ── Helper ─────────────────────────────────────────────────────
def get_val(proto, faults, delay, rate, field="tps"):
    k = (proto, faults, delay, rate)
    return data[k][field] if k in data else None

def skip_missing(x_vals, y_vals):
    return zip(*[(x, y) for x, y in zip(x_vals, y_vals) if y is not None]) if x_vals and y_vals else ([], [])


# ═══════════════════════════════════════════════════════════════
# CHART 1: Fixed TPS-Latency scatter (no duplicate colors)
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
for col, faults in enumerate(FAULTS):
    ax = axes[col]
    for proto in PROTOS:
        for delay in DELAYS:
            x_vals, y_vals = [], []
            for rate in RATES:
                v = get_val(proto, faults, delay, rate, "tps")
                l = get_val(proto, faults, delay, rate, "lat")
                if v and l:
                    x_vals.append(v)
                    y_vals.append(l)
            if x_vals:
                ls = "-" if delay == 0 else "--" if delay == 50 else ":"
                ax.plot(x_vals, y_vals, marker=MARKERS[proto], linestyle=ls,
                        color=PROTO_COLORS[(proto, faults)], markersize=6, linewidth=1.8,
                        alpha=0.85, label=f"{proto} d={delay}ms",
                        markerfacecolor="white" if delay > 0 else PROTO_COLORS[(proto, faults)])
    ax.set_xlabel("Consensus TPS (tx/s)", fontsize=10)
    ax.set_ylabel("Consensus Latency (ms)", fontsize=10)
    ax.set_title(f"f = {faults}", fontsize=12, fontweight="bold")
    ax.legend(fontsize=6.5, ncol=2, loc="upper left", framealpha=0.8)
    ax.grid(True, alpha=0.2)
fig.suptitle("Three protocols: TPS–Latency by Fault Tolerance (n=10)",
             fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "01_tps_vs_latency.png", dpi=150, bbox_inches="tight")
plt.close()
print("1/15: tps_vs_latency")


# ═══════════════════════════════════════════════════════════════
# CHART 2: TPS Advantage Heatmap — NovelDAG vs each opponent, one row per opponent
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(len(COMPARED), 3, figsize=(20, 5.5 * len(COMPARED)),
                          sharey=True)
if len(COMPARED) == 1:
    axes = np.array([axes])
vmax = 80
cmap_white_green = LinearSegmentedColormap.from_list("WhiteGreen",
    [(0, "white"), (0.3, "#c8e6c9"), (0.6, "#66bb6a"), (1, "#1b5e20")], N=256)

for row, opp in enumerate(COMPARED):
    for col, delay in enumerate(DELAYS):
        ax = axes[row][col]
        data_matrix = np.full((len(FAULTS), len(RATES)), np.nan)
        annot_matrix = [["" for _ in RATES] for _ in FAULTS]

        for i, faults in enumerate(FAULTS):
            for j, rate in enumerate(RATES):
                h_tps = get_val(HIGHLIGHTED, faults, delay, rate, "tps")
                o_tps = get_val(opp, faults, delay, rate, "tps")
                if h_tps and o_tps and o_tps > 0:
                    pct = (h_tps - o_tps) / o_tps * 100
                    data_matrix[i, j] = pct
                    annot_matrix[i][j] = "0%" if abs(pct) < 0.5 else f"{pct:+.0f}%"

        masked = np.ma.masked_invalid(data_matrix)
        im = ax.imshow(masked, cmap=cmap_white_green, aspect="auto", vmin=0, vmax=vmax)
        for i in range(len(FAULTS)):
            for j in range(len(RATES)):
                if annot_matrix[i][j]:
                    ax.text(j, i, annot_matrix[i][j], ha="center", va="center",
                            fontsize=9, fontweight="bold", color="black")
        ax.set_xticks(range(len(RATES)))
        ax.set_xticklabels([f"{r//1000}" for r in RATES], fontsize=8, rotation=45)
        ax.set_yticks(range(len(FAULTS)))
        ax.set_yticklabels([f"f={f}" for f in FAULTS], fontsize=10)
        if row == len(COMPARED) - 1:
            ax.set_xlabel("Injection Rate (K tx/s)", fontsize=10)
        title = f"{HIGHLIGHTED} vs {opp}, d={delay}ms ({delay*2}ms RTT)"
        ax.set_title(title, fontsize=11, fontweight="bold")

fig.subplots_adjust(bottom=0.10, top=0.92)
cbar_ax = fig.add_axes([0.25, 0.03, 0.5, 0.018])
cbar = fig.colorbar(im, cax=cbar_ax, orientation="horizontal")
cbar.set_label(f"{HIGHLIGHTED} TPS Advantage over opponent (%)  →  Green = {HIGHLIGHTED} wins",
               fontsize=9)
fig.suptitle(f"{HIGHLIGHTED} TPS Advantage over narwhal / wahoo (n=10)",
             fontsize=14, fontweight="bold", y=0.97)
fig.savefig(OUT_DIR / "02_heatmap_advantage.png", dpi=150, bbox_inches="tight")
plt.close()
print("2/15: heatmap_advantage")


# ═══════════════════════════════════════════════════════════════
# CHART 3: Small multiples - TPS vs Rate (9 subplots)
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(3, 3, figsize=(16, 13), sharex=True, sharey=False)
for row, faults in enumerate(FAULTS):
    for col, delay in enumerate(DELAYS):
        ax = axes[row][col]
        for proto in PROTOS:
            x_vals, y_vals = [], []
            for rate in RATES:
                v = get_val(proto, faults, delay, rate, "tps")
                if v:
                    x_vals.append(rate // 1000)
                    y_vals.append(v)
            if x_vals:
                ax.plot(x_vals, y_vals, marker=MARKERS[proto], linestyle="-",
                        color=PROTO_COLORS[(proto, faults)], linewidth=2, markersize=5,
                        label=proto)
        ax.set_title(f"f={faults}, delay={delay}ms", fontsize=10)
        ax.grid(True, alpha=0.2)
        ax.legend(fontsize=7)
        if row == 2:
            ax.set_xlabel("Injection Rate (K tx/s)", fontsize=9)
        if col == 0:
            ax.set_ylabel("Consensus TPS (tx/s)", fontsize=9)
fig.suptitle("Injection Rate vs Consensus Throughput — All Conditions",
             fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "03_small_multiples_tps.png", dpi=150, bbox_inches="tight")
plt.close()
print("3/15: small_multiples_tps")


# ═══════════════════════════════════════════════════════════════
# CHART 4: Latency penalty — TPS degradation from delay=0
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
for col, faults in enumerate(FAULTS):
    ax = axes[col]
    x_labels = []
    bar_positions = []
    for proto_idx, proto in enumerate(PROTOS):
        for delay_idx, delay in enumerate([50, 100]):
            degs = []
            for rate in RATES:
                base_tps = get_val(proto, faults, 0, rate, "tps")
                del_tps = get_val(proto, faults, delay, rate, "tps")
                if base_tps and del_tps and base_tps > 0:
                    degs.append((base_tps - del_tps) / base_tps * 100)
            if degs:
                mean_deg = statistics.mean(degs)
                pos = len(bar_positions)
                bar_positions.append(pos)
                x_labels.append(f"{proto}\nd{delay}")
                color = PROTO_COLORS[(proto, faults)]
                alpha = 0.5 if delay == 50 else 0.9
                ax.bar(pos, mean_deg, color=color, alpha=alpha, edgecolor="white",
                       label=f"{proto} d={delay}ms" if col == 0 else "")

    ax.set_xticks(bar_positions)
    ax.set_xticklabels(x_labels, fontsize=8)
    ax.set_ylabel("Avg TPS Loss vs delay=0 (%)", fontsize=10)
    ax.set_title(f"f = {faults}", fontsize=12, fontweight="bold")
    ax.grid(True, alpha=0.2, axis="y")
    if col == 0:
        ax.legend(fontsize=7)
fig.suptitle("TPS Degradation When Adding Network Delay", fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "04_latency_penalty_bars.png", dpi=150, bbox_inches="tight")
plt.close()
print("4/15: latency_penalty_bars")


# ═══════════════════════════════════════════════════════════════
# CHART 5: Bubble chart — TPS vs Latency, bubble size = rate
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
for col, delay in enumerate(DELAYS):
    ax = axes[col]
    for proto in PROTOS:
        for faults in FAULTS:
            x_vals, y_vals, sizes = [], [], []
            for rate in RATES:
                v = get_val(proto, faults, delay, rate, "tps")
                l = get_val(proto, faults, delay, rate, "lat")
                if v and l:
                    x_vals.append(v)
                    y_vals.append(l)
                    sizes.append(rate / 1000 * 0.5)
            if x_vals:
                ax.scatter(x_vals, y_vals, s=sizes, alpha=0.5,
                          color=PROTO_COLORS[(proto, faults)],
                          marker=MARKERS[proto], edgecolors="black", linewidth=0.3,
                          label=f"{proto} f={faults}" if col == 0 else "")
    ax.set_xlabel("TPS (tx/s)", fontsize=10)
    ax.set_ylabel("Latency (ms)", fontsize=10)
    ax.set_title(f"delay = {delay}ms ({delay*2}ms RTT)", fontsize=11, fontweight="bold")
    ax.grid(True, alpha=0.2)
    if col == 0:
        ax.legend(fontsize=6, ncol=2, loc="upper left")
fig.suptitle("Bubble Chart: TPS vs Latency (bubble size = injection rate)",
             fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "05_bubble_tps_latency.png", dpi=150, bbox_inches="tight")
plt.close()
print("5/15: bubble_tps_latency")


# ═══════════════════════════════════════════════════════════════
# CHART 6: Protocol efficiency — TPS / Injection Rate
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
for col, faults in enumerate(FAULTS):
    ax = axes[col]
    for proto in PROTOS:
        for delay in DELAYS:
            x_vals, y_vals = [], []
            for rate in RATES:
                v = get_val(proto, faults, delay, rate, "tps")
                if v:
                    x_vals.append(rate // 1000)
                    y_vals.append(v / rate * 100)
            if x_vals:
                ls = "-" if delay == 0 else "--" if delay == 50 else ":"
                ax.plot(x_vals, y_vals, marker=MARKERS[proto], linestyle=ls,
                        color=PROTO_COLORS[(proto, faults)], linewidth=1.8, markersize=5,
                        label=f"{proto} d={delay}")
    ax.axhline(y=100, color="gray", linestyle=":", alpha=0.5)
    ax.set_xlabel("Injection Rate (K tx/s)", fontsize=10)
    ax.set_ylabel("Efficiency (TPS / Rate %)", fontsize=10)
    ax.set_title(f"f = {faults}", fontsize=12, fontweight="bold")
    ax.set_ylim(0, 105)
    ax.legend(fontsize=6.5, ncol=2)
    ax.grid(True, alpha=0.2)
fig.suptitle("Protocol Efficiency: TPS as % of Injection Rate", fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "06_efficiency_pct.png", dpi=150, bbox_inches="tight")
plt.close()
print("6/15: efficiency_pct")


# ═══════════════════════════════════════════════════════════════
# CHART 7: Latency CDF-style — latency distribution across rates
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
for col, delay in enumerate(DELAYS):
    ax = axes[col]
    for proto in PROTOS:
        all_lat = []
        for faults in FAULTS:
            for rate in RATES:
                l = get_val(proto, faults, delay, rate, "lat")
                if l:
                    all_lat.append(l)
        if all_lat:
            sorted_lat = sorted(all_lat)
            cum = np.linspace(0, 100, len(sorted_lat))
            ax.plot(sorted_lat, cum, color=PROTO_COLORS.get((proto, 0), "#333"),
                    linewidth=2.5, label=proto, drawstyle="steps-post")
    ax.set_xlabel("Latency (ms)", fontsize=10)
    ax.set_ylabel("Cumulative % of Configurations", fontsize=10)
    ax.set_title(f"delay = {delay}ms ({delay*2}ms RTT)", fontsize=11, fontweight="bold")
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.2)
fig.suptitle("Latency Distribution: % of Configurations Below Threshold",
             fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "07_latency_distribution.png", dpi=150, bbox_inches="tight")
plt.close()
print("7/15: latency_distribution")


# ═══════════════════════════════════════════════════════════════
# CHART 8: Throughput-delay sensitivity (slope chart)
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
for col, faults in enumerate(FAULTS):
    ax = axes[col]
    for proto in PROTOS:
        slopes = []
        x_labels = []
        for rate in RATES:
            t0 = get_val(proto, faults, 0, rate, "tps")
            t100 = get_val(proto, faults, 100, rate, "tps")
            if t0 and t100 and t0 > 0:
                slopes.append((t0 - t100) / t0 * 100)
                x_labels.append(f"{rate//1000}")
        if slopes:
            ax.plot(range(len(slopes)), slopes, marker=MARKERS[proto],
                    color=PROTO_COLORS[(proto, faults)], linewidth=2, markersize=7,
                    label=proto)
    ax.set_xticks(range(len(x_labels)))
    ax.set_xticklabels(x_labels, fontsize=7, rotation=45)
    ax.set_ylabel("TPS Loss: 0ms→100ms delay (%)", fontsize=10)
    ax.set_xlabel("Injection Rate (K tx/s)", fontsize=9)
    ax.set_title(f"f = {faults}", fontsize=12, fontweight="bold")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.2)
fig.suptitle("Throughput Sensitivity to 100ms Delay by Injection Rate",
             fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "08_delay_sensitivity.png", dpi=150, bbox_inches="tight")
plt.close()
print("8/15: delay_sensitivity")


# ═══════════════════════════════════════════════════════════════
# CHART 9: Radar chart — multi-dimensional comparison at 180K
# ═══════════════════════════════════════════════════════════════
RATE_FOCUS = 180000
fig, axes = plt.subplots(1, 3, figsize=(18, 6), subplot_kw=dict(polar=True))
categories = ["TPS\n(higher=better)", "Efficiency\n(higher=better)", "Latency\n(lower=better)",
              "Stability\n(higher=better)", "Peak TPS\nratio"]

for col, delay in enumerate(DELAYS):
    ax = axes[col]
    angles = np.linspace(0, 2 * np.pi, len(categories), endpoint=False).tolist()
    angles += angles[:1]

    for i, faults in enumerate(FAULTS):
        per_proto_tps = {p: (get_val(p, faults, delay, RATE_FOCUS, "tps") or 0)
                         for p in PROTOS}
        per_proto_lat = {p: (get_val(p, faults, delay, RATE_FOCUS, "lat") or 10000)
                         for p in PROTOS}
        max_tps = max(max(per_proto_tps.values()), 1)
        min_lat = max(min(per_proto_lat.values()), 1)

        for p in PROTOS:
            tps = per_proto_tps[p]
            lat = per_proto_lat[p]
            std = get_val(p, faults, delay, RATE_FOCUS, "tps_std") or 0
            scores = [
                tps / max_tps * 100,
                tps / RATE_FOCUS * 100,
                min_lat / max(lat, 1) * 100,
                100 - (abs(std) / max(tps, 1) * 100),
                tps / max_tps * 100,
            ]
            vals = scores + scores[:1]
            color = PROTO_COLORS[(p, faults)]
            ls = {"narwhal": "-", "noveldag": "--", "wahoo": ":"}.get(p, "-")
            mk = MARKERS[p]
            ax.fill(angles, vals, alpha=0.08, color=color)
            ax.plot(angles, vals, marker=mk, linestyle=ls, linewidth=1.4,
                    markersize=3.5, color=color, label=f"{p} f={faults}")

    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(categories, fontsize=7)
    ax.set_title(f"delay={delay}ms", fontsize=11, fontweight="bold", pad=20)
    if col == 0:
        ax.legend(fontsize=5.0, loc="upper right", bbox_to_anchor=(1.45, 1.15))

fig.suptitle(f"Multi-Dimensional Comparison @ {RATE_FOCUS//1000}K Injection Rate",
             fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "09_radar_comparison.png", dpi=150, bbox_inches="tight")
plt.close()
print("9/15: radar_comparison")


# ═══════════════════════════════════════════════════════════════
# CHART 10: Latency advantage heatmap — one row per compared proto
# Allow negative values (red) so wahoo's slow-path penalty under f>=1
# is visible alongside the f=0 advantage.
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(len(COMPARED), 3, figsize=(20, 5.5 * len(COMPARED)),
                          sharey=True)
if len(COMPARED) == 1:
    axes = np.array([axes])
cmap_diverging = LinearSegmentedColormap.from_list("RedWhiteGreen",
    [(0, "#b71c1c"), (0.4, "#ffcdd2"), (0.5, "white"),
     (0.6, "#c8e6c9"), (1, "#1b5e20")], N=256)
for row, opp in enumerate(COMPARED):
    for col, delay in enumerate(DELAYS):
        ax = axes[row][col]
        data_matrix = np.full((len(FAULTS), len(RATES)), np.nan)
        for i, faults in enumerate(FAULTS):
            for j, rate in enumerate(RATES):
                h_lat = get_val(HIGHLIGHTED, faults, delay, rate, "lat")
                o_lat = get_val(opp, faults, delay, rate, "lat")
                if h_lat and o_lat and o_lat > 0:
                    # positive = HIGHLIGHTED has LOWER latency (wins)
                    data_matrix[i, j] = (o_lat - h_lat) / o_lat * 100

        masked = np.ma.masked_invalid(data_matrix)
        im = ax.imshow(masked, cmap=cmap_diverging, aspect="auto", vmin=-100, vmax=100)
        for i in range(len(FAULTS)):
            for j in range(len(RATES)):
                if not np.isnan(data_matrix[i, j]):
                    val = data_matrix[i, j]
                    label = "0%" if abs(val) < 0.5 else f"{val:+.0f}%"
                    ax.text(j, i, label, ha="center", va="center",
                            fontsize=9, fontweight="bold", color="black")
        ax.set_xticks(range(len(RATES)))
        ax.set_xticklabels([f"{r//1000}" for r in RATES], fontsize=8, rotation=45)
        ax.set_yticks(range(len(FAULTS)))
        ax.set_yticklabels([f"f={f}" for f in FAULTS], fontsize=10)
        if row == len(COMPARED) - 1:
            ax.set_xlabel("Injection Rate (K tx/s)", fontsize=10)
        ax.set_title(f"{HIGHLIGHTED} vs {opp}, d={delay}ms ({delay*2}ms RTT)",
                     fontsize=11, fontweight="bold")

fig.subplots_adjust(bottom=0.10, top=0.92)
cbar_ax = fig.add_axes([0.25, 0.03, 0.5, 0.018])
cbar2 = fig.colorbar(im, cax=cbar_ax, orientation="horizontal")
cbar2.set_label(f"{HIGHLIGHTED} Latency Advantage over opponent (%)  →  Green = lower (wins), Red = higher",
               fontsize=9)
fig.suptitle(f"{HIGHLIGHTED} Latency Advantage over narwhal / wahoo (n=10)",
             fontsize=14, fontweight="bold", y=0.97)
fig.savefig(OUT_DIR / "10_heatmap_latency_advantage.png", dpi=150, bbox_inches="tight")
plt.close()
print("10/15: heatmap_latency_advantage")


# ═══════════════════════════════════════════════════════════════
# CHART 11: Peak TPS comparison bar chart (3 protocols)
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
bar_width = 0.25
for col, delay in enumerate(DELAYS):
    ax = axes[col]
    for f_idx, faults in enumerate(FAULTS):
        peaks = {p: max((get_val(p, faults, delay, r, "tps") or 0) for r in RATES)
                 for p in PROTOS}
        x_base = f_idx * 3
        offsets = {p: (i - (len(PROTOS) - 1) / 2) * bar_width
                   for i, p in enumerate(PROTOS)}
        for p in PROTOS:
            ax.bar(x_base + offsets[p], peaks[p], bar_width,
                   color=PROTO_COLORS[(p, faults)], edgecolor="white",
                   label=p if f_idx == 0 else "")
        # Annotate opponents with % showing how much HIGHLIGHTED beats them
        h_peak = peaks.get(HIGHLIGHTED, 0)
        top = max(peaks.values()) if peaks else 0
        for p in COMPARED:
            o_peak = peaks.get(p, 0)
            if o_peak <= 0:
                continue
            pct = (h_peak - o_peak) / o_peak * 100
            color = "#388E3C" if pct >= 0 else "#D32F2F"
            ax.text(x_base + offsets[p], o_peak + top * 0.02,
                    f"{HIGHLIGHTED[:2]}{pct:+.0f}%", ha="center",
                    fontsize=7, fontweight="bold", color=color)

    ax.set_xticks([f_idx * 3 for f_idx in range(len(FAULTS))])
    ax.set_xticklabels([f"f={f}" for f in FAULTS], fontsize=10)
    ax.set_ylabel("Peak TPS (tx/s)", fontsize=10)
    ax.set_title(f"delay = {delay}ms", fontsize=12, fontweight="bold")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.2, axis="y")
fig.suptitle(f"Peak Consensus TPS by Protocol ({HIGHLIGHTED}’s % advantage over each opponent)",
             fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "11_peak_tps_bars.png", dpi=150, bbox_inches="tight")
plt.close()
print("11/15: peak_tps_bars")


# ═══════════════════════════════════════════════════════════════
# CHART 12: TPS advantage vs Rate — solid lines per compared proto,
# linestyle per fault. Each delay gets its own panel.
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
FAULT_LS = {0: "-", 1: "--", 3: ":"}
for col, delay in enumerate(DELAYS):
    ax = axes[col]
    for opp in COMPARED:
        for faults in FAULTS:
            x_vals, y_vals = [], []
            for rate in RATES:
                h_tps = get_val(HIGHLIGHTED, faults, delay, rate, "tps")
                o_tps = get_val(opp, faults, delay, rate, "tps")
                if h_tps and o_tps and o_tps > 0:
                    x_vals.append(rate // 1000)
                    y_vals.append((h_tps - o_tps) / o_tps * 100)
            if x_vals:
                ax.plot(x_vals, y_vals,
                        marker=MARKERS[opp], linestyle=FAULT_LS[faults],
                        color=PROTO_COLORS[(opp, faults)],
                        linewidth=1.8, markersize=6,
                        label=f"vs {opp} f={faults}")
    ax.axhline(y=0, color="gray", linestyle=":", alpha=0.5)
    ax.set_xlabel("Injection Rate (K tx/s)", fontsize=10)
    ax.set_ylabel(f"{HIGHLIGHTED} TPS Advantage (%)", fontsize=10)
    ax.set_title(f"delay = {delay}ms ({delay*2}ms RTT)", fontsize=11, fontweight="bold")
    ax.legend(fontsize=7, ncol=2)
    ax.grid(True, alpha=0.2)
fig.suptitle(f"{HIGHLIGHTED} TPS Advantage over narwhal / wahoo by Rate and Fault Count",
             fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "12_tps_advantage_curves.png", dpi=150, bbox_inches="tight")
plt.close()
print("12/15: tps_advantage_curves")


# ═══════════════════════════════════════════════════════════════
# CHART 13: TPS contour map (rate × delay), one panel per protocol
# for each shown fault count.
# ═══════════════════════════════════════════════════════════════
proto_cmaps = {"narwhal": "Oranges", "noveldag": "Blues", "wahoo": "Purples"}
focus_faults = [0, 3]
fig, axes = plt.subplots(len(focus_faults), len(PROTOS),
                          figsize=(5 * len(PROTOS), 4.5 * len(focus_faults)),
                          sharey=True)
if len(focus_faults) == 1:
    axes = np.array([axes])
if len(PROTOS) == 1:
    axes = axes.reshape(-1, 1)
for row, faults in enumerate(focus_faults):
    for col, proto in enumerate(PROTOS):
        ax = axes[row][col]
        delays_u = sorted(DELAYS)
        rates_u = sorted([r // 1000 for r in RATES])
        z_grid = np.full((len(rates_u), len(delays_u)), np.nan)
        for i, d in enumerate(delays_u):
            for j, r in enumerate(rates_u):
                v = get_val(proto, faults, d, r * 1000, "tps")
                if v:
                    z_grid[j, i] = v
        if not np.all(np.isnan(z_grid)):
            masked = np.ma.masked_invalid(z_grid)
            cs = ax.contourf(delays_u, rates_u, masked, levels=15, alpha=0.85,
                             cmap=proto_cmaps.get(proto, "viridis"))
            ax.contour(delays_u, rates_u, masked, levels=8, colors="black",
                       linewidths=0.5, alpha=0.4)
            fig.colorbar(cs, ax=ax, fraction=0.04, pad=0.02).set_label(
                "TPS", fontsize=8)
        if row == len(focus_faults) - 1:
            ax.set_xlabel("One-way Delay (ms)", fontsize=9)
        if col == 0:
            ax.set_ylabel("Injection Rate (K tx/s)", fontsize=9)
        ax.set_title(f"{proto}, f={faults}", fontsize=11, fontweight="bold")
fig.suptitle("TPS Contours: Rate × Delay per Protocol",
             fontsize=13, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "13_tps_contours.png", dpi=150, bbox_inches="tight")
plt.close()
print("13/15: tps_contours")


# ═══════════════════════════════════════════════════════════════
# CHART 14: Latency vs Delay (how latency grows with RTT)
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
for col, faults in enumerate(FAULTS):
    ax = axes[col]
    for proto in PROTOS:
        mean_lats = []
        for delay in DELAYS:
            lats = [get_val(proto, faults, delay, r, "lat") for r in RATES]
            lats = [l for l in lats if l]
            mean_lats.append(statistics.mean(lats) if lats else 0)
        ax.plot(DELAYS, mean_lats, marker=MARKERS[proto], linestyle="-",
                color=PROTO_COLORS[(proto, faults)], linewidth=2.5, markersize=10,
                label=proto)
        # Annotate with values
        for d, l in zip(DELAYS, mean_lats):
            if l:
                ax.annotate(f"{l:.0f}ms", (d, l), textcoords="offset points",
                           xytext=(0, 12), ha="center", fontsize=8, fontweight="bold")

    ax.set_xlabel("One-way Delay (ms)", fontsize=10)
    ax.set_ylabel("Mean Latency (ms)", fontsize=10)
    ax.set_title(f"f = {faults}", fontsize=12, fontweight="bold")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.2)
fig.suptitle("Average Consensus Latency vs Network Delay", fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "14_latency_vs_delay.png", dpi=150, bbox_inches="tight")
plt.close()
print("14/15: latency_vs_delay")


# ═══════════════════════════════════════════════════════════════
# CHART 15: Box plot — TPS distribution across runs (all rates)
# Three boxes per fault group, one per protocol.
# ═══════════════════════════════════════════════════════════════
fig, axes = plt.subplots(1, 3, figsize=(18, 5.5))
group_width = 0.7
box_w = group_width / max(len(PROTOS), 1)
for col, delay in enumerate(DELAYS):
    ax = axes[col]
    all_data = {p: [] for p in PROTOS}
    positions = {p: [] for p in PROTOS}
    for f_idx, faults in enumerate(FAULTS):
        center = f_idx * 2.5
        for i, p in enumerate(PROTOS):
            vals = []
            for rate in RATES:
                k = (p, faults, delay, rate)
                if k in data:
                    vals.extend(data[k]["all_tps"])
            if vals:
                all_data[p].append(vals)
                positions[p].append(
                    center + (i - (len(PROTOS) - 1) / 2) * box_w)

    for p in PROTOS:
        if all_data[p]:
            ax.boxplot(
                all_data[p], positions=positions[p], widths=box_w * 0.85,
                patch_artist=True,
                boxprops=dict(facecolor=PROTO_COLORS[(p, 0)], alpha=0.35),
                medianprops=dict(color="black", linewidth=1.5),
                flierprops=dict(marker="o", markersize=3, alpha=0.4),
            )

    ax.set_xticks([f_idx * 2.5 for f_idx in range(len(FAULTS))])
    ax.set_xticklabels([f"f={f}" for f in FAULTS], fontsize=10)
    ax.set_ylabel("TPS (tx/s)", fontsize=10)
    ax.set_title(f"delay = {delay}ms", fontsize=12, fontweight="bold")
    from matplotlib.patches import Patch
    legend_elements = [
        Patch(facecolor=PROTO_COLORS[(p, 0)], alpha=0.35, label=p)
        for p in PROTOS
    ]
    ax.legend(handles=legend_elements, fontsize=8)
    ax.grid(True, alpha=0.2, axis="y")
fig.suptitle("TPS Distribution Across All Runs (n=5 per config)",
             fontsize=14, fontweight="bold")
plt.tight_layout()
fig.savefig(OUT_DIR / "15_boxplot_tps_distribution.png", dpi=150, bbox_inches="tight")
plt.close()
print("15/15: boxplot_tps_distribution")

print(f"\nAll 15 charts saved to {OUT_DIR}/")
