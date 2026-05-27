#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BENCH_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${BENCH_DIR}"

CONFIG="scripts/configs/local_rtt_sweep_280k.yaml"
GROUP="local_rtt_280k"
PREFIX="local_rtt_sweep_280k"
RAW_CSV="csv_plots/${PREFIX}_runs.csv"
RTT_CSV="csv_plots/${PREFIX}_runs_with_rtt.csv"
PLOT_DIR="csv_plots/${PREFIX}_latency_vs_rtt"

mkdir -p csv_plots "${PLOT_DIR}"

python3 scripts/run_bench.py \
  --mode local \
  run \
  --config "${CONFIG}" \
  --group "${GROUP}" \
  --output-prefix "${PREFIX}" \
  "$@"

python3 - <<'PY'
from __future__ import annotations

import csv
from pathlib import Path

raw_csv = Path("csv_plots/local_rtt_sweep_280k_runs.csv")
rtt_csv = Path("csv_plots/local_rtt_sweep_280k_runs_with_rtt.csv")
plot_dir = Path("csv_plots/local_rtt_sweep_280k_latency_vs_rtt")
plot_dir.mkdir(parents=True, exist_ok=True)

if not raw_csv.exists():
    raise SystemExit(f"CSV not found: {raw_csv}")

with raw_csv.open() as f:
    rows = list(csv.DictReader(f))

fieldnames = [
    "run",
    "protocol",
    "faults",
    "rtt_ms",
    "one_way_delay_ms",
    "rate",
    "consensus_tps",
    "consensus_latency_ms",
    "end_to_end_tps",
    "end_to_end_latency_ms",
]

with rtt_csv.open("w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        delay = int(float(row["delay_ms"]))
        writer.writerow({
            "run": row["run"],
            "protocol": row["protocol"],
            "faults": row["faults"],
            "rtt_ms": delay * 2,
            "one_way_delay_ms": delay,
            "rate": row["rate"],
            "consensus_tps": row["consensus_tps"],
            "consensus_latency_ms": row["consensus_latency_ms"],
            "end_to_end_tps": row["end_to_end_tps"],
            "end_to_end_latency_ms": row["end_to_end_latency_ms"],
        })

try:
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
except Exception as exc:
    print(f"[WARN] matplotlib unavailable, skipped plots: {exc}")
    print(f"RTT CSV: {rtt_csv}")
    raise SystemExit(0)

colors = {
    "shortfin": "#4fb6b6",
    "narwhal": "#8fb7e8",
    "wahoo": "#e8a7bd",
}
markers = {"shortfin": "o", "narwhal": "s", "wahoo": "D"}
protocols = ["shortfin", "narwhal", "wahoo"]

with rtt_csv.open() as f:
    rtt_rows = list(csv.DictReader(f))

for fault in [0, 1, 3]:
    fig, ax = plt.subplots(figsize=(8, 4))
    for protocol in protocols:
        pts = []
        for row in rtt_rows:
            if int(row["faults"]) == fault and row["protocol"] == protocol:
                latency = float(row["end_to_end_latency_ms"])
                if latency > 0:
                    pts.append((int(row["rtt_ms"]), latency))
        if not pts:
            continue
        by_rtt = {}
        for rtt, latency in pts:
            by_rtt.setdefault(rtt, []).append(latency)
        xs = sorted(by_rtt)
        ys = [sum(by_rtt[x]) / len(by_rtt[x]) for x in xs]
        ax.plot(
            xs,
            ys,
            marker=markers.get(protocol, "o"),
            linewidth=2,
            color=colors.get(protocol),
            label=protocol,
        )
    ax.set_title(f"End-to-end latency vs RTT (n=10, f={fault}, rate=280k)")
    ax.set_xlabel("Injected RTT (ms)")
    ax.set_ylabel("End-to-end latency (ms)")
    ax.set_xticks([0, 100, 200, 300, 400])
    ax.set_ylim(bottom=0)
    ax.grid(True, alpha=0.25)
    ax.legend()
    fig.tight_layout()
    out = plot_dir / f"f{fault}_e2e_latency_vs_rtt.png"
    fig.savefig(out, dpi=180)
    plt.close(fig)
    print(f"Plot: {out}")

print(f"Raw CSV: {raw_csv}")
print(f"RTT CSV: {rtt_csv}")
PY
