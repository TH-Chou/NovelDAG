#!/usr/bin/env python3
"""Plot 50-node GCP end-to-end throughput/latency results.

The script reads existing benchmark CSV files, writes a single normalized
summary CSV, and renders a TPS-latency curve for the four protocols.
"""

from __future__ import annotations

import csv
from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
CSV_DIR = ROOT / "csv_plots"
FIG_DIR = ROOT / "figures"

SUMMARY_CSV = CSV_DIR / "gcp-n50-f0-four-protocols-e2e-summary-20260916.csv"
OUT_PDF = FIG_DIR / "gcp_n50_f0_four_protocols_e2e_tps_latency_20260916.pdf"
OUT_PNG = FIG_DIR / "gcp_n50_f0_four_protocols_e2e_tps_latency_20260916.png"

LINE3_MERGED = CSV_DIR / "gcp-n50-f0-line3-e2e-merged-with130-20260915.csv"
WAHOO_INPUTS = [
    CSV_DIR / "gcp-n50-f0-wahoo-r10k-40s-20260916.csv",
    CSV_DIR / "gcp-n50-f0-wahoo-r20k-40s-20260916.csv",
    CSV_DIR / "gcp-n50-f0-wahoo-r30k-40s-retry3-20260916.csv",
    CSV_DIR / "gcp-n50-f0-wahoo-r40k-40s-20260916.csv",
    CSV_DIR / "gcp-n50-f0-wahoo-r50k-40s-20260916.csv",
    CSV_DIR / "gcp-n50-f0-wahoo-r120k-40s-20260915.csv",
]

PROTOCOL_LABELS = {
    "shortfin": "Shortfin",
    "narwhal": "Narwhal/Tusk",
    "mahi_mahi": "Mahi-Mahi",
    "wahoo": "Wahoo",
}

PLOT_ORDER = ["shortfin", "narwhal", "mahi_mahi", "wahoo"]
COLORS = {
    "shortfin": "#1f77b4",
    "narwhal": "#ff7f0e",
    "mahi_mahi": "#2ca02c",
    "wahoo": "#d62728",
}
MARKERS = {
    "shortfin": "o",
    "narwhal": "s",
    "mahi_mahi": "^",
    "wahoo": "D",
}


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        row.setdefault("source_csv", path.relative_to(ROOT).as_posix())
        if not row["source_csv"]:
            row["source_csv"] = path.relative_to(ROOT).as_posix()
    return rows


def collect_rows() -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []

    # Main three-protocol 50-node series. This file intentionally excludes the
    # old Wahoo 120k row from the pre-repair combined CSV.
    for row in read_rows(LINE3_MERGED):
        if row["protocol"] in {"shortfin", "narwhal", "mahi_mahi"}:
            rows.append(row)

    # Wahoo was repaired and rerun separately; use those valid single-point CSVs.
    for path in WAHOO_INPUTS:
        rows.extend(read_rows(path))

    normalized = []
    for row in rows:
        if row.get("config_hash_verified") != "yes":
            continue
        if int(float(row["nodes"])) != 50 or int(float(row["faults"])) != 0:
            continue
        if row["protocol"] not in PROTOCOL_LABELS:
            continue
        normalized.append(
            {
                "protocol": row["protocol"],
                "protocol_label": PROTOCOL_LABELS[row["protocol"]],
                "nodes": int(float(row["nodes"])),
                "faults": int(float(row["faults"])),
                "rate": int(float(row["rate"])),
                "duration_s": int(float(row["duration_s"])),
                "consensus_tps": float(row["consensus_tps"]),
                "consensus_latency_ms": float(row["consensus_latency_ms"]),
                "end_to_end_tps": float(row["end_to_end_tps"]),
                "end_to_end_latency_ms": float(row["end_to_end_latency_ms"]),
                "source_csv": row["source_csv"],
            }
        )

    normalized.sort(key=lambda r: (PLOT_ORDER.index(r["protocol"]), r["rate"]))
    return normalized


def write_summary(rows: list[dict[str, object]]) -> None:
    CSV_DIR.mkdir(parents=True, exist_ok=True)
    fields = [
        "protocol",
        "protocol_label",
        "nodes",
        "faults",
        "rate",
        "duration_s",
        "consensus_tps",
        "consensus_latency_ms",
        "end_to_end_tps",
        "end_to_end_latency_ms",
        "source_csv",
    ]
    with SUMMARY_CSV.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def setup_style() -> None:
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
            "font.size": 9,
            "axes.labelsize": 9,
            "xtick.labelsize": 8,
            "ytick.labelsize": 8,
            "legend.fontsize": 8,
            "figure.dpi": 300,
            "savefig.dpi": 300,
            "savefig.bbox": "tight",
            "savefig.pad_inches": 0.04,
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "grid.alpha": 0.25,
            "grid.linewidth": 0.6,
        }
    )


def plot(rows: list[dict[str, object]]) -> None:
    setup_style()
    FIG_DIR.mkdir(parents=True, exist_ok=True)

    fig, ax = plt.subplots(figsize=(5.2, 3.3))
    for protocol in PLOT_ORDER:
        series = [r for r in rows if r["protocol"] == protocol]
        x = [float(r["end_to_end_tps"]) / 1000.0 for r in series]
        y = [float(r["end_to_end_latency_ms"]) / 1000.0 for r in series]
        ax.plot(
            x,
            y,
            marker=MARKERS[protocol],
            linewidth=1.7,
            markersize=4.8,
            color=COLORS[protocol],
            label=PROTOCOL_LABELS[protocol],
        )

    ax.set_xlabel("End-to-end throughput (k tx/s)")
    ax.set_ylabel("End-to-end latency (s)")
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False, ncol=2, loc="upper center")

    # Wahoo has much higher latency; the inset keeps the main all-protocol view
    # while making the three normal-latency curves readable.
    inset = ax.inset_axes([0.46, 0.13, 0.48, 0.44])
    for protocol in ["shortfin", "narwhal", "mahi_mahi"]:
        series = [r for r in rows if r["protocol"] == protocol]
        inset.plot(
            [float(r["end_to_end_tps"]) / 1000.0 for r in series],
            [float(r["end_to_end_latency_ms"]) / 1000.0 for r in series],
            marker=MARKERS[protocol],
            linewidth=1.2,
            markersize=3.5,
            color=COLORS[protocol],
        )
    inset.set_xlim(15, 110)
    inset.set_ylim(4.5, 13.0)
    inset.grid(True, alpha=0.22, linewidth=0.5)
    inset.tick_params(labelsize=7)
    inset.set_xlabel("k tx/s", fontsize=7, labelpad=0)
    inset.set_ylabel("s", fontsize=7, labelpad=0)

    fig.tight_layout()
    fig.savefig(OUT_PDF)
    fig.savefig(OUT_PNG)
    plt.close(fig)


def main() -> None:
    rows = collect_rows()
    write_summary(rows)
    plot(rows)
    print(f"Wrote {SUMMARY_CSV.relative_to(ROOT)}")
    print(f"Wrote {OUT_PDF.relative_to(ROOT)}")
    print(f"Wrote {OUT_PNG.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
