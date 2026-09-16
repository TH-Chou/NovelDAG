#!/usr/bin/env python3
"""Plot 10-node GCP Mahi-Mahi end-to-end throughput/latency results."""

from __future__ import annotations

import csv
from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
CSV_DIR = ROOT / "csv_plots"
FIG_DIR = ROOT / "figures"

SUMMARY_CSV = CSV_DIR / "gcp-n10-f0-mahi-line-20260916-summary.csv"
OUT_PDF = FIG_DIR / "gcp_n10_f0_mahi_e2e_tps_latency_20260916.pdf"
OUT_PNG = FIG_DIR / "gcp_n10_f0_mahi_e2e_tps_latency_20260916.png"

INPUTS = [
    CSV_DIR / "gcp-n10-f0-mahi-line-20260916-r30000.csv",
    CSV_DIR / "gcp-n10-f0-mahi-line-20260916-r60000.csv",
    CSV_DIR / "gcp-n10-f0-mahi-line-20260916-r90000.csv",
    CSV_DIR / "gcp_pilot_n10_f0_r120k_50s_20260914.csv",
    CSV_DIR / "gcp-n10-f0-mahi-line-20260916-r150000.csv",
    CSV_DIR / "gcp-n10-f0-mahi-line-20260916-r180000.csv",
]


def read_single(path: Path) -> dict[str, object]:
    with path.open(newline="") as f:
        rows = list(csv.DictReader(f))

    if path.name == "gcp_pilot_n10_f0_r120k_50s_20260914.csv":
        matches = [row for row in rows if row["protocol"] == "mahi_mahi"]
        if len(matches) != 1:
            raise ValueError(f"Expected one Mahi-Mahi row in {path}, got {len(matches)}")
        row = matches[0]
        row = {
            "protocol": "mahi_mahi",
            "nodes": "10",
            "faults": "0",
            "rate": "120000",
            "duration_s": row["requested_duration_s"],
            "consensus_tps": row["consensus_tps"],
            "consensus_latency_ms": row["consensus_latency_ms"],
            "end_to_end_tps": row["end_to_end_tps"],
            "end_to_end_latency_ms": row["end_to_end_latency_ms"],
            "config_hash_verified": "yes",
            "note": row["notes"],
        }
    else:
        if len(rows) != 1:
            raise ValueError(f"Expected exactly one row in {path}, got {len(rows)}")
        row = rows[0]
        row["note"] = "single run"

    return {
        "protocol": row["protocol"],
        "nodes": int(float(row["nodes"])),
        "faults": int(float(row["faults"])),
        "rate": int(float(row["rate"])),
        "duration_s": int(float(row["duration_s"])),
        "consensus_tps": float(row["consensus_tps"]),
        "consensus_latency_ms": float(row["consensus_latency_ms"]),
        "end_to_end_tps": float(row["end_to_end_tps"]),
        "end_to_end_latency_ms": float(row["end_to_end_latency_ms"]),
        "config_hash_verified": row["config_hash_verified"],
        "source_csv": path.relative_to(ROOT).as_posix(),
        "note": row["note"],
    }


def collect_rows() -> list[dict[str, object]]:
    rows = [read_single(path) for path in INPUTS]
    rows.sort(key=lambda r: int(r["rate"]))
    return rows


def write_summary(rows: list[dict[str, object]]) -> None:
    fields = [
        "protocol",
        "nodes",
        "faults",
        "rate",
        "duration_s",
        "consensus_tps",
        "consensus_latency_ms",
        "end_to_end_tps",
        "end_to_end_latency_ms",
        "config_hash_verified",
        "source_csv",
        "note",
    ]
    CSV_DIR.mkdir(parents=True, exist_ok=True)
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

    fig, ax = plt.subplots(figsize=(4.8, 3.0))
    x = [float(r["end_to_end_tps"]) / 1000.0 for r in rows]
    y = [float(r["end_to_end_latency_ms"]) / 1000.0 for r in rows]
    rates = [int(r["rate"]) // 1000 for r in rows]

    ax.plot(x, y, marker="o", linewidth=1.7, markersize=4.8, color="#2ca02c", label="Mahi-Mahi")
    for x_i, y_i, rate in zip(x, y, rates):
        ax.annotate(f"{rate}K", (x_i, y_i), xytext=(3, 4), textcoords="offset points", fontsize=7)

    ax.set_xlabel("End-to-end throughput (k tx/s)")
    ax.set_ylabel("End-to-end latency (s)")
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    ax.legend(frameon=False, loc="upper left")

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
