#!/usr/bin/env python3
"""Build a curated 10/20/50-node E2E TPS-latency summary and plot."""

from __future__ import annotations

import csv
from pathlib import Path

import matplotlib.pyplot as plt


ROOT = Path(__file__).resolve().parents[1]
REPO = ROOT.parent
CSV_DIR = ROOT / "csv_plots"
FIG_DIR = ROOT / "figures"

OUT_CSV = CSV_DIR / "gcp-f0-n10-n20-n50-four-protocols-e2e-summary-20260916.csv"
OUT_PDF = FIG_DIR / "gcp_f0_n10_n20_n50_four_protocols_e2e_20260916.pdf"
OUT_PNG = FIG_DIR / "gcp_f0_n10_n20_n50_four_protocols_e2e_20260916.png"

N10_LEGACY = REPO / "paperdata" / "csv" / "wan" / "wan_n10_plot_summary.csv"
N20_LEGACY = REPO / "paperdata" / "csv" / "wan" / "wan_n20_f0_plot_summary.csv"
N10_MAHI_FILES = [
    CSV_DIR / "gcp-n10-f0-mahi-line-20260916-summary.csv",
    CSV_DIR / "gcp-n10-f0-mahi_mahi-r200k-50s-20260916.csv",
    CSV_DIR / "gcp-n10-f0-mahi_mahi-r220k-50s-20260916.csv",
]
N20_MAHI = CSV_DIR / "gcp-n20-f0-mahi-line-20260916-summary.csv"
N50_SUMMARY = CSV_DIR / "gcp-n50-f0-four-protocols-e2e-summary-20260916.csv"

PROTOCOL_ORDER = ["shortfin", "narwhal", "mahi_mahi", "wahoo"]
PROTOCOL_LABELS = {
    "shortfin": "Shortfin",
    "narwhal": "Narwhal/Tusk",
    "mahi_mahi": "Mahi-Mahi",
    "wahoo": "Wahoo",
}
LEGACY_NAMES = {
    "noveldag": "shortfin",
    "narwhal": "narwhal",
    "wahoo": "wahoo",
}
COLORS = {
    "shortfin": "#0072B2",
    "narwhal": "#D55E00",
    "mahi_mahi": "#009E73",
    "wahoo": "#CC79A7",
}
MARKERS = {
    "shortfin": "o",
    "narwhal": "s",
    "mahi_mahi": "^",
    "wahoo": "D",
}


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def as_float(row: dict[str, str], key: str) -> float:
    return float(row[key])


def add_row(
    rows: list[dict[str, object]],
    *,
    protocol: str,
    nodes: int,
    faults: int,
    rate: int,
    duration_s: int | None,
    consensus_tps: float,
    consensus_latency_ms: float,
    end_to_end_tps: float,
    end_to_end_latency_ms: float,
    source_csv: Path | str,
    note: str,
) -> None:
    rows.append(
        {
            "protocol": protocol,
            "protocol_label": PROTOCOL_LABELS[protocol],
            "nodes": nodes,
            "faults": faults,
            "rate": rate,
            "duration_s": "" if duration_s is None else duration_s,
            "consensus_tps": round(consensus_tps, 3),
            "consensus_latency_ms": round(consensus_latency_ms, 3),
            "end_to_end_tps": round(end_to_end_tps, 3),
            "end_to_end_latency_ms": round(end_to_end_latency_ms, 3),
            "source_csv": (
                source_csv.relative_to(REPO).as_posix()
                if isinstance(source_csv, Path)
                else source_csv
            ),
            "note": note,
        }
    )


def collect_legacy(rows: list[dict[str, object]], path: Path, nodes: int) -> None:
    for row in read_csv(path):
        if int(float(row["faults"])) != 0 or int(float(row["nodes"])) != nodes:
            continue
        protocol = LEGACY_NAMES.get(row["protocol"])
        if protocol is None:
            continue
        add_row(
            rows,
            protocol=protocol,
            nodes=nodes,
            faults=0,
            rate=int(float(row["rate"])),
            duration_s=int(float(row["execution_time_s"])),
            consensus_tps=as_float(row, "consensus_tps"),
            consensus_latency_ms=as_float(row, "consensus_latency_ms"),
            end_to_end_tps=as_float(row, "e2e_tps"),
            end_to_end_latency_ms=as_float(row, "e2e_latency_ms"),
            source_csv=path,
            note="legacy baseline",
        )


def collect_n10_mahi(rows: list[dict[str, object]]) -> None:
    seen_rates: set[int] = set()
    for path in N10_MAHI_FILES:
        for row in read_csv(path):
            rate = int(float(row["rate"]))
            if rate in seen_rates:
                continue
            seen_rates.add(rate)
            add_row(
                rows,
                protocol="mahi_mahi",
                nodes=10,
                faults=0,
                rate=rate,
                duration_s=int(float(row["duration_s"])),
                consensus_tps=as_float(row, "consensus_tps"),
                consensus_latency_ms=as_float(row, "consensus_latency_ms"),
                end_to_end_tps=as_float(row, "end_to_end_tps"),
                end_to_end_latency_ms=as_float(row, "end_to_end_latency_ms"),
                source_csv=path,
                note=row.get("note", "current single run"),
            )


def collect_mahi_summary(rows: list[dict[str, object]], path: Path, nodes: int) -> None:
    for row in read_csv(path):
        add_row(
            rows,
            protocol="mahi_mahi",
            nodes=nodes,
            faults=0,
            rate=int(float(row["rate"])),
            duration_s=int(float(row["duration_s"])),
            consensus_tps=as_float(row, "consensus_tps"),
            consensus_latency_ms=as_float(row, "consensus_latency_ms"),
            end_to_end_tps=as_float(row, "end_to_end_tps"),
            end_to_end_latency_ms=as_float(row, "end_to_end_latency_ms"),
            source_csv=path,
            note=row.get("note", "current single run"),
        )


def collect_n50(rows: list[dict[str, object]]) -> None:
    for row in read_csv(N50_SUMMARY):
        protocol = row["protocol"]
        if protocol not in PROTOCOL_ORDER:
            continue
        add_row(
            rows,
            protocol=protocol,
            nodes=50,
            faults=0,
            rate=int(float(row["rate"])),
            duration_s=int(float(row["duration_s"])),
            consensus_tps=as_float(row, "consensus_tps"),
            consensus_latency_ms=as_float(row, "consensus_latency_ms"),
            end_to_end_tps=as_float(row, "end_to_end_tps"),
            end_to_end_latency_ms=as_float(row, "end_to_end_latency_ms"),
            source_csv=row["source_csv"],
            note="current curated 50-node summary",
        )


def collect_rows() -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    collect_legacy(rows, N10_LEGACY, 10)
    collect_n10_mahi(rows)
    collect_legacy(rows, N20_LEGACY, 20)
    collect_mahi_summary(rows, N20_MAHI, 20)
    collect_n50(rows)
    rows.sort(key=lambda r: (int(r["nodes"]), PROTOCOL_ORDER.index(str(r["protocol"])), int(r["rate"])))
    return rows


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
        "note",
    ]
    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
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

    fig, axes = plt.subplots(1, 3, figsize=(7.2, 2.65), sharex=False, sharey=False)
    for ax, nodes in zip(axes, [10, 20, 50]):
        subset = [r for r in rows if int(r["nodes"]) == nodes]
        for protocol in PROTOCOL_ORDER:
            series = [r for r in subset if r["protocol"] == protocol]
            if not series:
                continue
            x = [float(r["end_to_end_tps"]) / 1000.0 for r in series]
            y = [float(r["end_to_end_latency_ms"]) / 1000.0 for r in series]
            ax.plot(
                x,
                y,
                marker=MARKERS[protocol],
                color=COLORS[protocol],
                linewidth=1.35,
                markersize=3.6,
                label=PROTOCOL_LABELS[protocol],
            )
        ax.text(
            0.04,
            0.94,
            f"{nodes} nodes",
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=9,
        )
        ax.set_xlabel("E2E throughput (k tx/s)")
        ax.set_xlim(left=0)
        ax.set_ylim(bottom=0)

    axes[0].set_ylabel("E2E latency (s)")
    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(handles, labels, frameon=False, loc="upper center", ncol=4, bbox_to_anchor=(0.5, 1.04))
    fig.tight_layout(rect=[0, 0, 1, 0.92])
    fig.savefig(OUT_PDF)
    fig.savefig(OUT_PNG)
    plt.close(fig)


def main() -> None:
    rows = collect_rows()
    write_summary(rows)
    plot(rows)
    print(f"Wrote {OUT_CSV.relative_to(REPO)}")
    print(f"Wrote {OUT_PDF.relative_to(REPO)}")
    print(f"Wrote {OUT_PNG.relative_to(REPO)}")


if __name__ == "__main__":
    main()
