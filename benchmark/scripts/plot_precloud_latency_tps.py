#!/usr/bin/env python3
"""Plot consensus throughput-latency curves for the pre-cloud validation."""

import argparse
import csv
from pathlib import Path

import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MultipleLocator


PROTOCOLS = {
    "shortfin": {
        "label": "Shortfin",
        "color": "#0072B2",
        "marker": "o",
        "linestyle": "-",
    },
    "narwhal": {
        "label": "Narwhal/Tusk",
        "color": "#009E73",
        "marker": "s",
        "linestyle": "--",
    },
    "mahi_mahi": {
        "label": "Mahi-Mahi",
        "color": "#E69F00",
        "marker": "^",
        "linestyle": "-.",
    },
    "wahoo": {
        "label": "Wahoo",
        "color": "#D55E00",
        "marker": "D",
        "linestyle": ":",
    },
}

MODES = ("normal", "silence", "equivocation")


def parse_args():
    script_dir = Path(__file__).resolve().parent
    benchmark_dir = script_dir.parent
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=benchmark_dir / "csv_plots",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=benchmark_dir / "plots" / "precloud_20260914",
    )
    return parser.parse_args()


def load_mode(input_dir, mode):
    path = input_dir / f"local_precloud_{mode}_rtt0_60s_20260914_runs.csv"
    with path.open(newline="", encoding="utf-8-sig") as handle:
        rows = list(csv.DictReader(handle))

    expected_rates = {5_000, 15_000, 30_000, 60_000, 120_000, 180_000, 240_000, 300_000}
    grouped = {}
    for protocol in PROTOCOLS:
        values = [row for row in rows if row["protocol"] == protocol]
        values.sort(key=lambda row: int(row["rate"]))
        rates = {int(row["rate"]) for row in values}
        if rates != expected_rates:
            raise ValueError(f"{path}: unexpected rates for {protocol}: {sorted(rates)}")
        if any(float(row["consensus_tps"]) <= 0 for row in values):
            raise ValueError(f"{path}: non-positive consensus TPS for {protocol}")
        grouped[protocol] = values
    return grouped


def thousands(value, _position):
    return "0" if value == 0 else f"{value / 1_000:.0f}k"


def plot_mode(input_dir, output_dir, mode):
    grouped = load_mode(input_dir, mode)
    fig, ax = plt.subplots(figsize=(6.6, 4.3), constrained_layout=True)

    for protocol, style in PROTOCOLS.items():
        rows = grouped[protocol]
        throughput = [float(row["consensus_tps"]) for row in rows]
        latency = [float(row["consensus_latency_ms"]) / 1_000 for row in rows]
        ax.plot(
            throughput,
            latency,
            label=style["label"],
            color=style["color"],
            marker=style["marker"],
            linestyle=style["linestyle"],
            linewidth=1.8,
            markersize=6.0,
            markeredgecolor="white",
            markeredgewidth=0.7,
            zorder=3,
        )

    ax.set_xlim(0, 240_000)
    ax.set_ylim(0, 9.0)
    ax.set_xlabel("Consensus throughput (tx/s)")
    ax.set_ylabel("Consensus latency (s)")
    ax.xaxis.set_major_locator(MultipleLocator(40_000))
    ax.xaxis.set_major_formatter(FuncFormatter(thousands))
    ax.yaxis.set_major_locator(MultipleLocator(1.0))
    ax.grid(axis="both", color="#D9D9D9", linewidth=0.6, alpha=0.8)
    ax.set_axisbelow(True)
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.legend(
        loc="lower center",
        bbox_to_anchor=(0.5, 1.01),
        ncol=4,
        frameon=False,
        handlelength=2.2,
        columnspacing=1.3,
    )

    output_dir.mkdir(parents=True, exist_ok=True)
    stem = output_dir / f"local_precloud_{mode}_throughput_latency"
    fig.savefig(stem.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(stem.with_suffix(".png"), dpi=300, bbox_inches="tight")
    plt.close(fig)
    return stem


def plot_end_to_end_combined(input_dir, output_dir):
    fig, axes = plt.subplots(1, 3, figsize=(14.8, 4.4), sharex=True, sharey=True)
    panel_labels = {
        "normal": "(a) Normal",
        "silence": "(b) Silence",
        "equivocation": "(c) Equivocation",
    }

    for ax, mode in zip(axes, MODES):
        grouped = load_mode(input_dir, mode)
        for protocol, style in PROTOCOLS.items():
            rows = grouped[protocol]
            throughput = [float(row["end_to_end_tps"]) for row in rows]
            latency = [float(row["end_to_end_latency_ms"]) / 1_000 for row in rows]
            ax.plot(
                throughput,
                latency,
                label=style["label"],
                color=style["color"],
                marker=style["marker"],
                linestyle=style["linestyle"],
                linewidth=1.7,
                markersize=5.3,
                markeredgecolor="white",
                markeredgewidth=0.6,
                zorder=3,
            )

        ax.set_xlim(0, 240_000)
        ax.set_ylim(0, 15.0)
        ax.xaxis.set_major_locator(MultipleLocator(60_000))
        ax.xaxis.set_major_formatter(FuncFormatter(thousands))
        ax.yaxis.set_major_locator(MultipleLocator(2.5))
        ax.grid(axis="both", color="#D9D9D9", linewidth=0.6, alpha=0.8)
        ax.set_axisbelow(True)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.text(
            0.03,
            0.96,
            panel_labels[mode],
            transform=ax.transAxes,
            ha="left",
            va="top",
            fontsize=10,
            fontweight="bold",
        )

    handles, labels = axes[0].get_legend_handles_labels()
    fig.legend(
        handles,
        labels,
        loc="upper center",
        bbox_to_anchor=(0.5, 1.0),
        ncol=4,
        frameon=False,
        handlelength=2.2,
        columnspacing=1.5,
    )
    fig.supxlabel("End-to-end throughput (tx/s)", y=0.02)
    fig.supylabel("End-to-end latency (s)", x=0.015)
    fig.subplots_adjust(left=0.065, right=0.995, bottom=0.16, top=0.85, wspace=0.12)

    output_dir.mkdir(parents=True, exist_ok=True)
    stem = output_dir / "local_precloud_rtt0_end_to_end_throughput_latency"
    fig.savefig(stem.with_suffix(".pdf"), bbox_inches="tight")
    fig.savefig(stem.with_suffix(".png"), dpi=300, bbox_inches="tight")
    plt.close(fig)
    return stem


def main():
    args = parse_args()
    plt.rcParams.update(
        {
            "font.family": "serif",
            "font.serif": ["Times New Roman", "Times", "DejaVu Serif"],
            "font.size": 10,
            "axes.labelsize": 10,
            "xtick.labelsize": 9,
            "ytick.labelsize": 9,
            "legend.fontsize": 9,
            "pdf.fonttype": 42,
            "ps.fonttype": 42,
        }
    )
    for mode in MODES:
        stem = plot_mode(args.input_dir, args.output_dir, mode)
        print(f"Saved {stem}.pdf and {stem}.png")
    stem = plot_end_to_end_combined(args.input_dir, args.output_dir)
    print(f"Saved {stem}.pdf and {stem}.png")


if __name__ == "__main__":
    main()
