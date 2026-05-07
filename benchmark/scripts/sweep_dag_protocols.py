#!/usr/bin/env python3
"""Local rate sweep across all three DAG protocols with chart generation."""
import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as tick

from benchmark.local import LocalBench


NODE_PARAMS_BASE = {
    "header_size": 1_000,
    "max_header_delay": 200,
    "gc_depth": 50,
    "sync_retry_delay": 10_000,
    "sync_retry_nodes": 3,
    "batch_size": 500_000,
    "max_batch_delay": 200,
    "consensus_protocol": "round_robin",
}

PROTOCOLS = ["noveldag", "narwhal", "bullshark"]
PROTOCOL_LABELS = {"narwhal": "Narwhal", "bullshark": "Bullshark", "noveldag": "NovelDAG"}
PROTOCOL_MARKERS = {"narwhal": "o", "bullshark": "s", "noveldag": "D"}
PROTOCOL_COLORS = {"narwhal": "#2196F3", "bullshark": "#FF9800", "noveldag": "#4CAF50"}


def rate_range(start: int, end: int, step: int):
    return list(range(start, end + 1, step))


def run_once(rate: int, duration: int, nodes: int, workers: int, tx_size: int,
             faults: int, dag_protocol: str, debug: bool):
    bench_params = {
        "faults": faults,
        "nodes": nodes,
        "workers": workers,
        "rate": rate,
        "tx_size": tx_size,
        "duration": duration,
    }
    node_params = dict(NODE_PARAMS_BASE)
    node_params["dag_protocol"] = dag_protocol
    parser = LocalBench(bench_params, node_params).run(debug=debug)
    return parser.metrics()


def write_runs_csv(path: Path, rows):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "run", "rate", "protocol",
                "consensus_tps", "consensus_latency_ms",
                "end_to_end_tps", "end_to_end_latency_ms",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def aggregate(rows):
    """Group by (rate, protocol) and average across runs."""
    grouped = defaultdict(list)
    for row in rows:
        grouped[(int(row["rate"]), row["protocol"])].append(row)

    avg_rows = []
    for (rate, proto), items in sorted(grouped.items()):
        n = len(items)
        avg_rows.append({
            "rate": rate,
            "protocol": proto,
            "consensus_tps": sum(float(x["consensus_tps"]) for x in items) / n,
            "consensus_latency_ms": sum(float(x["consensus_latency_ms"]) for x in items) / n,
            "end_to_end_tps": sum(float(x["end_to_end_tps"]) for x in items) / n,
            "end_to_end_latency_ms": sum(float(x["end_to_end_latency_ms"]) for x in items) / n,
            "rounds": n,
        })
    return avg_rows


def write_average_csv(path: Path, avg_rows):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "rate", "protocol",
                "consensus_tps", "consensus_latency_ms",
                "end_to_end_tps", "end_to_end_latency_ms",
                "rounds",
            ],
        )
        writer.writeheader()
        writer.writerows(avg_rows)


@tick.FuncFormatter
def k_formatter(x, pos):
    if x >= 1_000:
        return f'{x/1000:.0f}k'
    return f'{x:.0f}'


@tick.FuncFormatter
def ms_formatter(x, pos):
    return f'{x:.0f}'


def plot_latency_vs_tps(avg_rows, out_path: Path, title_suffix: str = ""):
    """Latency vs achieved TPS, one curve per protocol."""
    fig, ax = plt.subplots(figsize=(8, 5))

    for proto in PROTOCOLS:
        pts = [r for r in avg_rows if r["protocol"] == proto]
        if not pts:
            continue
        pts.sort(key=lambda r: r["consensus_tps"])
        x = [r["consensus_tps"] for r in pts]
        y = [r["consensus_latency_ms"] / 1000 for r in pts]
        ax.plot(x, y, marker=PROTOCOL_MARKERS[proto], color=PROTOCOL_COLORS[proto],
                label=PROTOCOL_LABELS[proto], linewidth=1.5, markersize=7)

    ax.set_xlabel("Throughput (tx/s)", fontweight="bold")
    ax.set_ylabel("Latency (s)", fontweight="bold")
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    ax.xaxis.set_major_formatter(k_formatter)
    title = "Latency vs Throughput"
    if title_suffix:
        title += f" ({title_suffix})"
    ax.set_title(title)
    fig.tight_layout()
    for fmt in ["png", "pdf"]:
        fig.savefig(out_path.with_name(out_path.stem + f"_latency_vs_tps.{fmt}"),
                    bbox_inches="tight")
    plt.close(fig)


def plot_tps_vs_rate(avg_rows, out_path: Path, title_suffix: str = ""):
    """Achieved TPS vs input rate, one curve per protocol."""
    fig, ax = plt.subplots(figsize=(8, 5))

    for proto in PROTOCOLS:
        pts = [r for r in avg_rows if r["protocol"] == proto]
        if not pts:
            continue
        pts.sort(key=lambda r: r["rate"])
        x = [r["rate"] for r in pts]
        y = [r["consensus_tps"] for r in pts]
        ax.plot(x, y, marker=PROTOCOL_MARKERS[proto], color=PROTOCOL_COLORS[proto],
                label=PROTOCOL_LABELS[proto], linewidth=1.5, markersize=7)

    # Diagonal reference line (ideal: TPS = input rate)
    all_rates = [r["rate"] for r in avg_rows]
    max_val = max(max(all_rates), max(r["consensus_tps"] for r in avg_rows)) * 1.05
    ax.plot([0, max_val], [0, max_val], 'k--', linewidth=0.5, alpha=0.3, label="ideal")

    ax.set_xlabel("Input Rate (tx/s)", fontweight="bold")
    ax.set_ylabel("Throughput (tx/s)", fontweight="bold")
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)
    ax.xaxis.set_major_formatter(k_formatter)
    ax.yaxis.set_major_formatter(k_formatter)
    title = "Throughput vs Input Rate"
    if title_suffix:
        title += f" ({title_suffix})"
    ax.set_title(title)
    fig.tight_layout()
    for fmt in ["png", "pdf"]:
        fig.savefig(out_path.with_name(out_path.stem + f"_tps_vs_rate.{fmt}"),
                    bbox_inches="tight")
    plt.close(fig)


def parse_args():
    p = argparse.ArgumentParser(
        description="Local DAG protocol rate sweep with charts"
    )
    p.add_argument("--rate-start", type=int, default=300_000)
    p.add_argument("--rate-end", type=int, default=420_000)
    p.add_argument("--rate-step", type=int, default=30_000)
    p.add_argument("--rounds", type=int, default=2)
    p.add_argument("--duration", type=int, default=30)
    p.add_argument("--nodes", type=int, default=10)
    p.add_argument("--workers", type=int, default=1)
    p.add_argument("--faults", type=int, default=1)
    p.add_argument("--tx-size", type=int, default=512)
    p.add_argument("--debug", action="store_true", default=True)
    p.add_argument("--output-prefix", type=str,
                   default="csv_plots/local_dag_sweep_n10_f1")
    return p.parse_args()


def main():
    args = parse_args()

    if args.rounds <= 0:
        raise ValueError("rounds must be > 0")
    if args.rate_step <= 0:
        raise ValueError("rate-step must be > 0")
    if args.rate_end < args.rate_start:
        raise ValueError("rate-end must be >= rate-start")

    rates = rate_range(args.rate_start, args.rate_end, args.rate_step)
    output_prefix = Path(args.output_prefix)
    output_prefix.parent.mkdir(parents=True, exist_ok=True)

    runs_csv = output_prefix.with_name(output_prefix.name + "_runs.csv")
    average_csv = output_prefix.with_name(output_prefix.name + "_average.csv")

    total = len(PROTOCOLS) * len(rates) * args.rounds
    rows = []
    idx = 0
    for proto in PROTOCOLS:
        for rate in rates:
            for run_idx in range(1, args.rounds + 1):
                idx += 1
                print(
                    f"[{idx}/{total}] proto={proto} rate={rate} run={run_idx}/{args.rounds}",
                    flush=True,
                )
                metrics = run_once(
                    rate=rate, duration=args.duration,
                    nodes=args.nodes, workers=args.workers,
                    tx_size=args.tx_size, faults=args.faults,
                    dag_protocol=proto, debug=args.debug,
                )
                row = {
                    "run": run_idx,
                    "rate": rate,
                    "protocol": proto,
                    "consensus_tps": f'{metrics["consensus_tps"]:.6f}',
                    "consensus_latency_ms": f'{metrics["consensus_latency_ms"]:.6f}',
                    "end_to_end_tps": f'{metrics["end_to_end_tps"]:.6f}',
                    "end_to_end_latency_ms": f'{metrics["end_to_end_latency_ms"]:.6f}',
                }
                rows.append(row)
                print(f"  tps={row['consensus_tps']} latency={row['consensus_latency_ms']}ms")

    write_runs_csv(runs_csv, rows)
    avg_rows = aggregate(rows)
    write_average_csv(average_csv, avg_rows)

    print(f"\nruns csv: {runs_csv}")
    print(f"average csv: {average_csv}")

    # Print summary table
    print("\n=== SUMMARY ===")
    print(f"{'Rate':>8s} {'Proto':>10s} {'TPS':>10s} {'Lat(ms)':>10s}")
    print("-" * 42)
    for r in sorted(avg_rows, key=lambda r: (r["rate"], r["protocol"])):
        print(f"{r['rate']:>8d} {r['protocol']:>10s} {r['consensus_tps']:>10.1f} {r['consensus_latency_ms']:>10.1f}")

    # Generate charts
    title_suffix = f"n={args.nodes}, f={args.faults}, tx={args.tx_size}B"
    plot_latency_vs_tps(avg_rows, output_prefix, title_suffix)
    plot_tps_vs_rate(avg_rows, output_prefix, title_suffix)
    print(f"\nCharts saved to: {output_prefix}_latency_vs_tps.[png|pdf]")
    print(f"               {output_prefix}_tps_vs_rate.[png|pdf]")


if __name__ == "__main__":
    main()
