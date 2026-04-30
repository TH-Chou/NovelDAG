#!/usr/bin/env python3
import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from benchmark.local import LocalBench


DEFAULT_NODE_PARAMS = {
    "header_size": 1_000,
    "max_header_delay": 200,
    "gc_depth": 50,
    "sync_retry_delay": 10_000,
    "sync_retry_nodes": 3,
    "batch_size": 500_000,
    "max_batch_delay": 200,
    "consensus_protocol": "round_robin",
}


def rate_range(start: int, end: int, step: int):
    return list(range(start, end + 1, step))


def run_once(rate: int, duration: int, nodes: int, workers: int, tx_size: int, faults: int, debug: bool):
    bench_params = {
        "faults": faults,
        "nodes": nodes,
        "workers": workers,
        "rate": rate,
        "tx_size": tx_size,
        "duration": duration,
    }
    parser = LocalBench(bench_params, dict(DEFAULT_NODE_PARAMS)).run(debug=debug)
    return parser.metrics()


def write_runs_csv(path: Path, rows):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "run",
                "rate",
                "protocol",
                "consensus_tps",
                "consensus_latency_ms",
                "end_to_end_tps",
                "end_to_end_latency_ms",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def aggregate(rows):
    grouped = defaultdict(list)
    for row in rows:
        grouped[int(row["rate"])].append(row)

    avg_rows = []
    for rate in sorted(grouped.keys()):
        items = grouped[rate]
        n = len(items)
        avg_rows.append(
            {
                "rate": rate,
                "protocol": "round_robin",
                "consensus_tps": sum(float(x["consensus_tps"]) for x in items) / n,
                "consensus_latency_ms": sum(float(x["consensus_latency_ms"]) for x in items) / n,
                "end_to_end_tps": sum(float(x["end_to_end_tps"]) for x in items) / n,
                "end_to_end_latency_ms": sum(float(x["end_to_end_latency_ms"]) for x in items) / n,
                "rounds": n,
            }
        )
    return avg_rows


def write_average_csv(path: Path, avg_rows):
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "rate",
                "protocol",
                "consensus_tps",
                "consensus_latency_ms",
                "end_to_end_tps",
                "end_to_end_latency_ms",
                "rounds",
            ],
        )
        writer.writeheader()
        writer.writerows(avg_rows)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run local round_robin benchmark rate sweep and export runs/average CSV + plot"
    )
    parser.add_argument("--rate-start", type=int, default=30_000)
    parser.add_argument("--rate-end", type=int, default=240_000)
    parser.add_argument("--rate-step", type=int, default=30_000)
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--duration", type=int, default=20)
    parser.add_argument("--nodes", type=int, default=10)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--faults", type=int, default=1)
    parser.add_argument("--tx-size", type=int, default=512)
    parser.add_argument("--debug", action="store_true", default=True)
    parser.add_argument(
        "--output-prefix",
        type=str,
        default="results/local_round_robin_rate_sweep_n10_f1_30000_210000_step30000",
        help="Output prefix without extension",
    )
    return parser.parse_args()


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

    rows = []
    for rate in rates:
        for run_idx in range(1, args.rounds + 1):
            print(
                f"[run {run_idx}/{args.rounds}] rate={rate} protocol=round_robin duration={args.duration}s",
                flush=True,
            )
            metrics = run_once(
                rate=rate,
                duration=args.duration,
                nodes=args.nodes,
                workers=args.workers,
                tx_size=args.tx_size,
                faults=args.faults,
                debug=args.debug,
            )
            row = {
                "run": run_idx,
                "rate": rate,
                "protocol": "round_robin",
                "consensus_tps": f'{metrics["consensus_tps"]:.6f}',
                "consensus_latency_ms": f'{metrics["consensus_latency_ms"]:.6f}',
                "end_to_end_tps": f'{metrics["end_to_end_tps"]:.6f}',
                "end_to_end_latency_ms": f'{metrics["end_to_end_latency_ms"]:.6f}',
            }
            rows.append(row)
            print(f"  consensus_tps={row['consensus_tps']} latency={row['consensus_latency_ms']}ms")

    write_runs_csv(runs_csv, rows)
    avg_rows = aggregate(rows)
    write_average_csv(average_csv, avg_rows)

    print(f"runs csv: {runs_csv}")
    print(f"average csv: {average_csv}")

    # Print summary
    print("\n=== SUMMARY ===")
    for r in avg_rows:
        print(f"rate={r['rate']}: consensus_tps={r['consensus_tps']:.2f} latency={r['consensus_latency_ms']:.2f}ms e2e_latency={r['end_to_end_latency_ms']:.2f}ms")


if __name__ == "__main__":
    main()
