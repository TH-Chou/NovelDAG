#!/usr/bin/env python3
"""Run all 4 dag protocols locally with n=10, f=1, rate=150_000.

Usage: python3 benchmark/run_compare.py
"""
import sys
from benchmark.local import LocalBench
from benchmark.utils import Print, BenchError

PROTOCOLS = ["narwhal", "bullshark", "noveldag", "wahoo"]

BENCH = {
    "faults": 1,
    "nodes": 10,
    "workers": 1,
    "rate": 150_000,
    "tx_size": 512,
    "duration": 30,
}
NODE = {
    "header_size": 1_000,
    "max_header_delay": 2000,
    "gc_depth": 50,
    "sync_retry_delay": 10_000,
    "sync_retry_nodes": 3,
    "batch_size": 500_000,
    "max_batch_delay": 200,
    "consensus_protocol": "round_robin",
}


def main():
    results = {}
    for proto in PROTOCOLS:
        Print.heading(f"Running {proto} (n=10, f=1, rate=150k)")
        params = dict(NODE)
        params["dag_protocol"] = proto
        try:
            parser = LocalBench(BENCH, params).run(debug=False)
            results[proto] = parser.metrics()
            print(parser.result())
        except BenchError as e:
            Print.error(e)
            results[proto] = None

    print("\n" + "=" * 75)
    print(f"  {'Protocol':<12s}  {'Cons TPS':>10s}  {'Cons Lat (ms)':>14s}"
          f"  {'E2E TPS':>10s}  {'E2E Lat (ms)':>14s}")
    print("-" * 75)
    for proto in PROTOCOLS:
        m = results.get(proto)
        if m is None:
            print(f"  {proto:<12s}  ERROR")
            continue
        print(f"  {proto:<12s}  "
              f"{m['consensus_tps']:>10.1f}  "
              f"{m['consensus_latency_ms']:>14.1f}  "
              f"{m['end_to_end_tps']:>10.1f}  "
              f"{m['end_to_end_latency_ms']:>14.1f}")
    print("=" * 75)


if __name__ == "__main__":
    sys.exit(main())
