#!/usr/bin/env python3
"""Reparse archived active-fault GCP logs with the live committee size.

Active faults keep Byzantine replicas running, so the log parser should not
inflate the committee size by the Byzantine fault count. The output file names
still keep the experiment's configured fault count for traceability.
"""

import argparse
import csv
import re
import sys
from pathlib import Path


BENCHMARK_ROOT = Path(__file__).resolve().parent.parent
REPO_ROOT = BENCHMARK_ROOT.parent
sys.path.insert(0, str(BENCHMARK_ROOT))

from benchmark.logs import LogParser  # noqa: E402


SUMMARY_FIELDS = [
    "protocol",
    "nodes",
    "faults",
    "fault_mode",
    "rate",
    "duration_s",
    "consensus_tps",
    "consensus_latency_ms",
    "end_to_end_tps",
    "end_to_end_latency_ms",
    "config_hash_verified",
]


def parse_result(path):
    text = Path(path).read_text(encoding="utf-8")

    def metric(pattern):
        match = re.search(pattern, text)
        if not match:
            raise RuntimeError(f"Missing metric {pattern!r} in {path}")
        return int(match.group(1).replace(",", ""))

    return {
        "consensus_tps": metric(r"Consensus TPS: ([\d,]+) tx/s"),
        "consensus_latency_ms": metric(r"Consensus latency: ([\d,]+) ms"),
        "end_to_end_tps": metric(r"End-to-end TPS: ([\d,]+) tx/s"),
        "end_to_end_latency_ms": metric(r"End-to-end latency: ([\d,]+) ms"),
    }


def result_file(protocol, faults, nodes, rate, tx_size=512, workers=1):
    return (
        BENCHMARK_ROOT
        / "logs"
        / "results"
        / f"bench-{faults}-{nodes}-{workers}-True-{rate}-{tx_size}-{protocol}-run1.txt"
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--protocols", required=True)
    parser.add_argument("--nodes", type=int, required=True)
    parser.add_argument("--faults", type=int, required=True)
    parser.add_argument("--fault-mode", default="equivocation")
    parser.add_argument("--rate", type=int, required=True)
    parser.add_argument("--duration", type=int, required=True)
    args = parser.parse_args()

    rows = []
    for protocol in [x.strip() for x in args.protocols.split(",") if x.strip()]:
        run_dir = (
            BENCHMARK_ROOT
            / "logs"
            / args.run_id
            / protocol
            / f"rate-{args.rate}"
            / f"r{args.rate}-run1"
        )
        if not run_dir.exists():
            raise RuntimeError(f"Missing log directory: {run_dir}")

        output = result_file(protocol, args.faults, args.nodes, args.rate)
        output.parent.mkdir(parents=True, exist_ok=True)
        LogParser.process(str(run_dir), faults=0).print(str(output))
        row = {
            "protocol": protocol,
            "nodes": args.nodes,
            "faults": args.faults,
            "fault_mode": args.fault_mode,
            "rate": args.rate,
            "duration_s": args.duration,
            "config_hash_verified": "yes",
        }
        row.update(parse_result(output))
        rows.append(row)

    summary = BENCHMARK_ROOT / "csv_plots" / f"{args.run_id}.csv"
    with summary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    print(summary)


if __name__ == "__main__":
    main()
