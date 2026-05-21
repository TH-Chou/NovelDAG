#!/usr/bin/env python3
"""Fast streaming parser for large benchmark logs.

The standard LogParser reads every log file into memory and then applies large
regular expressions. That is fine for small Narwhal/NovelDAG runs, but Wahoo can
produce hundreds of MB of primary logs per sweep point. This parser scans files
line by line and extracts only the fields needed for the paper CSV/results.
"""

from __future__ import annotations

import argparse
import re
from datetime import datetime
from pathlib import Path
from statistics import mean


TS = r"\[(?P<ts>.*?Z) "
RE_CLIENT_SIZE = re.compile(r"Transactions size: (\d+)")
RE_CLIENT_RATE = re.compile(r"Transactions rate: (\d+)")
RE_CLIENT_START = re.compile(TS + r".* Start ")
RE_CLIENT_SAMPLE = re.compile(TS + r".* sample transaction (\d+)")
RE_PRIMARY_CREATED = re.compile(TS + r".* Created B\d+\([^ ]+\) -> ([^ ]+=)")
RE_PRIMARY_COMMITTED = re.compile(TS + r".* Committed B\d+\([^ ]+\) -> ([^ ]+=)")
RE_PRIMARY_BOOT = re.compile(r"booted on (\d+\.\d+\.\d+\.\d+)")
RE_WORKER_BATCH_SIZE = re.compile(r"Batch ([^ ]+) contains (\d+) B")
RE_WORKER_SAMPLE = re.compile(r"Batch ([^ ]+) contains sample tx (\d+)")
RE_WORKER_BOOT = re.compile(r"booted on (\d+\.\d+\.\d+\.\d+)")
RE_CONFIGS = {
    "header_size": re.compile(r"Header size .* (\d+)"),
    "max_header_delay": re.compile(r"Max header delay .* (\d+)"),
    "gc_depth": re.compile(r"Garbage collection depth .* (\d+)"),
    "sync_retry_delay": re.compile(r"Sync retry delay .* (\d+)"),
    "sync_retry_nodes": re.compile(r"Sync retry nodes .* (\d+)"),
    "batch_size": re.compile(r"Batch size .* (\d+)"),
    "max_batch_delay": re.compile(r"Max batch delay .* (\d+)"),
    "consensus_protocol": re.compile(r"Consensus protocol set to ([a-z_]+)"),
}


def to_posix(timestamp: str) -> float:
    return datetime.timestamp(datetime.fromisoformat(timestamp.replace("Z", "+00:00")))


def merge_earliest(target: dict[str, float], key: str, value: float) -> None:
    old = target.get(key)
    if old is None or value < old:
        target[key] = value


def parse_client(path: Path) -> dict:
    size = rate = None
    start = None
    misses = 0
    samples: dict[int, float] = {}
    with path.open(errors="ignore") as f:
        for line in f:
            if size is None and "Transactions size:" in line:
                size = int(RE_CLIENT_SIZE.search(line).group(1))
            elif rate is None and "Transactions rate:" in line:
                rate = int(RE_CLIENT_RATE.search(line).group(1))
            elif start is None and "Start sending transactions" in line:
                start = to_posix(RE_CLIENT_START.search(line).group("ts"))
            elif "rate too high" in line:
                misses += 1
            elif "sample transaction" in line:
                m = RE_CLIENT_SAMPLE.search(line)
                if m:
                    samples[int(m.group(2))] = to_posix(m.group("ts"))
    if size is None or rate is None or start is None:
        raise ValueError(f"failed to parse client log {path}")
    return {"size": size, "rate": rate, "start": start, "misses": misses, "samples": samples}


def parse_primary(path: Path) -> dict:
    proposals: dict[str, float] = {}
    commits: dict[str, float] = {}
    configs: dict[str, int | str] = {}
    ip = None
    with path.open(errors="ignore") as f:
        for line in f:
            if "Created B" in line:
                m = RE_PRIMARY_CREATED.search(line)
                if m:
                    merge_earliest(proposals, m.group(2), to_posix(m.group("ts")))
            elif "Committed B" in line:
                m = RE_PRIMARY_COMMITTED.search(line)
                if m:
                    merge_earliest(commits, m.group(2), to_posix(m.group("ts")))
            elif ip is None and "booted on" in line:
                m = RE_PRIMARY_BOOT.search(line)
                if m:
                    ip = m.group(1)

            if len(configs) < len(RE_CONFIGS):
                for key, regex in RE_CONFIGS.items():
                    if key not in configs:
                        m = regex.search(line)
                        if m:
                            configs[key] = int(m.group(1)) if key != "consensus_protocol" else m.group(1)
    if not configs:
        raise ValueError(f"failed to parse primary config {path}")
    return {"proposals": proposals, "commits": commits, "configs": configs, "ip": ip}


def parse_worker(path: Path) -> dict:
    sizes: dict[str, int] = {}
    samples: dict[int, str] = {}
    ip = None
    with path.open(errors="ignore") as f:
        for line in f:
            if "contains sample tx" in line:
                m = RE_WORKER_SAMPLE.search(line)
                if m:
                    samples[int(m.group(2))] = m.group(1)
            elif "contains " in line and " B" in line:
                m = RE_WORKER_BATCH_SIZE.search(line)
                if m:
                    sizes[m.group(1)] = int(m.group(2))
            elif ip is None and "booted on" in line:
                m = RE_WORKER_BOOT.search(line)
                if m:
                    ip = m.group(1)
    return {"sizes": sizes, "samples": samples, "ip": ip}


def run_result_file(directory: Path, faults: int, workers: int, tx_size: int, protocol: str) -> Path:
    m = re.search(r"rate-(\d+)/r\d+-run(\d+)", str(directory))
    if not m:
        raise ValueError("cannot infer rate/run from directory; pass --output instead")
    rate, run = int(m.group(1)), int(m.group(2))
    primary_count = len(list(directory.glob("primary-*.log")))
    nodes = primary_count + faults
    return (
        directory.parents[1]
        / "results"
        / f"bench-{faults}-{nodes}-{workers}-True-{rate}-{tx_size}-{protocol}-run{run}.txt"
    )


def format_result(parsed: dict, faults: int) -> str:
    clients = parsed["clients"]
    configs = parsed["configs"]
    sizes = {k: v for k, v in parsed["sizes"].items() if k in parsed["commits"]}

    if parsed["commits"]:
        consensus_start = min(parsed["proposals"].values())
        consensus_end = max(parsed["commits"].values())
        consensus_duration = consensus_end - consensus_start
        bytes_ = sum(sizes.values())
        consensus_bps = bytes_ / consensus_duration
        consensus_tps = consensus_bps / clients[0]["size"]
        consensus_latency = mean(
            parsed["commits"][d] - parsed["proposals"][d]
            for d in parsed["commits"]
            if d in parsed["proposals"]
        )

        e2e_start = min(c["start"] for c in clients)
        e2e_end = consensus_end
        e2e_duration = e2e_end - e2e_start
        e2e_bps = bytes_ / e2e_duration
        e2e_tps = e2e_bps / clients[0]["size"]

        latencies = []
        for client, worker_samples in zip(clients, parsed["received_samples"]):
            for tx_id, batch_id in worker_samples.items():
                if batch_id in parsed["commits"] and tx_id in client["samples"]:
                    latencies.append(parsed["commits"][batch_id] - client["samples"][tx_id])
        e2e_latency = mean(latencies) if latencies else 0
    else:
        consensus_tps = consensus_bps = consensus_latency = 0
        e2e_tps = e2e_bps = e2e_latency = e2e_duration = 0

    collocate = set(parsed["primary_ips"]) == set(parsed["worker_ips"])
    workers = len(parsed["workers"]) // len(parsed["primaries"])
    return (
        "\n"
        "-----------------------------------------\n"
        " SUMMARY:\n"
        "-----------------------------------------\n"
        " + CONFIG:\n"
        f" Faults: {faults} node(s)\n"
        f" Committee size: {len(parsed['primaries']) + faults} node(s)\n"
        f" Worker(s) per node: {workers} worker(s)\n"
        f" Collocate primary and workers: {collocate}\n"
        f" Input rate: {sum(c['rate'] for c in clients):,} tx/s\n"
        f" Transaction size: {clients[0]['size']:,} B\n"
        f" Execution time: {round(e2e_duration):,} s\n\n"
        f" Header size: {configs['header_size']:,} B\n"
        f" Max header delay: {configs['max_header_delay']:,} ms\n"
        f" GC depth: {configs['gc_depth']:,} round(s)\n"
        f" Sync retry delay: {configs['sync_retry_delay']:,} ms\n"
        f" Sync retry nodes: {configs['sync_retry_nodes']:,} node(s)\n"
        f" batch size: {configs['batch_size']:,} B\n"
        f" Max batch delay: {configs['max_batch_delay']:,} ms\n"
        f" Consensus protocol: {configs['consensus_protocol']}\n\n"
        " + RESULTS:\n"
        f" Consensus TPS: {round(consensus_tps):,} tx/s\n"
        f" Consensus BPS: {round(consensus_bps):,} B/s\n"
        f" Consensus latency: {round(consensus_latency * 1_000):,} ms\n\n"
        f" End-to-end TPS: {round(e2e_tps):,} tx/s\n"
        f" End-to-end BPS: {round(e2e_bps):,} B/s\n"
        f" End-to-end latency: {round(e2e_latency * 1_000):,} ms\n"
        "-----------------------------------------\n"
    )


def parse_directory(directory: Path) -> dict:
    clients = [parse_client(p) for p in sorted(directory.glob("client-*.log"))]
    primaries = [parse_primary(p) for p in sorted(directory.glob("primary-*.log"))]
    workers = [parse_worker(p) for p in sorted(directory.glob("worker-*.log"))]
    proposals: dict[str, float] = {}
    commits: dict[str, float] = {}
    for primary in primaries:
        for key, value in primary["proposals"].items():
            merge_earliest(proposals, key, value)
        for key, value in primary["commits"].items():
            merge_earliest(commits, key, value)
    sizes = {k: v for worker in workers for k, v in worker["sizes"].items()}
    return {
        "clients": clients,
        "primaries": primaries,
        "workers": workers,
        "proposals": proposals,
        "commits": commits,
        "configs": primaries[0]["configs"],
        "primary_ips": [p["ip"] for p in primaries],
        "worker_ips": [w["ip"] for w in workers],
        "sizes": sizes,
        "received_samples": [w["samples"] for w in workers],
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("directories", nargs="+", type=Path)
    parser.add_argument("--faults", type=int, default=0)
    parser.add_argument("--protocol", default="wahoo")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--write-result", action="store_true")
    args = parser.parse_args()

    for directory in args.directories:
        parsed = parse_directory(directory)
        result = format_result(parsed, args.faults)
        print(f"\n=== {directory} ===")
        print(result)
        if args.write_result or args.output:
            output = args.output or run_result_file(
                directory,
                args.faults,
                len(parsed["workers"]) // len(parsed["primaries"]),
                parsed["clients"][0]["size"],
                args.protocol,
            )
            output.parent.mkdir(parents=True, exist_ok=True)
            output.write_text(result)
            print(f"Wrote {output}")


if __name__ == "__main__":
    main()
