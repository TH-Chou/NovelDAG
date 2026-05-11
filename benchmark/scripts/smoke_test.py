#!/usr/bin/env python3
"""Smoke test: run all 4 DagProtocol variants locally and report TPS/latency.

Usage:  python3 benchmark/smoke_test.py
"""

import subprocess, sys, time, json
from os.path import join
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser
from benchmark.utils import PathMaker, Print


SMOKE_DURATION = 20  # seconds
NODES = 4
WORKERS = 1
PORT = 9000
RATE = 1000
TX_SIZE = 512

# Set after build
BIN_NODE = None
BIN_CLIENT = None


def _bin(cmd: str) -> str:
    """Replace ./node and ./benchmark_client with full paths."""
    return cmd.replace("./node ", BIN_NODE + " ").replace("./benchmark_client ", BIN_CLIENT + " ")

PARAMS = {
    "header_size": 1000,
    "max_header_delay": 2000,
    "gc_depth": 50,
    "sync_retry_delay": 5000,
    "sync_retry_nodes": 3,
    "batch_size": 500,
    "max_batch_delay": 2000,
    "consensus_protocol": "round_robin",
}

PROTOCOLS = ["narwhal", "bullshark", "noveldag", "wahoo"]


def kill_all():
    """Aggressively kill all node/benchmark processes and free ports."""
    # Kill tmux sessions
    try:
        subprocess.run("tmux kill-server", shell=True,
                       stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    except Exception:
        pass
    time.sleep(2)
    # Kill any lingering processes holding benchmark ports
    import os
    for port in range(PORT, PORT + 100):
        try:
            result = subprocess.run(f"lsof -ti :{port}", shell=True,
                                    capture_output=True, text=True)
            for pid in result.stdout.strip().split():
                os.kill(int(pid), 9)
        except Exception:
            pass
    time.sleep(2)


def clean():
    subprocess.run(f"{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}",
                   shell=True, stderr=subprocess.DEVNULL)
    time.sleep(0.5)


def run_bench(dag_protocol: str) -> dict:
    """Run a single benchmark and return metrics dict."""
    kill_all()
    clean()

    # ---- Generate configs ----
    from benchmark.config import Key, LocalCommittee, NodeParameters

    keys = []
    for i in range(NODES):
        kf = PathMaker.key_file(i)
        subprocess.run(_bin(CommandMaker.generate_key(kf)).split(),
                       check=True, capture_output=True)
        keys.append(Key.from_file(kf))

    names = [k.name for k in keys]
    committee = LocalCommittee(names, PORT, WORKERS)
    committee.print(PathMaker.committee_file())

    p = dict(PARAMS)
    p["dag_protocol"] = dag_protocol
    NodeParameters(p).print(PathMaker.parameters_file())

    # ---- Start clients ----
    workers_addrs = committee.workers_addresses(0)
    active = sum(len(a) for a in workers_addrs)
    rate_share = -(RATE // -active)  # ceil division
    all_worker_addrs = [x for y in workers_addrs for _, x in y]

    for i, addrs in enumerate(workers_addrs):
        for wid, addr in addrs:
            cmd = _bin(CommandMaker.run_client(addr, TX_SIZE, rate_share, all_worker_addrs))
            log = PathMaker.client_log_file(i, wid)
            name = f"c-{dag_protocol[:4]}-{i}-{wid}"
            subprocess.run(["tmux", "new", "-d", "-s", name,
                            f"{cmd} 2> {log}"], check=True)

    # ---- Start primaries ----
    for i, _ in enumerate(committee.primary_addresses(0)):
        cmd = _bin(CommandMaker.run_primary(
            PathMaker.key_file(i),
            PathMaker.committee_file(),
            PathMaker.db_path(i),
            PathMaker.parameters_file(),
            debug=False,
        ))
        log = PathMaker.primary_log_file(i)
        name = f"p-{dag_protocol[:4]}-{i}"
        subprocess.run(["tmux", "new", "-d", "-s", name,
                        f"{cmd} 2> {log}"], check=True)

    # ---- Start workers ----
    for i, addrs in enumerate(workers_addrs):
        for wid, addr in addrs:
            cmd = _bin(CommandMaker.run_worker(
                PathMaker.key_file(i),
                PathMaker.committee_file(),
                PathMaker.db_path(i, wid),
                PathMaker.parameters_file(),
                wid, debug=False,
            ))
            log = PathMaker.worker_log_file(i, wid)
            name = f"w-{dag_protocol[:4]}-{i}-{wid}"
            subprocess.run(["tmux", "new", "-d", "-s", name,
                            f"{cmd} 2> {log}"], check=True)

    # ---- Wait ----
    Print.info(f"Running {dag_protocol} for {SMOKE_DURATION}s...")
    time.sleep(SMOKE_DURATION + 5)
    kill_all()
    time.sleep(1)

    # ---- Parse ----
    try:
        parser = LogParser.process(PathMaker.logs_path(), faults=0)
        return parser.metrics()
    except Exception as e:
        Print.warn(f"  Parse failed: {e}")
        return {"dag_protocol": dag_protocol, "consensus_tps": 0,
                "consensus_latency_ms": 0}


def main():
    print("=" * 65)
    print("  NovelDAG Smoke Test — 4 Protocols, Local Benchmark")
    print(f"  Nodes: {NODES}  Workers: {WORKERS}  Duration: {SMOKE_DURATION}s")
    print("=" * 65)

    # Build once
    Print.info("Building with --features benchmark ...")
    result = subprocess.run(
        CommandMaker.compile().split(),
        cwd=PathMaker.node_crate_path(),
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        # Try without recompile — maybe already built
        print("  Build warning (may already be built):")
        print(result.stderr[-500:])

    # Use binaries directly from target/release/ to avoid
    # alias_binaries's 'rm -rf node' destroying the source crate.
    global BIN_NODE, BIN_CLIENT
    BIN_NODE = join(PathMaker.binary_path(), "node")
    BIN_CLIENT = join(PathMaker.binary_path(), "benchmark_client")

    results = {}
    for proto in PROTOCOLS:
        m = run_bench(proto)
        results[proto] = m
        tps = m.get("consensus_tps", 0)
        lat = m.get("consensus_latency_ms", 0)
        e2e_tps = m.get("end_to_end_tps", 0)
        e2e_lat = m.get("end_to_end_latency_ms", 0)
        print(f"  {proto:>12s}:  consensus {tps:>8.1f} tps  "
              f"{lat:>7.1f} ms  |  e2e {e2e_tps:>8.1f} tps  "
              f"{e2e_lat:>7.1f} ms")

    # Summary
    print("\n" + "=" * 65)
    print(f"  {'Protocol':<12s}  {'Consensus TPS':>15s}  {'Latency (ms)':>14s}")
    print("  " + "-" * 44)
    all_ok = True
    for proto in PROTOCOLS:
        tps = results[proto].get("consensus_tps", 0)
        lat = results[proto].get("consensus_latency_ms", 0)
        print(f"  {proto:<12s}  {tps:>15.1f}  {lat:>14.1f}")
        if tps == 0:
            all_ok = False
    print("=" * 65)

    if all_ok:
        print("\n  ALL PROTOCOLS OK.")
    else:
        failed = [p for p in PROTOCOLS if results[p].get("consensus_tps", 0) == 0]
        print(f"\n  WARNING: zero TPS for: {failed}")
        if "wahoo" in failed:
            print("  (Wahoo generates synthetic txs — check that Created/Committed logs appear)")

    kill_all()


if __name__ == "__main__":
    main()
