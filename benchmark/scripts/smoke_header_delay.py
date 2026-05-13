#!/usr/bin/env python3
"""Smoke test: three protocols × {200ms, 160ms} max_header_delay
at peak-rate region to confirm reduced timer preserves peak TPS."""
import json, subprocess, time
from pathlib import Path

BENCH_DIR = Path("/Users/apple/Documents/NovelDAG/benchmark")

def kill_all():
    subprocess.run(["pkill", "-9", "-f", "node "], stderr=subprocess.DEVNULL)
    subprocess.run(["tmux", "kill-server"], stderr=subprocess.DEVNULL)
    time.sleep(2)

def run(proto, rate, header_delay, duration=30):
    code = f"""
from benchmark.local import LocalBench
import json
bench = {{'faults': 0, 'nodes': 10, 'workers': 1,
         'rate': {rate}, 'tx_size': 512, 'duration': {duration}}}
node = {{'header_size': 1000, 'max_header_delay': {header_delay}, 'gc_depth': 50,
        'sync_retry_delay': 10000, 'sync_retry_nodes': 3,
        'batch_size': 500000, 'max_batch_delay': 200,
        'consensus_protocol': 'round_robin', 'dag_protocol': '{proto}'}}
try:
    m = LocalBench(bench, node).run(debug=False).metrics()
    print('METRICS_JSON:' + json.dumps(m))
except Exception as e:
    print(f'FAILED: {{e}}')
"""
    proc = subprocess.run(["python3", "-c", code], cwd=str(BENCH_DIR),
                          capture_output=True, text=True, timeout=180)
    for line in proc.stdout.splitlines():
        if line.startswith("METRICS_JSON:"):
            return json.loads(line[len("METRICS_JSON:"):])
    return None

CSV_OUT = BENCH_DIR / "header_delay_sweep.csv"

def main():
    protos = ["narwhal", "noveldag", "wahoo"]
    rates  = [60000, 150000, 240000, 300000]
    delays = [140, 120, 100]
    # Extra: include 200/160 at 300K to complete the matrix from previous runs.
    extras = [(p, d, 300000) for p in protos for d in (200, 160)]

    cfgs = [(p, d, r) for d in delays for p in protos for r in rates] + extras

    print(f"\nSmoke: {len(cfgs)} runs, 30s each\n")
    import csv
    with open(CSV_OUT, "a", newline="") as f:
        writer = csv.writer(f)
        if CSV_OUT.stat().st_size == 0:
            writer.writerow(["proto", "delay_ms", "rate", "tps", "latency_ms"])
        for i, (proto, delay, rate) in enumerate(cfgs, 1):
            kill_all()
            print(f"[{i}/{len(cfgs)}] {proto} delay={delay}ms rate={rate} ... ", end="", flush=True)
            m = run(proto, rate, delay)
            if not m:
                print("FAILED")
                writer.writerow([proto, delay, rate, 0, 0])
                f.flush()
                continue
            tps = m.get('consensus_tps', 0)
            lat = m.get('consensus_latency_ms', 0)
            print(f"TPS={tps:.0f} Lat={lat:.0f}ms")
            writer.writerow([proto, delay, rate, tps, lat])
            f.flush()
    print(f"\nResults saved to {CSV_OUT}")

if __name__ == "__main__":
    main()
