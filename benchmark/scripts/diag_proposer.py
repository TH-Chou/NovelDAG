#!/usr/bin/env python3
"""Run one short NovelDAG benchmark and extract DIAG_PROPOSER_GATE lines
from primary logs to find the critical-path gate."""
import json, subprocess, time, re, glob
from pathlib import Path

BENCH_DIR = Path("/Users/apple/Documents/NovelDAG/benchmark")

def kill_all():
    subprocess.run(["pkill", "-9", "-f", "node "], stderr=subprocess.DEVNULL)
    subprocess.run(["tmux", "kill-server"], stderr=subprocess.DEVNULL)
    time.sleep(2)

def run(rate, header_delay=2000, proto='noveldag'):
    code = f"""
from benchmark.local import LocalBench
import json
bench = {{'faults': 0, 'nodes': 10, 'workers': 1,
         'rate': {rate}, 'tx_size': 512, 'duration': 30}}
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
    print("FAILED:", proc.stdout[-2000:])
    return None

def extract_diag():
    logs = sorted(glob.glob(str(BENCH_DIR / "logs" / "primary-*.log")))
    results = []
    pattern = re.compile(r"DIAG_PROPOSER_GATE (.+)")
    for log in logs:
        node_name = Path(log).stem
        last_match = None
        with open(log) as f:
            for line in f:
                m = pattern.search(line)
                if m:
                    last_match = m.group(1)
        if last_match:
            kv = dict(re.findall(r"(\w+)=(\S+)", last_match))
            kv["node"] = node_name
            results.append(kv)
    return results

def summarize_diag():
    diag = extract_diag()
    if not diag:
        return None
    sums = {"p1": 0, "p2": 0, "qc": 0, "period": 0, "block": 0,
            "miss_p1": 0, "miss_p2": 0, "miss_qc": 0, "blk_attempts": 0, "n": 0}
    for r in diag:
        sums["p1"] += int(r.get("avg_p1_wait_ms", 0))
        sums["p2"] += int(r.get("avg_p2_wait_ms", 0))
        sums["qc"] += int(r.get("avg_qc_wait_ms", 0))
        sums["period"] += int(r.get("avg_round_period_ms", 0))
        sums["block"] += int(r.get("blocked_total_ms", 0))
        sums["miss_p1"] += int(r.get("missing_parents_1", 0))
        sums["miss_p2"] += int(r.get("missing_parents_2", 0))
        sums["miss_qc"] += int(r.get("missing_qc", 0))
        sums["blk_attempts"] += int(r.get("blocked_attempts", 0))
        sums["n"] += 1
    n = sums["n"]
    return {k: v // n if k in ("p1","p2","qc","period") else v for k, v in sums.items()}

def main():
    # Test whether max_header_delay affects Wahoo's commit latency.
    cfg = [
        ('wahoo',    150000, 200),
        ('wahoo',    240000, 200),
        ('wahoo',    150000, 160),
        ('wahoo',    240000, 160),
    ]
    results = []
    for proto, rate, delay in cfg:
        kill_all()
        print(f"\n=== {proto} f=0 rate={rate} max_header_delay={delay} duration=30s ===")
        m = run(rate, header_delay=delay, proto=proto)
        if not m:
            print("  Bench failed, skipping")
            continue
        tps = m.get('consensus_tps', 0)
        lat = m.get('consensus_latency_ms', 0)
        print(f"  TPS={tps:.0f} Lat={lat:.1f}ms")
        results.append((proto, delay, rate, tps, lat))

    print("\n\n=== SUMMARY ===")
    print(f"{'proto':<10} {'delay':>6} {'rate':>8} {'TPS':>10} {'Lat(ms)':>9}")
    print("-" * 50)
    for proto, delay, rate, tps, lat in results:
        print(f"{proto:<10} {delay:>6} {rate:>8} {tps:>10,.0f} {lat:>9.0f}")

if __name__ == "__main__":
    main()
