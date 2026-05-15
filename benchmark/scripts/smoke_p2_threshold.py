#!/usr/bin/env python3
"""Smoke: narwhal/noveldag/wahoo x d={0,50,100} x rate={200K,300K}.
Tests NovelDAG with parents_2=f+1 AND QC soft-gating.
Applies REAL dummynet network delay."""
import json, os, subprocess, time
from pathlib import Path

BENCH_DIR = Path("/Users/apple/Documents/NovelDAG/benchmark")
PF_RULES_FILE = "/tmp/pf_delay.conf"
SUDO_PASSWORD = os.environ.get("SWEEP_SUDO_PASSWORD", "561280")


def sudo_run(cmd: list[str], timeout: int = 20) -> tuple[bool, str]:
    full = ["sudo", "-S"] + cmd
    try:
        proc = subprocess.run(
            full, input=SUDO_PASSWORD + "\n", text=True,
            capture_output=True, timeout=timeout,
        )
        return (proc.returncode == 0, proc.stdout + proc.stderr)
    except subprocess.TimeoutExpired:
        return (False, "TIMEOUT")


def configure_delay(ms: int):
    """Set dummynet one-way delay; ms==0 disables."""
    if ms == 0:
        print("  Disabling dummynet...")
        sudo_run(["pfctl", "-d"])
        sudo_run(["dnctl", "-q", "flush"])
        return
    print(f"  Setting dummynet delay={ms}ms (RTT={ms*2}ms)...")
    Path(PF_RULES_FILE).write_text(
        "dummynet in  proto tcp from any to 127.0.0.0/8 pipe 1\n"
        "dummynet out proto tcp from any to 127.0.0.0/8 pipe 1\n"
    )
    sudo_run(["pfctl", "-e"])
    sudo_run(["dnctl", "pipe", "1", "config", "delay", str(ms)])
    sudo_run(["pfctl", "-f", PF_RULES_FILE])
    ok, out = sudo_run(["dnctl", "show"])
    if ok and f"{ms} ms" in out:
        print(f"  [OK] dummynet confirmed: {ms}ms")
    else:
        print(f"  [WARN] dummynet may not be active")


def kill_all():
    subprocess.run(["pkill", "-9", "-f", "node "], stderr=subprocess.DEVNULL)
    subprocess.run(["tmux", "kill-server"], stderr=subprocess.DEVNULL)
    time.sleep(2)


def run(proto, rate, duration=30):
    code = f"""
from benchmark.local import LocalBench
import json
bench = {{'faults': 0, 'nodes': 10, 'workers': 1,
         'rate': {rate}, 'tx_size': 512, 'duration': {duration}}}
node = {{'header_size': 1000, 'max_header_delay': 200, 'gc_depth': 50,
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
                          capture_output=True, text=True, timeout=300)
    for line in proc.stdout.splitlines():
        if line.startswith("METRICS_JSON:"):
            return json.loads(line[len("METRICS_JSON:"):])
    if proc.stderr:
        print(f"  stderr: {proc.stderr[:300]}")
    return None


def main():
    protos = ["narwhal", "noveldag", "wahoo"]
    delays = [0, 50, 100]
    rates = [200000, 300000]
    cfgs = [(d, p, r) for d in delays for p in protos for r in rates]

    print(f"\n=== NovelDAG optimized (p2=f+1 + QC soft-gate): {len(cfgs)} runs, 30s each ===\n")
    results = {}

    for i, (delay, proto, rate) in enumerate(cfgs, 1):
        kill_all()
        configure_delay(delay)
        time.sleep(1)
        label = f"d={delay}ms (RTT={delay*2}ms)"
        print(f"[{i:>2}/{len(cfgs)}] {proto:>8s} {label} rate={rate//1000}K ... ", end="", flush=True)
        m = run(proto, rate)
        if not m:
            print("FAILED")
            results[(proto, delay, rate)] = None
            continue
        tps = m.get('consensus_tps', 0)
        lat = m.get('consensus_latency_ms', 0)
        print(f"TPS={tps:.0f} Lat={lat:.0f}ms")
        results[(proto, delay, rate)] = (tps, lat)

    configure_delay(0)

    # Summary tables
    for rate in rates:
        print(f"\n{'='*100}")
        print(f"rate={rate//1000}K  (NovelDAG with p2_threshold=f+1 + QC soft-gating)")
        print(f"{'='*100}")
        for delay in delays:
            nd = results.get(('noveldag', delay, rate))
            nw = results.get(('narwhal', delay, rate))
            wh = results.get(('wahoo', delay, rate))
            print(f"\n  d={delay}ms (RTT={delay*2}ms):")
            row = f"    {'Proto':>8s}  {'TPS':>9s}  {'Latency':>8s}"
            if nd and nw:
                row += f"  {'ND vs NW':>18s}"
            if nd and wh:
                row += f"  {'ND vs WH':>18s}"
            print(row)
            print(f"    {'-'*70}")
            for p, label in [('narwhal','NW'), ('noveldag','ND'), ('wahoo','WH')]:
                r = results.get((p, delay, rate))
                if r:
                    print(f"    {label:>8s}  {r[0]:>8.0f}  {r[1]:>6.0f}ms")
            if nd and nw:
                tps_adv = (nd[0]-nw[0])/nw[0]*100
                lat_adv = (nw[1]-nd[1])/nw[1]*100
                print(f"    {'ND vs NW':>8s}  TPS {tps_adv:+.0f}%  Lat {lat_adv:+.0f}%")
            if nd and wh:
                tps_adv = (nd[0]-wh[0])/wh[0]*100
                lat_adv = (wh[1]-nd[1])/wh[1]*100
                print(f"    {'ND vs WH':>8s}  TPS {tps_adv:+.0f}%  Lat {lat_adv:+.0f}%")


if __name__ == "__main__":
    main()
