#!/usr/bin/env python3
"""Smoke: narwhal/noveldag/wahoo x {200K,300K} x header_size={1KB,500KB} at d=100ms.
Compares max_header_delay=200ms vs 10000ms (effectively infinite).
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


def run(proto, rate, delay_ms, header_size, max_header_delay, duration=30):
    code = f"""
from benchmark.local import LocalBench
import json
bench = {{'faults': 0, 'nodes': 10, 'workers': 1,
         'rate': {rate}, 'tx_size': 512, 'duration': {duration}}}
node = {{'header_size': {header_size}, 'max_header_delay': {max_header_delay}, 'gc_depth': 50,
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
        print(f"  ERR: {proc.stderr[:200]}")
    return None


def main():
    protos = ["narwhal", "noveldag", "wahoo"]
    rates = [200000, 300000]
    delay = 100
    header_sizes = [1000, 500_000]
    mhds = [200, 10000]

    cfgs = [(mhd, hs, p, r) for mhd in mhds for hs in header_sizes for p in protos for r in rates]
    results = {}

    print(f"\n=== max_header_delay comparison: d={delay}ms, {len(cfgs)} runs, 30s each ===\n")

    # Apply delay once for all runs since it's constant
    configure_delay(delay)
    time.sleep(1)

    for i, (mhd, hs, proto, rate) in enumerate(cfgs, 1):
        kill_all()
        hs_label = f"{hs//1000}KB"
        mhd_label = f"{mhd}ms" if mhd < 5000 else "inf"
        print(f"[{i:>2}/{len(cfgs)}] {proto:>8s} hs={hs_label:>5s} mhd={mhd_label:>5s} rate={rate//1000}K ... ", end="", flush=True)
        m = run(proto, rate, delay, hs, mhd)
        if not m:
            print("FAILED")
            results[(mhd, hs, proto, rate)] = None
            continue
        tps = m.get('consensus_tps', 0)
        lat = m.get('consensus_latency_ms', 0)
        print(f"TPS={tps:.0f} Lat={lat:.0f}ms")
        results[(mhd, hs, proto, rate)] = (tps, lat)

    configure_delay(0)

    for rate in rates:
        print(f"\n{'='*100}")
        print(f"rate={rate//1000}K, d={delay}ms")
        print(f"{'='*100}")
        for hs in header_sizes:
            hs_label = f"header_size={hs//1000}KB"
            print(f"\n  {hs_label}:")
            print(f"  {'proto':>8s}  {'mhd=200ms':>22s}  {'mhd=inf':>22s}  {'delta':>15s}")
            print(f"  {'':>8s}  {'TPS':>7s}  {'Lat':>6s}    {'TPS':>7s}  {'Lat':>6s}    {'TPS':>8s}  {'Lat':>8s}")
            print(f"  {'-'*80}")
            for p in protos:
                r200 = results.get((200, hs, p, rate))
                rinf = results.get((10000, hs, p, rate))
                if r200 and rinf:
                    dtps = (rinf[0] - r200[0]) / r200[0] * 100
                    dlat = (rinf[1] - r200[1]) / r200[1] * 100
                    print(f"  {p:>8s}  {r200[0]:>7.0f}  {r200[1]:>5.0f}ms   {rinf[0]:>7.0f}  {rinf[1]:>5.0f}ms   {dtps:>+7.0f}%  {dlat:>+7.0f}%")
                elif r200:
                    print(f"  {p:>8s}  {r200[0]:>7.0f}  {r200[1]:>5.0f}ms   {'FAIL':>7s}  {'-':>5s}")
                elif rinf:
                    print(f"  {p:>8s}  {'FAIL':>7s}  {'-':>5s}     {rinf[0]:>7.0f}  {rinf[1]:>5.0f}ms")


if __name__ == "__main__":
    main()
