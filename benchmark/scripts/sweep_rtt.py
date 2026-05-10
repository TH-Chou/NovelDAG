#!/usr/bin/env python3
"""
Comprehensive RTT latency sweep: NovelDAG vs Narwhal
  n=10, f ∈ {0, 1, 3}
  one-way delay ∈ {0, 50, 100} ms
  rates: 60K → 330K step 30K
  5 runs each, median of middle 3
"""

import subprocess
import sys
import os
import json
import statistics
import csv
import time
from datetime import datetime
from pathlib import Path

BENCH_DIR = Path("/Users/apple/Documents/NovelDAG/benchmark")

DELAYS = [0, 50, 100]       # one-way ms
FAULTS = [0, 1, 3]
PROTOCOLS = ["narwhal", "noveldag"]
RATES = list(range(60000, 331000, 30000))  # 60K..330K step 30K
RUNS = 5

CSV_PATH = BENCH_DIR / "rtt_sweep_results.csv"
PROGRESS_PATH = BENCH_DIR / "rtt_sweep_progress.json"


def run_osascript(cmd: str) -> bool:
    """Run a shell command with admin privileges. Returns True on success."""
    script = f'do shell script "{cmd}" with administrator privileges'
    try:
        subprocess.run(
            ["osascript", "-e", script],
            check=True, capture_output=True, text=True, timeout=30,
        )
        return True
    except subprocess.CalledProcessError as e:
        print(f"  [osascript ERROR] {e.stderr.strip()}")
        return False


PF_RULES_FILE = "/tmp/pf_delay.conf"

def configure_delay(ms: int):
    """Set dummynet one-way delay. 0 = disable."""
    if ms == 0:
        print("  Disabling dummynet...")
        run_osascript("pfctl -d")
        run_osascript("dnctl -q flush")
    else:
        print(f"  Setting dummynet delay={ms}ms...")
        # Write pf rules
        Path(PF_RULES_FILE).write_text(
            "dummynet in  proto tcp from any to 127.0.0.0/8 pipe 1\n"
            "dummynet out proto tcp from any to 127.0.0.0/8 pipe 1\n"
        )
        run_osascript("pfctl -e")
        run_osascript(f"dnctl pipe 1 config delay {ms}")
        run_osascript(f"pfctl -f {PF_RULES_FILE}")


def kill_all():
    """Kill all node processes and tmux sessions."""
    subprocess.run(["pkill", "-9", "-f", "node "], stderr=subprocess.DEVNULL)
    subprocess.run(["tmux", "kill-server"], stderr=subprocess.DEVNULL)
    time.sleep(2)


def run_benchmark(proto: str, faults: int, rate: int) -> dict | None:
    """Run a single benchmark. Returns dict with tps/latency or None on failure."""
    code = f"""
from benchmark.local import LocalBench
from benchmark.utils import Print
import json

bench = {{
    'faults': {faults},
    'nodes': 10,
    'workers': 1,
    'rate': {rate},
    'tx_size': 512,
    'duration': 20,
}}
node = {{
    'header_size': 1_000,
    'max_header_delay': 200,
    'gc_depth': 50,
    'sync_retry_delay': 10_000,
    'sync_retry_nodes': 3,
    'batch_size': 500_000,
    'max_batch_delay': 200,
    'consensus_protocol': 'round_robin',
    'dag_protocol': '{proto}',
}}
try:
    parser = LocalBench(bench, node).run(debug=False)
    m = parser.metrics()
    print("METRICS_JSON:" + json.dumps(m))
except Exception as e:
    print(f"FAILED: {{e}}")
"""
    try:
        result = subprocess.run(
            ["python3", "-c", code],
            capture_output=True, text=True, timeout=180,
            cwd=str(BENCH_DIR),
        )
        stdout = result.stdout
        # Extract METRICS_JSON line
        for line in stdout.splitlines():
            if line.startswith("METRICS_JSON:"):
                return json.loads(line[len("METRICS_JSON:"):])
        # Check for failure
        if "FAILED" in stdout:
            print(f"    FAILED: {stdout.strip()}")
            return None
        print(f"    Unexpected output: {stdout[:200]}")
        return None
    except subprocess.TimeoutExpired:
        print("    TIMEOUT")
        kill_all()
        return None
    except Exception as e:
        print(f"    ERROR: {e}")
        return None


def stable_mean(values: list[float]) -> tuple[float, float, bool]:
    """
    Sort 5 values, take middle 3, return (mean, stdev, has_outlier).
    Outlier = min or max deviates >30% from middle-3 mean.
    """
    if len(values) < 3:
        return (sum(values) / len(values), 0, True)
    sorted_vals = sorted(values)
    middle = sorted_vals[1:4] if len(sorted_vals) >= 5 else sorted_vals[1:-1] if len(sorted_vals) > 3 else sorted_vals
    mid_mean = statistics.mean(middle)
    mid_stdev = statistics.stdev(middle) if len(middle) > 1 else 0
    has_outlier = False
    if len(sorted_vals) >= 5 and mid_mean > 0:
        if abs(sorted_vals[0] - mid_mean) / mid_mean > 0.3:
            has_outlier = True
        if abs(sorted_vals[-1] - mid_mean) / mid_mean > 0.3:
            has_outlier = True
    return mid_mean, mid_stdev, has_outlier


def load_progress() -> dict:
    if PROGRESS_PATH.exists():
        return json.loads(PROGRESS_PATH.read_text())
    return {"completed": [], "current_delay": None}


def save_progress(progress: dict):
    PROGRESS_PATH.write_text(json.dumps(progress, indent=2))


def config_key(delay, faults, proto, rate) -> str:
    return f"d{delay}_f{faults}_{proto}_r{rate}"


def main():
    progress = load_progress()
    completed = set(progress.get("completed", []))

    # Load existing CSV or create new
    csv_exists = CSV_PATH.exists()
    csv_file = open(CSV_PATH, "a", newline="")
    writer = csv.writer(csv_file)
    if not csv_exists:
        writer.writerow(["protocol", "faults", "delay_ms", "rate", "run",
                          "tps", "latency_ms", "is_outlier"])

    total_configs = len(DELAYS) * len(FAULTS) * len(PROTOCOLS) * len(RATES)
    config_num = 0

    for delay in DELAYS:
        print(f"\n{'='*60}")
        print(f"DELAY = {delay}ms one-way ({delay*2}ms RTT)")
        print(f"{'='*60}")
        configure_delay(delay)
        time.sleep(1)

        # Verify delay
        if delay > 0:
            verify = subprocess.run(
                ["osascript", "-e", 'do shell script "dnctl show" with administrator privileges'],
                capture_output=True, text=True,
            )
            if f"{delay} ms" in verify.stdout:
                print(f"  [OK] dummynet confirmed: {delay}ms")
            else:
                print(f"  [WARN] dummynet may not be active!")

        for faults in FAULTS:
            for proto in PROTOCOLS:
                for rate in RATES:
                    config_num += 1
                    key = config_key(delay, faults, proto, rate)

                    if key in completed:
                        print(f"[{config_num}/{total_configs}] SKIP {key} (already done)")
                        continue

                    print(f"\n[{config_num}/{total_configs}] {key}")
                    tps_vals = []
                    lat_vals = []

                    for run in range(1, RUNS + 1):
                        ts = datetime.now().strftime("%H:%M:%S")
                        print(f"  [{ts}] Run {run}/{RUNS}...", end=" ", flush=True)
                        kill_all()
                        res = run_benchmark(proto, faults, rate)

                        if res is None:
                            print("FAILED -> will retry")
                            # Retry once
                            time.sleep(3)
                            kill_all()
                            print("  Retry...", end=" ", flush=True)
                            res = run_benchmark(proto, faults, rate)

                        if res:
                            tps = res.get("consensus_tps", 0)
                            lat = res.get("consensus_latency_ms", 0)
                            tps_vals.append(tps)
                            lat_vals.append(lat)
                            print(f"TPS={tps:.0f} Lat={lat:.1f}ms")
                        else:
                            tps_vals.append(0)
                            lat_vals.append(0)
                            print("NO RESULT")

                    # Compute stable means
                    if len([v for v in tps_vals if v > 0]) >= 3:
                        tps_mean, tps_std, tps_outlier = stable_mean(
                            [v for v in tps_vals if v > 0])
                        lat_mean, lat_std, lat_outlier = stable_mean(
                            [v for v in lat_vals if v > 0])
                    else:
                        tps_mean = statistics.mean(tps_vals) if tps_vals else 0
                        tps_std = 0
                        tps_outlier = True
                        lat_mean = statistics.mean(lat_vals) if lat_vals else 0
                        lat_std = 0
                        lat_outlier = True

                    # Write to CSV
                    for i, (t, l) in enumerate(zip(tps_vals, lat_vals)):
                        is_out = (i == 0 and abs(tps_vals[0] - tps_mean) / max(tps_mean, 1) > 0.3) or \
                                 (i == len(tps_vals)-1 and abs(tps_vals[-1] - tps_mean) / max(tps_mean, 1) > 0.3)
                        writer.writerow([proto, faults, delay, rate, i+1, t, l, is_out])

                    # Write aggregate row
                    writer.writerow([proto, faults, delay, rate, "mean", tps_mean, lat_mean,
                                     "outlier" if tps_outlier else "stable"])
                    csv_file.flush()

                    print(f"  => Mean TPS={tps_mean:.0f} Lat={lat_mean:.1f}ms "
                          f"{'[OUTLIER]' if tps_outlier else '[OK]'}")

                    completed.add(key)
                    progress["completed"] = list(completed)
                    progress["last_update"] = datetime.now().isoformat()
                    save_progress(progress)

    csv_file.close()
    print(f"\n=== ALL DONE ===")
    print(f"Results: {CSV_PATH}")


if __name__ == "__main__":
    main()
