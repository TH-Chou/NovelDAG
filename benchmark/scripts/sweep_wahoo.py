#!/usr/bin/env python3
"""Wahoo RTT latency sweep — appends to the same CSV used by sweep_rtt.py
so plot_all_charts.py can compare all three protocols on identical
(faults, delay_ms, rate) cells.

Mirrors `sweep_rtt.py` exactly:
  n=10, f in {0, 1, 3}
  one-way delay in {0, 50, 100} ms
  rates 60K..330K step 30K
  5 runs each, robust mean over middle 3
"""

import subprocess
import sys
import os
import json
import statistics
import csv
import time
import builtins
from datetime import datetime
from pathlib import Path

BENCH_DIR = Path("/Users/apple/Documents/NovelDAG/benchmark")
LOG_PATH = BENCH_DIR / "sweep_wahoo.log"

# Tee every print to LOG_PATH so the user can `tail -f` from another
# terminal while the sweep runs unattended.
_LOG_FILE = open(LOG_PATH, "a", buffering=1)
_orig_print = builtins.print

def print(*args, **kwargs):  # noqa: A001 - intentional shadow
    kwargs.setdefault("flush", True)
    _orig_print(*args, **kwargs)
    try:
        msg = kwargs.get("sep", " ").join(str(a) for a in args)
        end = kwargs.get("end", "\n")
        _LOG_FILE.write(msg + end)
        _LOG_FILE.flush()
    except Exception:
        pass

DELAYS = [0, 50, 100]
FAULTS = [0, 1, 3]
PROTOCOLS = ["wahoo"]
RATES = list(range(60000, 331000, 30000))
RUNS = 5

# Same CSV file as sweep_rtt.py — we append.
CSV_PATH = BENCH_DIR / "csv_plots" / "rtt_sweep_results.csv"
PROGRESS_PATH = BENCH_DIR / "wahoo_sweep_progress.json"

PF_RULES_FILE = "/tmp/pf_delay.conf"


def run_osascript(cmd: str) -> bool:
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


def configure_delay(ms: int):
    if ms == 0:
        print("  Disabling dummynet...")
        run_osascript("pfctl -d")
        run_osascript("dnctl -q flush")
    else:
        print(f"  Setting dummynet delay={ms}ms...")
        Path(PF_RULES_FILE).write_text(
            "dummynet in  proto tcp from any to 127.0.0.0/8 pipe 1\n"
            "dummynet out proto tcp from any to 127.0.0.0/8 pipe 1\n"
        )
        run_osascript("pfctl -e")
        run_osascript(f"dnctl pipe 1 config delay {ms}")
        run_osascript(f"pfctl -f {PF_RULES_FILE}")


def kill_all():
    subprocess.run(["pkill", "-9", "-f", "node "], stderr=subprocess.DEVNULL)
    subprocess.run(["tmux", "kill-server"], stderr=subprocess.DEVNULL)
    time.sleep(2)


def run_benchmark(proto: str, faults: int, rate: int):
    code = f"""
from benchmark.local import LocalBench
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
    # Use Popen so we can heartbeat while waiting and dump output on
    # failure. We collect stdout/stderr ourselves to keep the parent's
    # screen tidy in the happy case.
    proc = subprocess.Popen(
        ["python3", "-c", code],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, cwd=str(BENCH_DIR),
    )
    start = time.time()
    timeout = 180
    last_beat = start
    while True:
        rc = proc.poll()
        if rc is not None:
            break
        now = time.time()
        if now - start > timeout:
            proc.kill()
            print(f"    TIMEOUT after {timeout}s")
            kill_all()
            return None
        if now - last_beat >= 5:
            elapsed = int(now - start)
            _orig_print(f".[{elapsed}s]", end="", flush=True)
            _LOG_FILE.write(f".[{elapsed}s]")
            _LOG_FILE.flush()
            last_beat = now
        time.sleep(0.25)
    out, err = proc.communicate(timeout=5)
    # Wipe the heartbeat dots so the next print starts clean.
    if time.time() - start >= 5:
        _orig_print("", flush=True)
        _LOG_FILE.write("\n")

    for line in out.splitlines():
        if line.startswith("METRICS_JSON:"):
            return json.loads(line[len("METRICS_JSON:"):])

    # Failure path — dump tail of subprocess output for debugging.
    print(f"    FAILED rc={proc.returncode}")
    tail_out = "\n".join(out.splitlines()[-30:]) if out else "(empty stdout)"
    tail_err = "\n".join(err.splitlines()[-15:]) if err else ""
    print("    --- subprocess stdout (last 30 lines) ---")
    for line in tail_out.splitlines():
        print(f"    | {line}")
    if tail_err.strip():
        print("    --- subprocess stderr (last 15 lines) ---")
        for line in tail_err.splitlines():
            print(f"    | {line}")
    return None


def stable_mean(values):
    if len(values) < 3:
        return (sum(values) / len(values), 0, True)
    sorted_vals = sorted(values)
    middle = sorted_vals[1:4] if len(sorted_vals) >= 5 else \
             sorted_vals[1:-1] if len(sorted_vals) > 3 else sorted_vals
    mid_mean = statistics.mean(middle)
    mid_stdev = statistics.stdev(middle) if len(middle) > 1 else 0
    has_outlier = False
    if len(sorted_vals) >= 5 and mid_mean > 0:
        if abs(sorted_vals[0] - mid_mean) / mid_mean > 0.3:
            has_outlier = True
        if abs(sorted_vals[-1] - mid_mean) / mid_mean > 0.3:
            has_outlier = True
    return mid_mean, mid_stdev, has_outlier


def load_progress():
    if PROGRESS_PATH.exists():
        return json.loads(PROGRESS_PATH.read_text())
    return {"completed": []}


def save_progress(progress):
    PROGRESS_PATH.write_text(json.dumps(progress, indent=2))


def config_key(delay, faults, proto, rate):
    return f"d{delay}_f{faults}_{proto}_r{rate}"


def main():
    progress = load_progress()
    completed = set(progress.get("completed", []))

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

        if delay > 0:
            verify = subprocess.run(
                ["osascript", "-e",
                 'do shell script "dnctl show" with administrator privileges'],
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
                        print(f"[{config_num}/{total_configs}] SKIP {key}")
                        continue

                    print(f"\n[{config_num}/{total_configs}] {key}")
                    tps_vals, lat_vals = [], []

                    for run in range(1, RUNS + 1):
                        ts = datetime.now().strftime("%H:%M:%S")
                        print(f"  [{ts}] Run {run}/{RUNS}...", end=" ", flush=True)
                        kill_all()
                        res = run_benchmark(proto, faults, rate)

                        if res is None:
                            print("FAILED -> retry")
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

                    if len([v for v in tps_vals if v > 0]) >= 3:
                        tps_mean, _, tps_outlier = stable_mean(
                            [v for v in tps_vals if v > 0])
                        lat_mean, _, _ = stable_mean(
                            [v for v in lat_vals if v > 0])
                    else:
                        tps_mean = statistics.mean(tps_vals) if tps_vals else 0
                        tps_outlier = True
                        lat_mean = statistics.mean(lat_vals) if lat_vals else 0

                    for i, (t, l) in enumerate(zip(tps_vals, lat_vals)):
                        is_out = (i == 0 and abs(tps_vals[0] - tps_mean) / max(tps_mean, 1) > 0.3) or \
                                 (i == len(tps_vals)-1 and abs(tps_vals[-1] - tps_mean) / max(tps_mean, 1) > 0.3)
                        writer.writerow([proto, faults, delay, rate, i+1, t, l, is_out])

                    writer.writerow([proto, faults, delay, rate, "mean",
                                     tps_mean, lat_mean,
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
