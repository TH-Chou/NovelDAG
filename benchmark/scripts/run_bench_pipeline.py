"""Orchestrator: run benchmarks, collect logs, parse, plot.

Reuses LocalBench, Bench, LogParser, InstanceManager from the existing package.
"""

from __future__ import annotations

import csv
import hashlib
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
BENCH_DIR = _SCRIPT_DIR.parent
sys.path.insert(0, str(BENCH_DIR))

from run_bench_config import get_outlier_config, get_output_config, resolve_groups  # noqa: E402
from run_bench_outliers import aggregate_metrics, apply_outlier_rejection  # noqa: E402
PF_RULES_FILE = "/tmp/pf_delay.conf"


# ═══════════════════════════════════════════════════════════════
# Dummynet helpers (local mode only)
# ═══════════════════════════════════════════════════════════════

SUDO_PASSWORD = os.environ.get("SWEEP_SUDO_PASSWORD", "561280")

def sudo_run(cmd: list[str], timeout: int = 20) -> tuple[bool, str]:
    full = ["sudo", "-S"] + cmd
    try:
        proc = subprocess.run(
            full, input=SUDO_PASSWORD + "\n", text=True,
            capture_output=True,
            timeout=timeout,
        )
        return (proc.returncode == 0, proc.stdout + proc.stderr)
    except subprocess.TimeoutExpired:
        return (False, "TIMEOUT")


def _configure_delay(ms: int, verbose: bool = True) -> None:
    if ms == 0:
        # 零延迟无需操作 dummynet——系统默认无包过滤。
        if verbose:
            print("  Delay=0ms (no dummynet setup needed)", flush=True)
        return
    if verbose:
        print(f"  Setting dummynet delay={ms}ms (RTT={ms * 2}ms)...", flush=True)
    Path(PF_RULES_FILE).write_text(
        "dummynet in  proto tcp from any to 127.0.0.0/8 pipe 1\n"
        "dummynet out proto tcp from any to 127.0.0.0/8 pipe 1\n"
    )
    sudo_run(["pfctl", "-e"])
    sudo_run(["dnctl", "pipe", "1", "config", "delay", str(ms)])
    sudo_run(["pfctl", "-f", PF_RULES_FILE])
    ok, out = sudo_run(["dnctl", "show"])
    if ok and f"{ms} ms" in out:
        print(f"  [OK] dummynet confirmed: {ms}ms", flush=True)
    else:
        print(f"  [WARN] dummynet may not be active", flush=True)


def _kill_all() -> None:
    subprocess.run(["pkill", "-9", "-f", "node "], stderr=subprocess.DEVNULL)
    subprocess.run(["tmux", "kill-server"], stderr=subprocess.DEVNULL)
    time.sleep(2)


# ═══════════════════════════════════════════════════════════════
# Checkpoints
# ═══════════════════════════════════════════════════════════════


def _checkpoint_path(checkpoint_dir: Path, group: str) -> Path:
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    return checkpoint_dir / f"{group}_checkpoint.json"


def _load_checkpoint(path: Path) -> set[str]:
    if path.exists():
        return set(json.loads(path.read_text()).get("completed", []))
    return set()


def _save_checkpoint(path: Path, completed: set[str]) -> None:
    path.write_text(json.dumps({"completed": sorted(completed)}))


def _point_hash(point: dict[str, Any]) -> str:
    """Stable hash for a config point so checkpoints survive YAML reordering."""
    s = f"{point['protocol']}_{point['faults']}_{point['delay']}_{point['rate']}_{point['run_index']}"
    return hashlib.md5(s.encode()).hexdigest()[:12]


# ═══════════════════════════════════════════════════════════════
# Local runner
# ═══════════════════════════════════════════════════════════════


def _run_one_local(point: dict[str, Any]) -> dict[str, Any] | None:
    """Execute a single config point locally and return metrics dict."""
    bench = point["bench"]
    node = point["node"]
    bench_code = {
        "faults": point["faults"],
        "nodes": bench["nodes"],
        "workers": bench["workers"],
        "rate": point["rate"],
        "tx_size": bench["tx_size"],
        "duration": bench["duration"],
    }
    inner = f"""
from benchmark.local import LocalBench
import json
bench = {bench_code!r}
node = {node!r}
try:
    m = LocalBench(bench, node).run(debug=False).metrics()
    print('METRICS_JSON:' + json.dumps(m))
except Exception as e:
    print(f'FAILED: {{e}}')
"""
    try:
        result = subprocess.run(
            ["python3", "-c", inner],
            capture_output=True,
            text=True,
            timeout=min(300, bench.get("duration", 30) * 10),
            cwd=str(BENCH_DIR),
        )
        for line in result.stdout.splitlines():
            if line.startswith("METRICS_JSON:"):
                return json.loads(line[len("METRICS_JSON:"):])
        # 子进程失败时输出 stderr 以便排查
        if result.stderr:
            print(f"  [stderr] {result.stderr.strip()[:300]}", flush=True)
        return None
    except subprocess.TimeoutExpired:
        _kill_all()
        print("  [TIMEOUT]", flush=True)
        return None


def run_local(
    points: list[dict[str, Any]],
    *,
    debug: bool = False,
    fresh: bool = False,
    checkpoint_dir: Path | None = None,
    output_csv: Path | None = None,
) -> int:
    """Run all config points locally. Returns number of successful runs."""
    ckpt_dir = checkpoint_dir or (BENCH_DIR / ".checkpoints")
    groups: dict[str, list[dict[str, Any]]] = {}
    for p in points:
        groups.setdefault(p["group"], []).append(p)

    total_ok = 0
    total = len(points)

    for gname, gpoints in groups.items():
        ckpt_path = _checkpoint_path(ckpt_dir, gname)
        completed = _load_checkpoint(ckpt_path) if not fresh else set()
        # Group by delay to minimize dummynet reconfig
        by_delay: dict[int, list[dict[str, Any]]] = {}
        for p in gpoints:
            by_delay.setdefault(p["delay"], []).append(p)

        delays = sorted(by_delay.keys())
        for delay_val in delays:
            _configure_delay(delay_val)
            time.sleep(1)
            delay_points = by_delay[delay_val]
            for pt in delay_points:
                ph = _point_hash(pt)
                if ph in completed:
                    print(
                        f"  [SKIP] {pt['label']} run={pt['run_index']} (already completed)"
                    )
                    continue

                _kill_all()
                print(
                    f"  [{pt['label']} run={pt['run_index']}] ... ",
                    end="",
                    flush=True,
                )
                m = _run_one_local(pt)
                if m:
                    tps = m.get("consensus_tps", 0)
                    lat = m.get("consensus_latency_ms", 0)
                    print(f"TPS={tps:.0f} Lat={lat:.0f}ms")
                    _append_csv_row(
                        output_csv or BENCH_DIR / "csv_plots" / f"{gname}_runs.csv",
                        pt,
                        m,
                    )
                    completed.add(ph)
                    _save_checkpoint(ckpt_path, completed)
                    total_ok += 1
                else:
                    print("FAILED")
                    # Mark as completed anyway so we don't loop forever
                    completed.add(ph)
                    _save_checkpoint(ckpt_path, completed)

        _configure_delay(0)

    print(f"\nDone. {total_ok}/{total} runs succeeded.")
    return total_ok


# ═══════════════════════════════════════════════════════════════
# Remote runner (stubs for GCP/AWS)
# ═══════════════════════════════════════════════════════════════


def run_remote(
    points: list[dict[str, Any]],
    *,
    mode: str = "aws",
    debug: bool = False,
    batch_id: str = "default",
    settings_path: str = "settings.json",
) -> None:
    """Run benchmarks on cloud via Bench."""
    from fabric import Connection

    from benchmark.remote import Bench

    ctx = Connection("localhost")  # Fabric context unused in our Bench override
    b = Bench(ctx, settings_file=settings_path)
    # Build params in the format Bench expects
    # For simplicity, iterate per config point via Bench.run()
    groups: dict[str, list[dict[str, Any]]] = {}
    for p in points:
        groups.setdefault(p["group"], []).append(p)

    for gname, gpoints in groups.items():
        by_shape: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for p in gpoints:
            bench = p["bench"]
            key = (
                p["protocol"],
                p["faults"],
                bench["nodes"],
                bench["workers"],
                bench["tx_size"],
                bench["duration"],
                bench["runs"],
            )
            by_shape.setdefault(key, []).append(p)

        for key, shape_points in sorted(by_shape.items()):
            proto, faults, nodes, workers, tx_size, duration, runs = key
            rates = sorted({p["rate"] for p in shape_points})
            first = shape_points[0]
            node_params = first["node"].copy()
            node_params["dag_protocol"] = proto
            if len({p["delay"] for p in shape_points}) > 1 or first["delay"] != 0:
                print(
                    "  [WARN] Cloud mode does not apply local dummynet delays; "
                    f"ignoring delay labels for group '{gname}'."
                )
            bench_params = {
                "faults": faults,
                "nodes": [nodes],
                "workers": workers,
                "collocate": True,
                "rate": rates,
                "tx_size": tx_size,
                "duration": duration,
                "runs": runs,
            }
            print(
                f"  Running group '{gname}': protocol={proto}, "
                f"faults={faults}, rates={rates}"
            )
            b.run(bench_params, node_params, debug)
    print("Done.")


def collect_remote(
    batch_id: str = "default",
    output_dir: str = "batch_downloads",
    settings_path: str = "settings.json",
) -> None:
    """Download archived batch logs from cloud machines."""
    from fabric import Connection

    from benchmark.remote import Bench

    ctx = Connection("localhost")
    b = Bench(ctx, settings_file=settings_path)
    b.collect_batch(batch_id, output_dir)
    print(f"Logs downloaded to {output_dir}/")


# ═══════════════════════════════════════════════════════════════
# Log parser
# ═══════════════════════════════════════════════════════════════


def parse_logs(
    logs_dir: str | Path,
    *,
    faults: int = 0,
    config_raw: dict[str, Any] | None = None,
    output_csv: str | Path | None = None,
) -> dict[str, Any]:
    """Parse logs from a directory, applying outlier rejection from config."""
    logs_path = Path(logs_dir)
    if not logs_path.exists():
        raise FileNotFoundError(f"Logs directory not found: {logs_dir}")

    from benchmark.logs import LogParser

    parser = LogParser.process(str(logs_path), faults=faults)
    metrics = parser.metrics()
    print(parser.result())

    # Apply outlier rejection to per-run data if available
    outlier_cfg = get_outlier_config(config_raw or {})
    # For single-directory parse, we just have one set of metrics
    # Outlier rejection is applied during CSV aggregation below

    if output_csv:
        _write_summary_csv(Path(output_csv), metrics)

    return metrics


def parse_and_aggregate(
    points: list[dict[str, Any]],
    logs_dir: str | Path,
    *,
    config_raw: dict[str, Any] | None = None,
    csv_dir: str | Path | None = None,
) -> None:
    """Parse all per-run result files, apply outlier rejection, write aggregated CSV."""
    from benchmark.logs import LogParser

    logs_path = Path(logs_dir)
    outlier_cfg = get_outlier_config(config_raw or {})
    output_cfg = get_output_config(config_raw or {})
    csv_path = Path(csv_dir or output_cfg["csv_dir"])

    csv_path.mkdir(parents=True, exist_ok=True)

    # Group runs by (protocol, faults, delay, rate)
    by_config: dict[tuple[str, int, int, int], list[dict[str, Any]]] = {}
    for pt in points:
        key = (pt["protocol"], pt["faults"], pt["delay"], pt["rate"])
        m = _read_result_file(logs_path, pt)
        if m:
            by_config.setdefault(key, []).append(m)

    if not by_config:
        # Fall back to direct LogParser
        parser = LogParser.process(str(logs_path), faults=0)
        print(parser.result())
        return

    per_run_csv = csv_path / "all_runs.csv"
    agg_csv = csv_path / "aggregated.csv"

    with open(per_run_csv, "w", newline="") as f_runs, open(
        agg_csv, "w", newline=""
    ) as f_agg:
        runs_writer = csv.writer(f_runs)
        runs_writer.writerow(
            [
                "run",
                "protocol",
                "faults",
                "delay_ms",
                "rate",
                "consensus_tps",
                "consensus_latency_ms",
                "end_to_end_tps",
                "end_to_end_latency_ms",
            ]
        )
        agg_writer = csv.writer(f_agg)
        agg_writer.writerow(
            [
                "protocol",
                "faults",
                "delay_ms",
                "rate",
                "run",
                "tps",
                "latency_ms",
            ]
        )

        for (proto, fault, delay, rate), runs in sorted(by_config.items()):
            # Write individual runs
            for i, r in enumerate(runs, 1):
                runs_writer.writerow(
                    [
                        i,
                        proto,
                        fault,
                        delay,
                        rate,
                        f'{r["consensus_tps"]:.6f}',
                        f'{r["consensus_latency_ms"]:.6f}',
                        f'{r.get("end_to_end_tps", 0):.6f}',
                        f'{r.get("end_to_end_latency_ms", 0):.6f}',
                    ]
                )

            # Apply outlier rejection
            kept, stats = apply_outlier_rejection(
                runs,
                method=outlier_cfg["method"],
                middle_n=outlier_cfg["middle_n"],
                std_dev_threshold=outlier_cfg.get("std_dev_threshold", 2.0),
            )
            n_kept = stats["kept_runs"]
            agg = aggregate_metrics(kept)
            agg_writer.writerow(
                [
                    proto,
                    fault,
                    delay,
                    rate,
                    "mean",
                    f'{agg["consensus_tps"]:.6f}',
                    f'{agg["consensus_latency_ms"]:.6f}',
                ]
            )
            if stats["rejected_indices"]:
                print(
                    f"  {proto} f={fault} d={delay} r={rate//1000}K: "
                    f"kept {n_kept}/{stats['total_runs']}, "
                    f"rejected indices={stats['rejected_indices']}"
                )

    print(f"Per-run CSV: {per_run_csv}")
    print(f"Aggregated CSV: {agg_csv}")


def _read_result_file(
    logs_path: Path, point: dict[str, Any]
) -> dict[str, Any] | None:
    """Try to read a parsed result file for a config point."""
    # Result files follow PathMaker.result_file() naming
    from benchmark.utils import PathMaker

    run_index = int(point.get("run_index", 0) or 0)
    if run_index:
        result_path = Path(PathMaker.run_result_file(
            point["faults"],
            point["bench"]["nodes"],
            point["bench"]["workers"],
            True,  # collocate
            point["rate"],
            point["bench"]["tx_size"],
            run_index,
            point["protocol"],
        ))
    else:
        result_path = Path(PathMaker.result_file(
            point["faults"],
            point["bench"]["nodes"],
            point["bench"]["workers"],
            True,  # collocate
            point["rate"],
            point["bench"]["tx_size"],
            point["protocol"],
        ))
    full = logs_path / result_path.name if not result_path.is_absolute() else result_path
    alt = logs_path / "results" / result_path.name

    for p in (result_path, full, alt):
        if p.exists():
            try:
                return _parse_result_text(p.read_text())
            except Exception:
                return None
    return None


def _parse_result_text(text: str) -> dict[str, Any]:
    import re

    m = {}
    for line in text.split("\n"):
        if m2 := re.match(r"\s*Consensus TPS:\s*([\d,]+(?:\.\d+)?)", line):
            m["consensus_tps"] = float(m2.group(1).replace(",", ""))
        elif m2 := re.match(r"\s*Consensus latency:\s*([\d,]+(?:\.\d+)?)\s*ms", line):
            m["consensus_latency_ms"] = float(m2.group(1).replace(",", ""))
        elif m2 := re.match(r"\s*End-to-end TPS:\s*([\d,]+(?:\.\d+)?)", line):
            m["end_to_end_tps"] = float(m2.group(1).replace(",", ""))
        elif m2 := re.match(r"\s*End-to-end latency:\s*([\d,]+(?:\.\d+)?)\s*ms", line):
            m["end_to_end_latency_ms"] = float(m2.group(1).replace(",", ""))
    return m


def _append_csv_row(path: Path, point: dict[str, Any], metrics: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    existed = path.exists()
    with open(path, "a", newline="") as f:
        w = csv.writer(f)
        if not existed:
            w.writerow(
                [
                    "run",
                    "protocol",
                    "faults",
                    "delay_ms",
                    "rate",
                    "consensus_tps",
                    "consensus_latency_ms",
                    "end_to_end_tps",
                    "end_to_end_latency_ms",
                ]
            )
        w.writerow(
            [
                point["run_index"],
                point["protocol"],
                point["faults"],
                point["delay"],
                point["rate"],
                f"{metrics.get('consensus_tps', 0):.6f}",
                f"{metrics.get('consensus_latency_ms', 0):.6f}",
                f"{metrics.get('end_to_end_tps', 0):.6f}",
                f"{metrics.get('end_to_end_latency_ms', 0):.6f}",
            ]
        )


def _write_summary_csv(path: Path, metrics: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "consensus_tps",
                "consensus_latency_ms",
                "end_to_end_tps",
                "end_to_end_latency_ms",
            ]
        )
        w.writerow(
            [
                f"{metrics.get('consensus_tps', 0):.6f}",
                f"{metrics.get('consensus_latency_ms', 0):.6f}",
                f"{metrics.get('end_to_end_tps', 0):.6f}",
                f"{metrics.get('end_to_end_latency_ms', 0):.6f}",
            ]
        )
    print(f"Summary CSV: {path}")


# ═══════════════════════════════════════════════════════════════
# Plotting
# ═══════════════════════════════════════════════════════════════


def generate_plots(
    csv_path: str | Path,
    *,
    out_dir: str | Path | None = None,
    chart_type: str = "all",
) -> None:
    """Generate charts from CSV data."""
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    csv_path = Path(csv_path)
    out_dir = Path(out_dir or "plots")
    out_dir.mkdir(parents=True, exist_ok=True)

    if not csv_path.exists():
        print(f"CSV not found: {csv_path}")
        return

    # Quick TPS vs Latency scatter
    raw: dict[str, list[tuple[float, float, str]]] = {}
    with open(csv_path) as f:
        for row in csv.DictReader(f):
            proto = row.get("protocol", "unknown")
            try:
                tps = float(row.get("consensus_tps", row.get("tps", 0)))
                lat = float(row.get("consensus_latency_ms", row.get("latency_ms", 0)))
                if tps > 0 and lat > 0:
                    raw.setdefault(proto, []).append((tps, lat, proto))
            except (ValueError, TypeError):
                continue

    if not raw:
        print("No data found in CSV.")
        return

    markers = {"narwhal": "s", "bullshark": "^", "shortfin": "o", "sailfin": "P", "wahoo": "D"}
    colors = {"narwhal": "#FF5722", "bullshark": "#FF9800", "shortfin": "#2196F3", "sailfin": "#E69F00", "wahoo": "#9C27B0"}

    fig, axes = plt.subplots(1, 2, figsize=(14, 5.5))

    # Chart 1: TPS vs Latency
    ax = axes[0]
    for proto, pts in raw.items():
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        ax.scatter(
            xs,
            ys,
            marker=markers.get(proto, "o"),
            color=colors.get(proto, "#333"),
            label=proto,
            s=30,
            alpha=0.7,
        )
    ax.set_xlabel("Consensus TPS (tx/s)")
    ax.set_ylabel("Consensus Latency (ms)")
    ax.set_title("TPS vs Latency")
    ax.legend(fontsize=8)
    ax.grid(True, alpha=0.2)

    # Chart 2: TPS distribution (box plot)
    ax = axes[1]
    proto_data = {}
    for proto, pts in raw.items():
        proto_data[proto] = [p[0] for p in pts]
    ax.boxplot(
        proto_data.values(),
        labels=proto_data.keys(),
        patch_artist=True,
        boxprops=dict(facecolor="#ddd", alpha=0.5),
    )
    ax.set_ylabel("Consensus TPS (tx/s)")
    ax.set_title("TPS Distribution")
    ax.grid(True, alpha=0.2, axis="y")

    fig.suptitle("Benchmark Results", fontsize=13, fontweight="bold")
    plt.tight_layout()
    out_png = out_dir / "benchmark_summary.png"
    fig.savefig(out_png, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"Chart saved: {out_png}")
