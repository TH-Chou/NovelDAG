#!/usr/bin/env python3
"""Unified NovelDAG benchmark CLI.

Usage:
  python run_bench.py --mode local run [--config configs/smoke.yaml] [--group smoke]
  python run_bench.py --mode aws run --config configs/remote.yaml
  python run_bench.py parse --logs-dir logs/ --config configs/smoke.yaml
  python run_bench.py plot --csv csv_plots/all_runs.csv
  python run_bench.py --mode local full --config configs/smoke.yaml
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path
from typing import Any

_SCRIPT_DIR = Path(__file__).resolve().parent
_BENCH_DIR = _SCRIPT_DIR.parent
sys.path.insert(0, str(_BENCH_DIR))

from run_bench_checks import PreflightError, check_cloud, check_local  # noqa: E402
from run_bench_config import ConfigError, get_output_config, load_config, resolve_groups  # noqa: E402
from run_bench_pipeline import (  # noqa: E402
    _configure_delay,
    collect_remote,
    generate_plots,
    parse_logs,
    run_local,
    run_remote,
)


def _comma_ints(s: str) -> list[int]:
    return [int(x.strip()) for x in s.split(",") if x.strip()]


def _comma_strs(s: str) -> list[str]:
    return [x.strip() for x in s.split(",") if x.strip()]


def _absolutize_existing_paths(args: argparse.Namespace) -> None:
    """Preserve caller-relative paths before switching to benchmark root.

    Defaults like settings.json intentionally remain relative so they resolve
    against benchmark/. Explicit paths that already exist from the caller's cwd
    are made absolute to keep older commands working.
    """
    for attr in ("settings", "config", "logs_dir", "output_csv", "csv"):
        value = getattr(args, attr, None)
        if not value:
            continue
        path = Path(value).expanduser()
        if path.exists():
            setattr(args, attr, str(path.resolve()))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Unified NovelDAG benchmark CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--mode",
        choices=["local", "aws", "gcp"],
        default=None,
        help="Run mode (required for run/collect/full)",
    )
    parser.add_argument(
        "--settings",
        type=str,
        default="settings.json",
        help="Path to settings.json for cloud modes (default: settings.json)",
    )
    parser.add_argument(
        "--sudo-password",
        type=str,
        default=None,
        help="Sudo password for dummynet in local mode (overrides SWEEP_SUDO_PASSWORD env var)",
    )

    sub = parser.add_subparsers(dest="command", help="Subcommand")

    # ── run ─────────────────────────────────────────────────
    run_p = sub.add_parser("run", help="Execute benchmarks")
    run_p.add_argument("--config", type=str, help="Path to YAML config file")
    run_p.add_argument("--group", type=str, help="Run a specific group from config")
    run_p.add_argument("--protocols", type=str, help="Comma-separated protocols")
    run_p.add_argument("--rates", type=str, help="Comma-separated injection rates")
    run_p.add_argument("--faults", type=str, help="Comma-separated fault counts")
    run_p.add_argument("--delays", type=str, help="Comma-separated one-way delays (ms)")
    run_p.add_argument("--nodes", type=int, help="Number of nodes")
    run_p.add_argument("--duration", type=int, help="Benchmark duration per run (s)")
    run_p.add_argument("--runs", type=int, help="Runs per config point")
    run_p.add_argument("--header-size", type=int, help="Header size in bytes")
    run_p.add_argument("--max-header-delay", type=int, help="Max header delay (ms)")
    run_p.add_argument("--batch-size", type=int, help="Max batch size (bytes)")
    run_p.add_argument("--tx-size", type=int, help="Transaction size (bytes)")
    run_p.add_argument(
        "--dag-protocol", type=str, help="DAG protocol (narwhal/noveldag/wahoo)"
    )
    run_p.add_argument("--output-prefix", type=str, help="Output CSV prefix")
    run_p.add_argument(
        "--fresh", action="store_true", help="Clear checkpoints and start fresh"
    )
    run_p.add_argument(
        "--dry-run", action="store_true", help="Print test matrix without running"
    )
    run_p.add_argument("--debug", action="store_true", help="Enable debug output")

    # ── collect ─────────────────────────────────────────────
    col_p = sub.add_parser("collect", help="Download logs from cloud")
    col_p.add_argument("--batch-id", type=str, default="default", help="Batch ID")
    col_p.add_argument(
        "--out-dir", type=str, default="batch_downloads", help="Output directory"
    )

    # ── parse ───────────────────────────────────────────────
    parse_p = sub.add_parser("parse", help="Parse logs into aggregated CSV")
    parse_p.add_argument("--logs-dir", type=str, help="Path to log directory")
    parse_p.add_argument("--config", type=str, help="Path to YAML config (for outlier settings)")
    parse_p.add_argument("--faults", type=int, default=0, help="Fault count for parsing")
    parse_p.add_argument(
        "--outlier", type=str, choices=["none", "middle-N", "std-dev"],
        help="Outlier rejection method"
    )
    parse_p.add_argument("--middle-n", type=int, default=3, help="Keep middle N runs")
    parse_p.add_argument("--std-dev", type=float, default=2.0, help="Std-dev threshold")
    parse_p.add_argument("--output-csv", type=str, help="Output CSV path")

    # ── plot ────────────────────────────────────────────────
    plot_p = sub.add_parser("plot", help="Generate charts from CSV")
    plot_p.add_argument("--csv", type=str, required=True, help="Path to CSV file")
    plot_p.add_argument(
        "--chart-type",
        type=str,
        choices=["latency-vs-tps", "tps-vs-rate", "all"],
        default="all",
        help="Chart type to generate (default: all)",
    )
    plot_p.add_argument("--out-dir", type=str, default="plots", help="Output directory")

    # ── full ────────────────────────────────────────────────
    full_p = sub.add_parser("full", help="run -> collect -> parse -> plot")
    full_p.add_argument("--config", type=str, help="Path to YAML config file")
    full_p.add_argument("--group", type=str, help="Run a specific group from config")
    full_p.add_argument("--protocols", type=str)
    full_p.add_argument("--rates", type=str)
    full_p.add_argument("--faults", type=str)
    full_p.add_argument("--delays", type=str)
    full_p.add_argument("--nodes", type=int)
    full_p.add_argument("--duration", type=int)
    full_p.add_argument("--runs", type=int)
    full_p.add_argument("--header-size", type=int)
    full_p.add_argument("--max-header-delay", type=int)
    full_p.add_argument("--batch-size", type=int)
    full_p.add_argument("--tx-size", type=int)
    full_p.add_argument("--dag-protocol", type=str)
    full_p.add_argument("--fresh", action="store_true")
    full_p.add_argument("--dry-run", action="store_true")
    full_p.add_argument("--debug", action="store_true")
    full_p.add_argument("--logs-dir", type=str, help="Log directory (for parse step)")
    full_p.add_argument("--outlier", type=str, choices=["none", "middle-N", "std-dev"])
    full_p.add_argument("--middle-n", type=int, default=3)
    full_p.add_argument("--std-dev", type=float, default=2.0)
    full_p.add_argument("--batch-id", type=str, default="default")

    args = parser.parse_args()
    _absolutize_existing_paths(args)
    os.chdir(_BENCH_DIR)

    # ── Dispatch ────────────────────────────────────────────
    try:
        if args.command == "run":
            _cmd_run(args)
        elif args.command == "collect":
            _cmd_collect(args)
        elif args.command == "parse":
            _cmd_parse(args)
        elif args.command == "plot":
            _cmd_plot(args)
        elif args.command == "full":
            _cmd_full(args)
        else:
            parser.print_help()
    except (ConfigError, PreflightError) as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("\nInterrupted.", file=sys.stderr)
        sys.exit(130)


# ═══════════════════════════════════════════════════════════════
# Command implementations
# ═══════════════════════════════════════════════════════════════


def _build_cli_overrides(args: argparse.Namespace) -> dict[str, Any]:
    overrides: dict[str, Any] = {}
    for attr in (
        "protocols",
        "rates",
        "faults",
        "delays",
        "nodes",
        "duration",
        "runs",
        "header_size",
        "max_header_delay",
        "batch_size",
        "tx_size",
        "dag_protocol",
    ):
        val = getattr(args, attr, None)
        if val is not None:
            overrides[attr] = val
    # Parse comma-separated lists
    for attr in ("protocols", "rates", "faults", "delays"):
        if attr in overrides and isinstance(overrides[attr], str):
            if attr == "protocols":
                overrides[attr] = _comma_strs(overrides[attr])
            else:
                overrides[attr] = _comma_ints(overrides[attr])
    return overrides


def _resolve_config_and_points(
    args: argparse.Namespace,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Load config, apply CLI overrides, resolve to flat list of points."""
    config_path = getattr(args, "config", None)
    if config_path:
        raw = load_config(config_path)
        cli_overrides = _build_cli_overrides(args)
        resolved = resolve_groups(raw, args.group, cli_overrides)
        # Flatten all groups
        points = []
        for gpoints in resolved.values():
            points.extend(gpoints)
        return raw, points

    # No config — build minimal points from CLI flags
    cli_overrides = _build_cli_overrides(args)
    protocols = cli_overrides.get("protocols", ["noveldag"])
    rates = cli_overrides.get("rates", [60000])
    faults = cli_overrides.get("faults", [0])
    delays = cli_overrides.get("delays", [0])
    runs = cli_overrides.get("runs", 1)
    nodes = cli_overrides.get("nodes", 10)
    duration = cli_overrides.get("duration", 20)
    dag_protocol = cli_overrides.get("dag_protocol", protocols[0])

    from itertools import product

    bench = {
        "nodes": nodes,
        "workers": 1,
        "collocate": True,
        "tx_size": cli_overrides.get("tx_size", 512),
        "duration": duration,
        "runs": runs,
    }
    node = {
        "header_size": cli_overrides.get("header_size", 1000),
        "max_header_delay": cli_overrides.get("max_header_delay", 2000),
        "gc_depth": 50,
        "sync_retry_delay": 10000,
        "sync_retry_nodes": 3,
        "batch_size": cli_overrides.get("batch_size", 500000),
        "max_batch_delay": 200,
        "consensus_protocol": "round_robin",
        "dag_protocol": dag_protocol,
    }

    points = []
    for proto, rate, fault, delay in product(protocols, rates, faults, delays):
        pt_node = node.copy()
        pt_node["dag_protocol"] = proto
        for run_idx in range(1, runs + 1):
            points.append(
                {
                    "bench": bench.copy(),
                    "node": pt_node,
                    "delay": delay,
                    "protocol": proto,
                    "faults": fault,
                    "rate": rate,
                    "run_index": run_idx,
                    "group": "cli",
                    "label": f"d{delay}_f{fault}_{proto}_r{rate}",
                }
            )

    return {}, points


def _cmd_run(args: argparse.Namespace) -> None:
    mode = args.mode
    if not mode:
        print("Error: --mode is required for 'run'. Choose local, aws, or gcp.")
        sys.exit(1)

    raw, points = _resolve_config_and_points(args)
    print(f"Resolved {len(points)} config points.")

    if args.dry_run:
        for p in points:
            print(
                f"  [{p['group']}] {p['protocol']:>8s} f={p['faults']} "
                f"d={p['delay']:>3}ms rate={p['rate']:>6d} run={p['run_index']}"
            )
        return

    # Pre-flight checks
    if mode == "local":
        delays = sorted({p["delay"] for p in points})
        check_local(delays)
        if args.sudo_password:
            import os

            os.environ["SWEEP_SUDO_PASSWORD"] = args.sudo_password

        output_cfg = get_output_config(raw)
        csv_path = Path(output_cfg["csv_dir"]) / (
            (args.output_prefix or "bench") + "_runs.csv"
        )
        run_local(
            points,
            debug=args.debug,
            fresh=args.fresh,
            checkpoint_dir=Path(output_cfg["checkpoint_dir"]),
            output_csv=csv_path,
        )
    else:
        check_cloud(args.settings)
        run_remote(
            points, mode=mode, debug=args.debug, settings_path=args.settings
        )


def _cmd_collect(args: argparse.Namespace) -> None:
    if not args.mode:
        print("Error: --mode is required for 'collect'. Choose aws or gcp.")
        sys.exit(1)
    collect_remote(args.batch_id, args.out_dir, args.settings)


def _cmd_parse(args: argparse.Namespace) -> None:
    logs_dir = args.logs_dir or "logs"
    raw = load_config(args.config) if args.config else {}

    # CLI outlier overrides
    if args.outlier:
        raw.setdefault("outlier_rejection", {})["method"] = args.outlier
    if args.middle_n != 3:
        raw.setdefault("outlier_rejection", {})["middle_n"] = args.middle_n
    if args.std_dev != 2.0:
        raw.setdefault("outlier_rejection", {})["std_dev_threshold"] = args.std_dev

    parse_logs(
        logs_dir,
        faults=args.faults,
        config_raw=raw,
        output_csv=args.output_csv,
    )


def _cmd_plot(args: argparse.Namespace) -> None:
    generate_plots(
        args.csv,
        out_dir=args.out_dir,
        chart_type=args.chart_type,
    )


def _cmd_full(args: argparse.Namespace) -> None:
    """Full pipeline: run -> collect (cloud) -> parse -> plot."""
    # Reuse _cmd_run
    _cmd_run(args)

    mode = args.mode
    if mode in ("aws", "gcp"):
        print("\n--- Collecting remote logs ---")
        _cmd_collect(args)

    print("\n--- Parsing logs ---")
    logs_dir = args.logs_dir or "logs"
    raw = load_config(args.config) if args.config else {}
    if args.outlier:
        raw.setdefault("outlier_rejection", {})["method"] = args.outlier
    output_cfg = get_output_config(raw)
    csv_path = Path(output_cfg["csv_dir"]) / "bench_runs.csv"
    parse_logs(logs_dir, faults=0, config_raw=raw, output_csv=csv_path)

    print("\n--- Plotting ---")
    generate_plots(csv_path, out_dir=output_cfg["plot_dir"], chart_type="all")

    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
