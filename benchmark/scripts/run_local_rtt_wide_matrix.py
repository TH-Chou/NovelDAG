#!/usr/bin/env python3
"""Run and validate a resumable local wide-load matrix under Linux netem."""

from __future__ import annotations

import argparse
import atexit
import csv
import json
import math
import os
import random
import re
import shutil
import signal
import subprocess
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


PROTOCOLS = ("shortfin", "narwhal", "mahi_mahi", "wahoo")
MODES = {
    "normal": (0, "silence"),
    "silence": (1, "silence"),
    "equivocation": (1, "equivocation"),
}
DEFAULT_RATES = (5_000, 15_000, 30_000, 60_000, 120_000, 180_000, 240_000, 300_000)
CSV_FIELDS = (
    "run",
    "protocol",
    "faults",
    "fault_mode",
    "delay_ms",
    "rate",
    "duration_s",
    "consensus_tps",
    "consensus_latency_ms",
    "end_to_end_tps",
    "end_to_end_latency_ms",
)


class MatrixError(RuntimeError):
    pass


def parse_args() -> argparse.Namespace:
    script_dir = Path(__file__).resolve().parent
    benchmark_dir = script_dir.parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--session", required=True, help="Safe output filename prefix")
    parser.add_argument("--one-way-delay-ms", type=int, default=100)
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--nodes", type=int, default=4)
    parser.add_argument("--rates", default=",".join(map(str, DEFAULT_RATES)))
    parser.add_argument("--protocols", default=",".join(PROTOCOLS))
    parser.add_argument("--modes", default=",".join(MODES))
    parser.add_argument("--retry-runs", type=int, default=2)
    parser.add_argument("--seed", type=int, default=20260914)
    parser.add_argument("--cooldown-seconds", type=int, default=5)
    parser.add_argument("--benchmark-dir", type=Path, default=benchmark_dir)
    parser.add_argument("--fresh", action="store_true", help="Ignore complete line CSVs")
    parser.add_argument(
        "--postcheck-only",
        action="store_true",
        help="Recheck completed mode CSVs with strict shape rules and retry flagged points",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def parse_csv_list(raw: str, cast=str) -> list:
    return [cast(value.strip()) for value in raw.split(",") if value.strip()]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def write_csv(path: Path, rows: list[dict[str, str]], fields: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def as_float(row: dict[str, str], field: str) -> float:
    try:
        value = float(row[field])
    except (KeyError, TypeError, ValueError):
        return math.nan
    return value


def row_is_usable(row: dict[str, str]) -> bool:
    metrics = (
        as_float(row, "consensus_tps"),
        as_float(row, "consensus_latency_ms"),
        as_float(row, "end_to_end_tps"),
        as_float(row, "end_to_end_latency_ms"),
    )
    return all(math.isfinite(value) and value > 0 for value in metrics)


def normalize_row(row: dict[str, str], mode: str, delay_ms: int) -> dict[str, str]:
    normalized = {field: row.get(field, "") for field in CSV_FIELDS}
    normalized["fault_mode"] = mode
    normalized["delay_ms"] = str(delay_ms)
    return normalized


def complete_initial_line(
    rows: list[dict[str, str]], protocol: str, rates: list[int]
) -> bool:
    matching = [row for row in rows if row.get("protocol") == protocol]
    observed = [int(row["rate"]) for row in matching if row.get("rate", "").isdigit()]
    return len(matching) == len(rates) and sorted(observed) == sorted(rates)


def detect_anomalies(
    rows: list[dict[str, str]], mode: str, rates: list[int]
) -> dict[int, list[str]]:
    issues: dict[int, list[str]] = defaultdict(list)
    by_rate: dict[int, dict[str, str]] = {}
    for row in rows:
        try:
            rate = int(row["rate"])
        except (KeyError, TypeError, ValueError):
            continue
        by_rate[rate] = row

    useful_fraction = 0.75 if mode == "equivocation" else 1.0
    for rate in rates:
        row = by_rate.get(rate)
        if row is None:
            issues[rate].append("missing result")
            continue
        if not row_is_usable(row):
            issues[rate].append("missing, zero, or non-finite metric")
            continue

        useful_cap = rate * useful_fraction
        consensus_tps = as_float(row, "consensus_tps")
        end_to_end_tps = as_float(row, "end_to_end_tps")
        if max(consensus_tps, end_to_end_tps) > useful_cap * 1.15 + 500:
            issues[rate].append("throughput exceeds useful-input cap")
        if rate <= 120_000 and end_to_end_tps < useful_cap * 0.72:
            issues[rate].append("unexpected low-load utilization")

    ordered = sorted(rate for rate in rates if rate in by_rate and row_is_usable(by_rate[rate]))
    for previous, current, following in zip(ordered, ordered[1:], ordered[2:]):
        prev_row, row, next_row = by_rate[previous], by_rate[current], by_rate[following]
        prev_tps = as_float(prev_row, "end_to_end_tps")
        tps = as_float(row, "end_to_end_tps")
        next_tps = as_float(next_row, "end_to_end_tps")
        if tps < 0.75 * min(prev_tps, next_tps):
            issues[current].append("isolated throughput trough")

        prev_latency = as_float(prev_row, "end_to_end_latency_ms")
        latency = as_float(row, "end_to_end_latency_ms")
        next_latency = as_float(next_row, "end_to_end_latency_ms")
        if latency > 3_000 and latency > 2.50 * max(prev_latency, next_latency):
            issues[current].append("isolated latency spike")

    if len(ordered) >= 2:
        previous, current = ordered[-2], ordered[-1]
        previous_tps = as_float(by_rate[previous], "end_to_end_tps")
        current_tps = as_float(by_rate[current], "end_to_end_tps")
        if current_tps < 0.65 * previous_tps:
            issues[current].append("terminal throughput collapse")

    return dict(issues)


def archive_partial(path: Path) -> None:
    if not path.exists():
        return
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    archived = path.with_name(f"{path.stem}.partial-{stamp}{path.suffix}")
    path.replace(archived)


class MatrixRunner:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.benchmark_dir = args.benchmark_dir.resolve()
        self.csv_dir = self.benchmark_dir / "csv_plots"
        self.result_dir = self.benchmark_dir / "results" / args.session
        self.log_dir = self.result_dir / "logs"
        self.manifest_path = self.result_dir / "manifest.json"
        self.orchestrator_log = self.result_dir / "orchestrator.log"
        self.netem_active = False
        self.manifest = {
            "session": args.session,
            "started_at": utc_now(),
            "status": "planned",
            "one_way_delay_ms": args.one_way_delay_ms,
            "expected_rtt_ms": args.one_way_delay_ms * 2,
            "duration_s": args.duration,
            "nodes": args.nodes,
            "rates": args.rates,
            "protocols": args.protocols,
            "modes": args.modes,
            "retry_runs": args.retry_runs,
            "lines": {},
        }

    def log(self, message: str) -> None:
        line = f"[{datetime.now().isoformat(timespec='seconds')}] {message}"
        print(line, flush=True)
        self.result_dir.mkdir(parents=True, exist_ok=True)
        with self.orchestrator_log.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def save_manifest(self) -> None:
        self.result_dir.mkdir(parents=True, exist_ok=True)
        temporary = self.manifest_path.with_suffix(".json.tmp")
        temporary.write_text(json.dumps(self.manifest, indent=2, sort_keys=True) + "\n")
        temporary.replace(self.manifest_path)

    def check_environment(self) -> None:
        if sys.platform != "linux":
            raise MatrixError("This runner requires Linux/WSL tc netem")
        if os.geteuid() != 0:
            raise MatrixError("Run as root so tc netem can configure loopback")
        if not re.fullmatch(r"[A-Za-z0-9_.-]+", self.args.session):
            raise MatrixError("--session may contain only letters, digits, dot, dash, underscore")
        for executable in ("tc", "ping", "python3", "tmux"):
            if shutil.which(executable) is None:
                raise MatrixError(f"Missing executable: {executable}")
        for binary in (self.benchmark_dir / "node", self.benchmark_dir / "benchmark_client"):
            if not binary.exists():
                raise MatrixError(f"Missing benchmark binary: {binary}")

        qdisc = subprocess.run(
            ["tc", "qdisc", "show", "dev", "lo"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        if "noqueue" not in qdisc and "netem" not in qdisc:
            raise MatrixError(f"Loopback already has an unexpected qdisc: {qdisc.strip()}")

        process_check = subprocess.run(
            ["pgrep", "-af", "run_bench.py|benchmark_client|target/release/node"],
            capture_output=True,
            text=True,
        )
        unrelated = [
            line
            for line in process_check.stdout.splitlines()
            if "run_local_rtt_wide_matrix.py" not in line and "pgrep -af" not in line
        ]
        if unrelated:
            raise MatrixError("Benchmark processes are already running:\n" + "\n".join(unrelated))

    def configure_netem(self) -> None:
        subprocess.run(
            [
                "tc",
                "qdisc",
                "replace",
                "dev",
                "lo",
                "root",
                "netem",
                "delay",
                f"{self.args.one_way_delay_ms}ms",
            ],
            check=True,
        )
        self.netem_active = True
        ping = subprocess.run(
            ["ping", "-c", "3", "-W", "2", "127.0.0.1"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
        match = re.search(r"= [^/]+/([^/]+)/", ping)
        observed = float(match.group(1)) if match else math.nan
        self.manifest["observed_ping_rtt_ms"] = observed
        expected = self.args.one_way_delay_ms * 2
        if not math.isfinite(observed) or abs(observed - expected) > max(20, expected * 0.20):
            raise MatrixError(f"Unexpected loopback RTT: observed={observed}, expected={expected}")
        self.log(f"netem active: one-way={self.args.one_way_delay_ms} ms, ping RTT={observed:.1f} ms")

    def cleanup(self) -> None:
        if not self.netem_active:
            return
        subprocess.run(
            ["tc", "qdisc", "del", "dev", "lo", "root"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        self.netem_active = False

    def run_benchmark_command(
        self,
        mode: str,
        protocol: str,
        rates: list[int],
        runs: int,
        output_prefix: str,
        log_path: Path,
    ) -> None:
        faults, fault_mode = MODES[mode]
        command = [
            "python3",
            "scripts/run_bench.py",
            "--mode",
            "local",
            "run",
            "--protocols",
            protocol,
            "--rates",
            ",".join(map(str, rates)),
            "--faults",
            str(faults),
            "--fault-mode",
            fault_mode,
            "--nodes",
            str(self.args.nodes),
            "--duration",
            str(self.args.duration),
            "--runs",
            str(runs),
            "--output-prefix",
            output_prefix,
            "--fresh",
        ]
        log_path.parent.mkdir(parents=True, exist_ok=True)
        timeout = len(rates) * runs * min(330, self.args.duration * 10) + 120
        with log_path.open("w", encoding="utf-8") as handle:
            handle.write("COMMAND: " + " ".join(command) + "\n")
            handle.flush()
            try:
                result = subprocess.run(
                    command,
                    cwd=self.benchmark_dir,
                    stdout=handle,
                    stderr=subprocess.STDOUT,
                    timeout=timeout,
                )
            except subprocess.TimeoutExpired as error:
                subprocess.run(["tmux", "kill-server"], stderr=subprocess.DEVNULL)
                subprocess.run(["pkill", "-9", "-f", "node "], stderr=subprocess.DEVNULL)
                raise MatrixError(f"Benchmark command timed out: {log_path}") from error
        if result.returncode != 0:
            raise MatrixError(f"Benchmark command failed ({result.returncode}): {log_path}")

    def line_paths(self, mode: str, protocol: str) -> tuple[str, Path, Path]:
        prefix = f"{self.args.session}_{mode}_{protocol}_initial"
        return prefix, self.csv_dir / f"{prefix}_runs.csv", self.log_dir / f"{mode}_{protocol}_initial.log"

    def run_initial_line(self, mode: str, protocol: str, rates: list[int], index: int) -> list[dict[str, str]]:
        prefix, csv_path, log_path = self.line_paths(mode, protocol)
        existing = read_csv(csv_path)
        if not self.args.fresh and complete_initial_line(existing, protocol, rates):
            self.log(f"reuse complete line: {mode}/{protocol}")
            return [normalize_row(row, mode, self.args.one_way_delay_ms) for row in existing]
        if csv_path.exists():
            archive_partial(csv_path)

        shuffled_rates = rates.copy()
        random.Random(self.args.seed + index).shuffle(shuffled_rates)
        self.log(f"start line: {mode}/{protocol}; rates={shuffled_rates}")
        self.run_benchmark_command(mode, protocol, shuffled_rates, 1, prefix, log_path)
        rows = read_csv(csv_path)
        self.log(f"finish line: {mode}/{protocol}; rows={len(rows)}")
        return [normalize_row(row, mode, self.args.one_way_delay_ms) for row in rows]

    def retry_rate(self, mode: str, protocol: str, rate: int) -> list[dict[str, str]]:
        prefix = f"{self.args.session}_{mode}_{protocol}_retry_r{rate}"
        csv_path = self.csv_dir / f"{prefix}_runs.csv"
        log_path = self.log_dir / f"{mode}_{protocol}_retry_r{rate}.log"
        existing = read_csv(csv_path)
        matching = [row for row in existing if row.get("protocol") == protocol and row.get("rate") == str(rate)]
        if self.args.fresh or len(matching) < self.args.retry_runs:
            if csv_path.exists():
                archive_partial(csv_path)
            self.log(f"retry anomaly: {mode}/{protocol} rate={rate}; runs={self.args.retry_runs}")
            self.run_benchmark_command(
                mode,
                protocol,
                [rate],
                self.args.retry_runs,
                prefix,
                log_path,
            )
            matching = read_csv(csv_path)
        return [normalize_row(row, mode, self.args.one_way_delay_ms) for row in matching]

    def select_line(
        self,
        initial_rows: list[dict[str, str]],
        issues: dict[int, list[str]],
        mode: str,
        protocol: str,
        rates: list[int],
    ) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
        by_rate = {int(row["rate"]): row for row in initial_rows if row.get("rate", "").isdigit()}
        selected: list[dict[str, str]] = []
        attempts: list[dict[str, str]] = []

        for rate in rates:
            candidates = []
            if rate in by_rate:
                candidates.append(("initial", by_rate[rate]))
            if rate in issues:
                candidates.extend(("retry", row) for row in self.retry_rate(mode, protocol, rate))
            usable = [(source, row) for source, row in candidates if row_is_usable(row)]
            if not usable:
                raise MatrixError(f"No usable result for {mode}/{protocol} rate={rate}")

            usable.sort(key=lambda item: as_float(item[1], "end_to_end_tps"))
            source, chosen_row = usable[len(usable) // 2] if rate in issues else usable[0]
            chosen = chosen_row.copy()
            chosen["selection"] = source if rate in issues else "initial"
            chosen["anomaly_reason"] = "; ".join(issues.get(rate, []))
            selected.append(chosen)

            for attempt_source, row in candidates:
                attempt = row.copy()
                attempt["attempt_source"] = attempt_source
                attempt["selected"] = "yes" if row is chosen_row else "no"
                attempt["anomaly_reason"] = "; ".join(issues.get(rate, []))
                attempts.append(attempt)

        return selected, attempts

    def write_mode_csv(self, mode: str, selected: list[dict[str, str]]) -> Path:
        path = self.csv_dir / f"{self.args.session}_{mode}_runs.csv"
        fields = CSV_FIELDS + ("selection", "anomaly_reason")
        order = {protocol: index for index, protocol in enumerate(PROTOCOLS)}
        rows = sorted(selected, key=lambda row: (order[row["protocol"]], int(row["rate"])))
        write_csv(path, rows, fields)
        return path

    def postcheck(self, modes: list[str], protocols: list[str], rates: list[int]) -> None:
        if self.manifest_path.exists():
            self.manifest = json.loads(self.manifest_path.read_text())
        all_attempts_path = self.csv_dir / f"{self.args.session}_attempts.csv"
        all_attempts = read_csv(all_attempts_path)
        postcheck_manifest = {}

        for mode in modes:
            mode_path = self.csv_dir / f"{self.args.session}_{mode}_runs.csv"
            mode_rows = read_csv(mode_path)
            if len(mode_rows) != len(protocols) * len(rates):
                raise MatrixError(
                    f"Postcheck requires a complete {mode} CSV: "
                    f"expected {len(protocols) * len(rates)} rows, found {len(mode_rows)}"
                )

            for protocol in protocols:
                protocol_rows = [row for row in mode_rows if row.get("protocol") == protocol]
                issues = detect_anomalies(protocol_rows, mode, rates)
                if not issues:
                    self.log(f"postcheck accepted: {mode}/{protocol}")
                    continue

                description = ", ".join(
                    f"{rate}:{'|'.join(reasons)}" for rate, reasons in sorted(issues.items())
                )
                self.log(f"postcheck anomalies: {mode}/{protocol}: {description}")
                postcheck_manifest[f"{mode}/{protocol}"] = {
                    str(rate): reasons for rate, reasons in issues.items()
                }

                for rate, reasons in sorted(issues.items()):
                    base = next(
                        (row for row in protocol_rows if row.get("rate") == str(rate)),
                        None,
                    )
                    candidates = []
                    if base is not None:
                        candidates.append(("postcheck-base", base))
                    candidates.extend(
                        ("postcheck-retry", row)
                        for row in self.retry_rate(mode, protocol, rate)
                    )
                    usable = [(source, row) for source, row in candidates if row_is_usable(row)]
                    if not usable:
                        raise MatrixError(
                            f"Postcheck found no usable result for {mode}/{protocol} rate={rate}"
                        )
                    usable.sort(key=lambda item: as_float(item[1], "end_to_end_tps"))
                    source, chosen_row = usable[len(usable) // 2]
                    replacement = chosen_row.copy()
                    replacement["selection"] = source
                    replacement["anomaly_reason"] = "; ".join(reasons)

                    mode_rows = [
                        replacement
                        if row.get("protocol") == protocol and row.get("rate") == str(rate)
                        else row
                        for row in mode_rows
                    ]
                    protocol_rows = [
                        replacement if row.get("rate") == str(rate) else row
                        for row in protocol_rows
                    ]

                    for attempt_source, row in candidates:
                        attempt = row.copy()
                        attempt["attempt_source"] = attempt_source
                        attempt["selected"] = "yes" if row is chosen_row else "no"
                        attempt["anomaly_reason"] = "; ".join(reasons)
                        all_attempts.append(attempt)

            self.write_mode_csv(mode, mode_rows)

        write_csv(
            all_attempts_path,
            all_attempts,
            CSV_FIELDS + ("attempt_source", "selected", "anomaly_reason"),
        )
        self.manifest["postcheck"] = {
            "completed_at": utc_now(),
            "anomalies": postcheck_manifest,
        }
        self.save_manifest()
        self.log(f"postcheck complete: {len(postcheck_manifest)} lines required retries")

    def run(self, modes: list[str], protocols: list[str], rates: list[int]) -> None:
        self.result_dir.mkdir(parents=True, exist_ok=True)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.manifest["status"] = "running"
        try:
            self.manifest["git_commit"] = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=self.benchmark_dir,
                check=True,
                capture_output=True,
                text=True,
            ).stdout.strip()
        except subprocess.SubprocessError:
            self.manifest["git_commit"] = "unknown"
        self.save_manifest()

        all_attempts: list[dict[str, str]] = []
        selected_by_mode: dict[str, list[dict[str, str]]] = defaultdict(list)
        schedule = [(mode, protocol) for mode in modes for protocol in protocols]
        for index, (mode, protocol) in enumerate(schedule):
            initial = self.run_initial_line(mode, protocol, rates, index)
            issues = detect_anomalies(initial, mode, rates)
            if issues:
                description = ", ".join(
                    f"{rate}:{'|'.join(reasons)}" for rate, reasons in sorted(issues.items())
                )
                self.log(f"line anomalies: {mode}/{protocol}: {description}")
            else:
                self.log(f"line accepted without retries: {mode}/{protocol}")

            selected, attempts = self.select_line(initial, issues, mode, protocol, rates)
            selected_by_mode[mode].extend(selected)
            all_attempts.extend(attempts)
            self.write_mode_csv(mode, selected_by_mode[mode])

            key = f"{mode}/{protocol}"
            self.manifest["lines"][key] = {
                "completed_at": utc_now(),
                "anomalies": {str(rate): reasons for rate, reasons in issues.items()},
                "selected_rows": len(selected),
            }
            self.save_manifest()
            if index + 1 < len(schedule):
                time.sleep(self.args.cooldown_seconds)

        attempts_path = self.csv_dir / f"{self.args.session}_attempts.csv"
        write_csv(
            attempts_path,
            all_attempts,
            CSV_FIELDS + ("attempt_source", "selected", "anomaly_reason"),
        )
        self.manifest["status"] = "complete"
        self.manifest["completed_at"] = utc_now()
        self.manifest["attempts_csv"] = str(attempts_path)
        self.save_manifest()
        self.log(f"matrix complete: {len(schedule)} lines, {len(schedule) * len(rates)} selected points")


def main() -> None:
    args = parse_args()
    args.rates = parse_csv_list(args.rates, int)
    args.protocols = parse_csv_list(args.protocols)
    args.modes = parse_csv_list(args.modes)
    if not args.rates or len(set(args.rates)) != len(args.rates):
        raise MatrixError("Rates must be nonempty and unique")
    if any(protocol not in PROTOCOLS for protocol in args.protocols):
        raise MatrixError(f"Protocols must be selected from {PROTOCOLS}")
    if any(mode not in MODES for mode in args.modes):
        raise MatrixError(f"Modes must be selected from {tuple(MODES)}")
    if args.retry_runs < 1:
        raise MatrixError("--retry-runs must be at least one")

    runner = MatrixRunner(args)
    runner.check_environment()
    if args.dry_run:
        print(json.dumps(runner.manifest, indent=2))
        return

    atexit.register(runner.cleanup)
    for signum in (signal.SIGINT, signal.SIGTERM):
        signal.signal(signum, lambda received, _frame: sys.exit(128 + received))

    try:
        runner.configure_netem()
        if args.postcheck_only:
            runner.postcheck(args.modes, args.protocols, args.rates)
        else:
            runner.run(args.modes, args.protocols, args.rates)
    except Exception:
        runner.manifest["status"] = "failed"
        runner.manifest["failed_at"] = utc_now()
        runner.save_manifest()
        raise
    finally:
        runner.cleanup()


if __name__ == "__main__":
    try:
        main()
    except MatrixError as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(2)
