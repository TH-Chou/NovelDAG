#!/usr/bin/env python3
"""Manage a GCP testbed and run a resumable NovelDAG single-point matrix."""

import argparse
import csv
import json
import os
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import re
import shutil
import subprocess
import sys
import time


BENCHMARK_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SETTINGS = BENCHMARK_ROOT / "settings.gcp.json"
DEFAULT_PROTOCOLS = "narwhal,shortfin,mahi_mahi,wahoo"
SUMMARY_FIELDS = [
    "protocol",
    "nodes",
    "faults",
    "rate",
    "duration_s",
    "consensus_tps",
    "consensus_latency_ms",
    "end_to_end_tps",
    "end_to_end_latency_ms",
    "config_hash_verified",
]


def run(command, *, cwd=None, env=None, capture=False, check=True):
    display = " ".join(str(x) for x in command)
    print("+", display, flush=True)
    return subprocess.run(
        [str(x) for x in command],
        cwd=str(cwd) if cwd else None,
        env=env,
        text=True,
        capture_output=capture,
        check=check,
    )


class GcpTestbed:
    def __init__(self, settings_path, expected_nodes):
        self.settings_path = Path(settings_path).resolve()
        with self.settings_path.open(encoding="utf-8") as handle:
            settings = json.load(handle)

        instances = settings["instances"]
        self.project = instances["project"]
        self.label = instances["name"]
        self.key = Path(settings["key"]["path"]).expanduser()
        self.user = instances.get("ssh_user", settings["key"]["user"])
        self.configured_zones = list(instances["zones"])
        self.expected_regions = [self.region(zone) for zone in self.configured_zones]
        self.expected_nodes = expected_nodes
        self.expected_per_region = expected_nodes // len(self.expected_regions)
        self.expected_disk_size = int(instances["disk_size_gb"])
        self.expected_commit = instances.get("image_commit")
        self.machine_type = instances["type"]
        self.network = instances["network"]
        self.subnetwork = instances.get("subnetwork", "")
        self.firewall_rule = instances["firewall_rule"]
        self.image_project = instances["image_project"]
        self.image_family = instances["image_family"]

        if expected_nodes % len(self.expected_regions):
            raise ValueError("Node count must divide evenly across configured regions")

    @staticmethod
    def region(zone):
        return zone.rsplit("-", 1)[0]

    def gcloud_json(self, arguments):
        result = run(
            ["gcloud"] + list(arguments) + ["--project", self.project, "--format=json"],
            capture=True,
        )
        return json.loads(result.stdout or "[]")

    def instances(self):
        return self.gcloud_json(
            ["compute", "instances", "list", "--filter", "labels.name=" + self.label]
        )

    @staticmethod
    def zone_of(instance):
        return str(instance["zone"]).rsplit("/", 1)[-1]

    @staticmethod
    def public_ip(instance):
        for interface in instance.get("networkInterfaces", []):
            for access in interface.get("accessConfigs", []):
                if access.get("natIP"):
                    return str(access["natIP"])
        return None

    def validate_inventory(self, require_status=None):
        instances = self.instances()
        if len(instances) != self.expected_nodes:
            raise RuntimeError(
                "Expected {} instances, found {}".format(
                    self.expected_nodes, len(instances)
                )
            )

        by_region = Counter(self.region(self.zone_of(item)) for item in instances)
        expected = {region: self.expected_per_region for region in self.expected_regions}
        if dict(by_region) != expected:
            raise RuntimeError(
                "Unexpected regional layout: {} (expected {})".format(
                    dict(by_region), expected
                )
            )

        disks = self.gcloud_json(
            ["compute", "disks", "list", "--filter", "name~'^{}-'".format(self.label)]
        )
        invalid_disks = [
            disk
            for disk in disks
            if int(disk.get("sizeGb", 0)) != self.expected_disk_size
            or str(disk.get("type", "")).rsplit("/", 1)[-1] != "pd-ssd"
        ]
        if len(disks) != self.expected_nodes or invalid_disks:
            raise RuntimeError(
                "Disk inventory is not {} x {}GB pd-ssd".format(
                    self.expected_nodes, self.expected_disk_size
                )
            )

        statuses = Counter(str(item.get("status", "UNKNOWN")) for item in instances)
        if require_status and statuses != Counter({require_status: self.expected_nodes}):
            raise RuntimeError("Unexpected instance statuses: {}".format(dict(statuses)))

        zones = Counter(self.zone_of(item) for item in instances)
        print("Inventory: regions={} zones={} statuses={}".format(
            dict(sorted(by_region.items())),
            dict(sorted(zones.items())),
            dict(statuses),
        ))
        print("Disks: {} x {}GB pd-ssd".format(len(disks), self.expected_disk_size))
        return instances

    def _transition(self, action, source_statuses, target_status, timeout=900):
        instances = self.instances()
        by_zone = defaultdict(list)
        for item in instances:
            if item.get("status") in source_statuses:
                by_zone[self.zone_of(item)].append(item["name"])

        for zone, names in sorted(by_zone.items()):
            print("{} {} instance(s) in {}".format(action, len(names), zone))
            run(
                [
                    "gcloud",
                    "compute",
                    "instances",
                    action,
                ]
                + names
                + [
                    "--project",
                    self.project,
                    "--zone",
                    zone,
                    "--quiet",
                    "--async",
                ]
            )

        started_at = time.monotonic()
        deadline = started_at + timeout
        while time.monotonic() < deadline:
            current = self.instances()
            statuses = Counter(str(item.get("status", "UNKNOWN")) for item in current)
            print("Waiting for {}: {}".format(target_status, dict(statuses)), flush=True)
            if statuses == Counter({target_status: self.expected_nodes}):
                self.validate_inventory(require_status=target_status)
                return current
            if action == "start" and time.monotonic() - started_at >= 30:
                transitional = {"PROVISIONING", "STAGING", "REPAIRING", "STOPPING"}
                if not transitional.intersection(statuses) and statuses.get("TERMINATED"):
                    failed_zones = Counter(
                        self.zone_of(item)
                        for item in current
                        if item.get("status") == "TERMINATED"
                    )
                    raise RuntimeError(
                        "Instances failed to start in zones: {}".format(
                            dict(sorted(failed_zones.items()))
                        )
                    )
            time.sleep(5)
        raise TimeoutError("Timed out waiting for {}".format(target_status))

    def start(self):
        return self._transition("start", {"TERMINATED"}, "RUNNING")

    def stop(self):
        return self._transition(
            "stop",
            {"PROVISIONING", "STAGING", "RUNNING"},
            "TERMINATED",
        )

    def _available_zones(self, region, preferred_zone):
        zones = self.gcloud_json(
            [
                "compute",
                "zones",
                "list",
                "--filter",
                "name~'^{}-' AND status=UP".format(region),
            ]
        )
        names = sorted(str(item["name"]) for item in zones)
        return [preferred_zone] + [name for name in names if name != preferred_zone]

    @staticmethod
    def _capacity_error(result):
        output = "{}\n{}".format(result.stdout, result.stderr)
        return (
            "ZONE_RESOURCE_POOL_EXHAUSTED" in output
            or "resource_availability" in output
        )

    def _create_instance(self, name, zone, public_key):
        command = [
            "gcloud",
            "compute",
            "instances",
            "create",
            name,
            "--project",
            self.project,
            "--zone",
            zone,
            "--machine-type",
            self.machine_type,
            "--network",
            self.network,
            "--tags",
            self.firewall_rule,
            "--labels",
            "name={}".format(self.label),
            "--image-project",
            self.image_project,
            "--image-family",
            self.image_family,
            "--boot-disk-size",
            "{}GB".format(self.expected_disk_size),
            "--boot-disk-type",
            "pd-ssd",
            "--metadata",
            "enable-oslogin=FALSE,ssh-keys={}:{}".format(self.user, public_key),
            "--quiet",
        ]
        if self.subnetwork:
            command.extend(["--subnet", self.subnetwork])
        return run(command, capture=True, check=False)

    def replace_zone(self, source_zone, target_zone):
        source_region = self.region(source_zone)
        if source_zone == target_zone:
            raise ValueError("Source and target zones must differ")
        if source_region != self.region(target_zone):
            raise ValueError("Zone replacement must stay in the same GCP region")

        instances = self.validate_inventory(require_status="TERMINATED")
        source = sorted(
            item["name"] for item in instances if self.zone_of(item) == source_zone
        )
        if not source:
            raise RuntimeError("No instances found in {}".format(source_zone))

        public_key_path = Path(str(self.key) + ".pub")
        if not public_key_path.exists():
            raise RuntimeError("Missing SSH public key {}".format(public_key_path))
        public_key = public_key_path.read_text(encoding="utf-8").strip()

        print("Deleting {} stopped instance(s) in {}".format(len(source), source_zone))
        run(
            [
                "gcloud",
                "compute",
                "instances",
                "delete",
            ]
            + source
            + [
                "--project",
                self.project,
                "--zone",
                source_zone,
                "--delete-disks=all",
                "--quiet",
            ]
        )

        candidate_zones = self._available_zones(source_region, target_zone)
        placements = Counter()
        for name in source:
            last_result = None
            for zone in candidate_zones:
                result = self._create_instance(name, zone, public_key)
                if result.returncode == 0:
                    placements[zone] += 1
                    break
                last_result = result
                if self._capacity_error(result):
                    print("No capacity in {}; trying the next zone".format(zone))
                    continue
                raise RuntimeError(
                    "Failed to create {} in {}:\n{}".format(
                        name, zone, result.stderr.strip()
                    )
                )
            else:
                raise RuntimeError(
                    "No capacity for {} in {}: {}".format(
                        name,
                        source_region,
                        last_result.stderr.strip() if last_result else "no zones",
                    )
                )

        print("Replacement placements: {}".format(dict(sorted(placements.items()))))
        return self.validate_inventory()

    def local_commit(self):
        result = run(
            ["git", "rev-parse", "HEAD"],
            cwd=BENCHMARK_ROOT.parent,
            capture=True,
        )
        return result.stdout.strip()

    def _ssh(self, ip, remote_command):
        return run(
            [
                "ssh",
                "-o",
                "BatchMode=yes",
                "-o",
                "ConnectTimeout=12",
                "-o",
                "StrictHostKeyChecking=no",
                "-o",
                "UserKnownHostsFile=/dev/null",
                "-i",
                self.key,
                "{}@{}".format(self.user, ip),
                remote_command,
            ],
            capture=True,
            check=False,
        )

    def parallel_ssh(self, remote_command, collect_stdout=False):
        instances = self.validate_inventory(require_status="RUNNING")
        ips = [self.public_ip(item) for item in instances]
        if any(ip is None for ip in ips):
            raise RuntimeError("At least one running instance has no public IP")

        failures = []
        outputs = []
        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = {pool.submit(self._ssh, ip, remote_command): ip for ip in ips}
            for future in as_completed(futures):
                ip = futures[future]
                result = future.result()
                if result.returncode:
                    failures.append((ip, result.stderr.strip()))
                elif collect_stdout:
                    outputs.append(result.stdout.strip())

        if failures:
            raise RuntimeError("SSH check failed on {} node(s): {}".format(
                len(failures), failures[:3]
            ))
        return outputs

    def check_binaries(self, timeout=180):
        commit = self.expected_commit or self.local_commit()
        command = (
            "test -x /home/{user}/node && "
            "test -x /home/{user}/benchmark_client && "
            "test \"$(git -C /home/{user}/NovelDAG rev-parse HEAD)\" = {commit}"
        ).format(user=self.user, commit=commit)
        deadline = time.monotonic() + timeout
        while True:
            try:
                self.parallel_ssh(command)
                break
            except RuntimeError as error:
                if time.monotonic() >= deadline:
                    raise
                print("Nodes are not SSH-ready yet: {}".format(error), flush=True)
                time.sleep(10)
        print("Binary check: {}/{} nodes at commit {}".format(
            self.expected_nodes, self.expected_nodes, commit[:8]
        ))

    def check_distributed_config(self):
        home = "/home/{}".format(self.user)
        command = (
            "test -s {home}/logs/.committee.json && "
            "test -s {home}/logs/.parameters.json && "
            "ls {home}/logs/.node-*.json >/dev/null 2>&1 && "
            "sha256sum {home}/logs/.committee.json {home}/logs/.parameters.json"
        ).format(home=home)
        outputs = self.parallel_ssh(command, collect_stdout=True)
        hashes = {output for output in outputs}
        if len(hashes) != 1:
            raise RuntimeError(
                "Committee/parameter hashes differ across nodes: {} variants".format(
                    len(hashes)
                )
            )
        print("Config check: identical committee and parameters on all nodes")


def find_fab():
    candidate = shutil.which("fab")
    if candidate:
        return candidate
    fallback = Path.home() / ".local" / "bin" / "fab"
    if fallback.exists():
        return str(fallback)
    raise RuntimeError("Fabric executable 'fab' was not found")


def parse_result(path):
    text = Path(path).read_text(encoding="utf-8")

    def metric(pattern):
        match = re.search(pattern, text)
        if not match:
            raise RuntimeError("Missing metric {!r} in {}".format(pattern, path))
        return int(match.group(1).replace(",", ""))

    return {
        "consensus_tps": metric(r"Consensus TPS: ([\d,]+) tx/s"),
        "consensus_latency_ms": metric(r"Consensus latency: ([\d,]+) ms"),
        "end_to_end_tps": metric(r"End-to-end TPS: ([\d,]+) tx/s"),
        "end_to_end_latency_ms": metric(r"End-to-end latency: ([\d,]+) ms"),
    }


def write_summary(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=SUMMARY_FIELDS)
        writer.writeheader()
        writer.writerows(rows)
    temporary.replace(path)


def run_protocol(testbed, args, protocol, archive_root):
    result_path = BENCHMARK_ROOT / "logs" / "results" / (
        "bench-{faults}-{nodes}-1-True-{rate}-512-{protocol}-run1.txt".format(
            faults=args.faults,
            nodes=args.nodes,
            rate=args.rate,
            protocol=protocol,
        )
    )
    result_path.unlink(missing_ok=True)

    environment = os.environ.copy()
    environment["NOVELDAG_SKIP_REMOTE_BUILD"] = "1"
    run(
        [
            find_fab(),
            "remote",
            "--settings={}".format(args.settings),
            "--dag-protocol={}".format(protocol),
            "--protocol={}".format(args.consensus),
            "--nodes={}".format(args.nodes),
            "--faults={}".format(args.faults),
            "--workers=1",
            "--rate={}".format(args.rate),
            "--tx-size=512",
            "--duration={}".format(args.duration),
            "--runs=1",
            "--benchmark-delay={}".format(args.benchmark_delay),
        ],
        cwd=BENCHMARK_ROOT,
        env=environment,
    )

    if not result_path.exists():
        raise RuntimeError("Benchmark did not produce {}".format(result_path))

    testbed.check_distributed_config()
    protocol_archive = archive_root / protocol
    protocol_archive.mkdir(parents=True, exist_ok=True)
    rate_logs = BENCHMARK_ROOT / "logs" / "rate-{}".format(args.rate)
    if rate_logs.exists():
        destination = protocol_archive / rate_logs.name
        if destination.exists():
            shutil.rmtree(str(destination))
        shutil.move(str(rate_logs), str(destination))
    shutil.copy2(str(result_path), str(protocol_archive / result_path.name))
    return parse_result(result_path)


def run_matrix(testbed, args):
    protocols = [item.strip() for item in args.protocols.split(",") if item.strip()]
    run_id = args.run_id or "gcp-n{}-f{}-r{}-{}".format(
        args.nodes,
        args.faults,
        args.rate,
        datetime.now().strftime("%Y%m%d-%H%M%S"),
    )
    archive_root = BENCHMARK_ROOT / "logs" / run_id
    summary_path = BENCHMARK_ROOT / "csv_plots" / (run_id + ".csv")
    archive_root.mkdir(parents=True, exist_ok=True)

    rows = []
    if args.resume and summary_path.exists():
        with summary_path.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))
    completed = {row["protocol"] for row in rows}

    try:
        testbed.start()
        testbed.check_binaries()
        for protocol in protocols:
            if protocol in completed:
                print("Skipping completed protocol {}".format(protocol))
                continue
            print("=== Running {} ===".format(protocol), flush=True)
            metrics = run_protocol(testbed, args, protocol, archive_root)
            row = {
                "protocol": protocol,
                "nodes": args.nodes,
                "faults": args.faults,
                "rate": args.rate,
                "duration_s": args.duration,
                "config_hash_verified": "yes",
            }
            row.update(metrics)
            rows.append(row)
            write_summary(summary_path, rows)
            print("Completed {}: {}".format(protocol, metrics), flush=True)
    finally:
        if args.keep_running:
            print("Leaving instances running because --keep-running was set")
        else:
            print("Stopping the testbed", flush=True)
            testbed.stop()

    print("Summary:", summary_path)
    print("Archived logs:", archive_root)


def build_parser():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("status", "start", "stop", "replace-zone", "run")
    )
    parser.add_argument("--settings", default=str(DEFAULT_SETTINGS))
    parser.add_argument("--nodes", type=int, default=50)
    parser.add_argument("--faults", type=int, default=0)
    parser.add_argument("--rate", type=int, default=120000)
    parser.add_argument("--duration", type=int, default=40)
    parser.add_argument("--benchmark-delay", type=int, default=20)
    parser.add_argument("--protocols", default=DEFAULT_PROTOCOLS)
    parser.add_argument("--consensus", default="round_robin")
    parser.add_argument("--run-id")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--keep-running", action="store_true")
    parser.add_argument("--source-zone")
    parser.add_argument("--target-zone")
    return parser


def main():
    args = build_parser().parse_args()
    testbed = GcpTestbed(args.settings, args.nodes)
    if args.command == "status":
        testbed.validate_inventory()
    elif args.command == "start":
        testbed.start()
        testbed.check_binaries()
    elif args.command == "stop":
        testbed.stop()
    elif args.command == "replace-zone":
        if not args.source_zone or not args.target_zone:
            raise ValueError("replace-zone requires --source-zone and --target-zone")
        testbed.replace_zone(args.source_zone, args.target_zone)
    else:
        run_matrix(testbed, args)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Interrupted", file=sys.stderr)
        raise SystemExit(130)
