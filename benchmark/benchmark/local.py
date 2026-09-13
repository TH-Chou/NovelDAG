# Copyright(C) Facebook, Inc. and its affiliates.
import subprocess
from math import ceil
from os.path import basename, splitext
from time import sleep

from benchmark.commands import CommandMaker
from benchmark.config import (
    Key,
    LocalCommittee,
    NodeParameters,
    BenchParameters,
    ConfigError,
)
from benchmark.logs import LogParser, ParseError
from benchmark.utils import Print, BenchError, PathMaker


class LocalBench:
    BASE_PORT = 4000

    def __init__(self, bench_parameters_dict, node_parameters_dict):
        try:
            self.bench_parameters = BenchParameters(bench_parameters_dict)
            self.node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError("Invalid nodes or bench parameters", e)

    def __getattr__(self, attr):
        return getattr(self.bench_parameters, attr)

    def _background_run(self, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f"RUST_LOG=info {command} 2> {log_file}"
        subprocess.run(["tmux", "new", "-d", "-s", name, cmd], check=True)

    def _kill_nodes(self):
        try:
            cmd = CommandMaker.kill().split()
            subprocess.run(cmd, stderr=subprocess.DEVNULL)
        except subprocess.SubprocessError as e:
            raise BenchError("Failed to kill testbed", e)

    def run(self, debug=False):
        assert isinstance(debug, bool)
        Print.heading("Starting local benchmark")

        # Kill any previous testbed.
        self._kill_nodes()

        try:
            Print.info("Setting up testbed...")
            nodes, rate = self.nodes[0], self.rate[0]

            # Cleanup all files.
            cmd = f"{CommandMaker.clean_logs()} ; {CommandMaker.cleanup()}"
            subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)
            sleep(0.5)  # Removing the store may take time.

            # Recompile the latest code.
            cmd = CommandMaker.compile().split()
            subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

            # Create alias for the client and nodes binary.
            cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
            subprocess.run([cmd], shell=True)

            # Generate configuration files.
            keys = []
            key_files = [PathMaker.key_file(i) for i in range(nodes)]
            for filename in key_files:
                cmd = CommandMaker.generate_key(filename).split()
                subprocess.run(cmd, check=True)
                keys += [Key.from_file(filename)]

            names = [x.name for x in keys]
            committee = LocalCommittee(names, self.BASE_PORT, self.workers)
            committee.print(PathMaker.committee_file())

            self.node_parameters.print(PathMaker.parameters_file())

            # Run the clients (they will wait for the nodes to be ready).
            silent_faults = self.faults if self.fault_mode == 'silence' else 0
            workers_addresses = committee.workers_addresses(silent_faults)
            active_workers = sum(len(addresses) for addresses in workers_addresses)
            if active_workers == 0:
                raise BenchError("No active workers available to inject transactions")
            rate_share = ceil(rate / active_workers)
            for i, addresses in enumerate(workers_addresses):
                for id, address in addresses:
                    cmd = CommandMaker.run_client(
                        address,
                        self.tx_size,
                        rate_share,
                        [x for y in workers_addresses for _, x in y],
                    )
                    log_file = PathMaker.client_log_file(i, id)
                    self._background_run(cmd, log_file)

            # Run the primaries. In silence mode, faulty authorities are not
            # started. In active attack modes, all authorities run and the
            # last `faults` primaries receive process-local attack settings.
            primary_addresses = committee.primary_addresses(silent_faults)
            all_primary_addresses = committee.primary_addresses(0)
            byzantine_start = nodes - self.faults
            dag_protocol = self.node_parameters.json['dag_protocol'].replace('-', '_')
            mahi_mahi = dag_protocol.startswith('mahi_mahi')
            for i, address in enumerate(primary_addresses):
                env = None
                if self.fault_mode == 'invalid_payload' and i >= byzantine_start:
                    env = 'NOVELDAG_BYZANTINE_ATTACK=invalid_payload'
                elif self.fault_mode == 'equivocation' and i >= byzantine_start:
                    byzantine_addresses = ','.join(
                        all_primary_addresses[byzantine_start:]
                    )
                    variants = max(1, nodes - self.faults if mahi_mahi else self.faults)
                    env = (
                        'NOVELDAG_BYZANTINE_ATTACK=equivocation '
                        f'NOVELDAG_DAG_PROTOCOL={dag_protocol} '
                        f'NOVELDAG_BYZANTINE_PRIMARY_ADDRS={byzantine_addresses} '
                        f'NOVELDAG_EQUIVOCATION_VARIANTS={variants}'
                    )
                cmd = CommandMaker.run_primary(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i),
                    PathMaker.parameters_file(),
                    debug=debug,
                    env=env,
                )
                log_file = PathMaker.primary_log_file(i)
                self._background_run(cmd, log_file)

            # Run the workers (except the faulty ones in silence mode).
            for i, addresses in enumerate(workers_addresses):
                for id, address in addresses:
                    env = None
                    if self.fault_mode == 'equivocation' and i >= byzantine_start:
                        authorities = list(committee.json['authorities'].values())
                        byzantine_worker_addresses = ','.join(
                            authority['workers'][id]['worker_to_worker']
                            for authority in authorities[byzantine_start:]
                        )
                        variants = max(
                            1, nodes - self.faults if mahi_mahi else self.faults
                        )
                        env = (
                            'NOVELDAG_BYZANTINE_ATTACK=equivocation '
                            f'NOVELDAG_DAG_PROTOCOL={dag_protocol} '
                            f'NOVELDAG_BYZANTINE_WORKER_ADDRS={byzantine_worker_addresses} '
                            f'NOVELDAG_EQUIVOCATION_VARIANTS={variants}'
                        )
                    cmd = CommandMaker.run_worker(
                        PathMaker.key_file(i),
                        PathMaker.committee_file(),
                        PathMaker.db_path(i, id),
                        PathMaker.parameters_file(),
                        id,  # The worker's id.
                        debug=debug,
                        env=env,
                    )
                    log_file = PathMaker.worker_log_file(i, id)
                    self._background_run(cmd, log_file)

            # Wait for all transactions to be processed.
            Print.info(f"Running benchmark ({self.duration} sec)...")
            sleep(self.duration)
            self._kill_nodes()

            # Parse logs and return the parser.
            Print.info("Parsing logs...")
            return LogParser.process(PathMaker.logs_path(), faults=self.faults)

        except (subprocess.SubprocessError, ParseError) as e:
            self._kill_nodes()
            raise BenchError("Failed to run benchmark", e)
