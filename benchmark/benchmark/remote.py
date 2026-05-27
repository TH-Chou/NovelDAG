# Copyright(C) Facebook, Inc. and its affiliates.
from collections import OrderedDict
from fabric import Connection, ThreadingGroup as Group
from fabric.exceptions import GroupException
from paramiko import RSAKey
from paramiko.ssh_exception import PasswordRequiredException, SSHException
from concurrent.futures import ThreadPoolExecutor, as_completed
from os.path import basename, join, splitext
from pathlib import Path
from time import sleep
from math import ceil
from copy import deepcopy
from shutil import rmtree
import subprocess
import threading
import re
import socket
import time
import importlib.util

from benchmark.config import Committee, Key, NodeParameters, BenchParameters, ConfigError
from benchmark.utils import BenchError, Print, PathMaker, progress_bar
from benchmark.commands import CommandMaker
from benchmark.logs import LogParser, ParseError
from benchmark.instance import InstanceManager


class FabricError(Exception):
    ''' Wrapper for Fabric exception with a meaningfull error message. '''

    def __init__(self, error):
        assert isinstance(error, GroupException)
        message = list(error.result.values())[-1]
        super().__init__(message)


class ExecutionError(Exception):
    pass


class Bench:
    SSH_RETRIES = 5
    SSH_RETRY_DELAY_SECONDS = 3
    PRIMARY_READY_TIMEOUT_SECONDS = 60
    PRIMARY_READY_POLL_SECONDS = 1
    PRIMARY_READY_CONNECT_TIMEOUT_SECONDS = 1

    def __init__(self, ctx, settings_file='settings.json'):
        self.manager = InstanceManager.make(settings_file)
        self.settings = self.manager.settings
        try:
            ctx.connect_kwargs.pkey = RSAKey.from_private_key_file(
                self.manager.settings.key_path
            )
            ctx.connect_kwargs.timeout = 60
            ctx.connect_kwargs.banner_timeout = 60
            self.connect = ctx.connect_kwargs
        except (IOError, PasswordRequiredException, SSHException) as e:
            raise BenchError('Failed to load SSH key', e)

    def _check_stderr(self, output):
        if isinstance(output, dict):
            for x in output.values():
                if x.stderr:
                    raise ExecutionError(x.stderr)
        else:
            if output.stderr:
                raise ExecutionError(output.stderr)

    def install(self):
        Print.info('Installing rust and cloning the repo...')
        cmd = [
            'sudo apt-get update',
            'sudo apt-get -y upgrade',
            'sudo apt-get -y autoremove',
            'sudo apt-get -y install build-essential cmake clang',
            'curl --proto "=https" --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y',
            'source $HOME/.cargo/env',
            'rustup default stable',
            f'(git clone {self.settings.repo_url} || (cd {self.settings.repo_name} ; git pull))',
        ]
        hosts = self.manager.hosts(flat=True)
        for attempt in range(1, self.SSH_RETRIES + 1):
            try:
                g = Group(*hosts, user=self.settings.ssh_user, connect_kwargs=self.connect)
                g.run(' && '.join(cmd), hide=True)
                Print.heading(f'Initialized testbed of {len(hosts)} nodes')
                return
            except (GroupException, ExecutionError) as e:
                if 'Error reading SSH protocol banner' in str(e) and attempt < self.SSH_RETRIES:
                    Print.warn(
                        f'SSH transient error while installing '
                        f'(attempt {attempt}/{self.SSH_RETRIES}); retrying...'
                    )
                    sleep(self.SSH_RETRY_DELAY_SECONDS)
                    continue
                e = FabricError(e) if isinstance(e, GroupException) else e
                raise BenchError('Failed to install repo on testbed', e)

    def kill(self, hosts=[], delete_logs=False):
        assert isinstance(hosts, list)
        assert isinstance(delete_logs, bool)
        hosts = hosts if hosts else self.manager.hosts(flat=True)
        delete_logs = CommandMaker.clean_run_logs() if delete_logs else 'true'
        # Defensive cleanup: normal kill (tmux kill-server) + force-kill
        # any lingering processes that might have escaped tmux.
        force_kill = '(pkill -9 -f "^node$" || true) ; (pkill -9 -f "^benchmark_client$" || true)'
        cmd = [delete_logs, f'({CommandMaker.kill()} || true)', force_kill]
        for attempt in range(1, self.SSH_RETRIES + 1):
            try:
                g = Group(*hosts, user=self.settings.ssh_user, connect_kwargs=self.connect)
                g.run(' && '.join(cmd), hide=True)
                return
            except GroupException as e:
                if 'Error reading SSH protocol banner' in str(e) and attempt < self.SSH_RETRIES:
                    Print.warn(
                        f'SSH transient error while killing nodes '
                        f'(attempt {attempt}/{self.SSH_RETRIES}); retrying...'
                    )
                    sleep(self.SSH_RETRY_DELAY_SECONDS)
                    continue
                raise BenchError('Failed to kill nodes', FabricError(e))

    def _count_alive(self, hosts):
        """Pgrep each host to count how many still run the primary process.
        Returns 0 if all are dead (abort), otherwise the count of alive hosts."""
        dead = 0
        for host in hosts:
            try:
                c = Connection(host, user=self.settings.ssh_user, connect_kwargs=self.connect)
                result = c.run('pgrep -c "^node$" || true', hide=True)
                count = int(result.stdout.strip() or '0')
                if count == 0:
                    dead += 1
            except Exception:
                dead += 1
        return len(hosts) - dead

    @staticmethod
    def _split_host_port(address):
        host, port = address.rsplit(':', 1)
        return host, int(port)

    def _can_connect(self, address):
        host, port = self._split_host_port(address)
        try:
            with socket.create_connection(
                (host, port),
                timeout=self.PRIMARY_READY_CONNECT_TIMEOUT_SECONDS,
            ):
                return True
        except OSError:
            return False

    def _wait_for_primary_ports(self, addresses):
        """Block until every active primary-to-primary port accepts TCP.

        Primaries are launched in parallel, but on a multi-region testbed the
        first node can start proposing while some peers are still binding their
        sockets. This barrier narrows that startup skew before clients are
        released and makes early-round Shortfin parent collection less fragile.
        """
        assert isinstance(addresses, list)
        if not addresses:
            raise ExecutionError('No active primary ports to wait for')

        deadline = time.monotonic() + self.PRIMARY_READY_TIMEOUT_SECONDS
        pending = set(addresses)
        Print.info(f'Waiting for {len(pending)} active primary ports...')

        while pending:
            with ThreadPoolExecutor(max_workers=min(32, len(pending))) as pool:
                futures = {
                    pool.submit(self._can_connect, address): address
                    for address in pending
                }
                ready = {
                    futures[future]
                    for future in as_completed(futures)
                    if future.result()
                }

            pending -= ready
            if not pending:
                Print.info('All active primary ports are reachable')
                return

            if time.monotonic() >= deadline:
                missing = ', '.join(sorted(pending))
                raise ExecutionError(
                    'Primary ready barrier timed out; '
                    f'unreachable primary ports: {missing}'
                )

            sleep(self.PRIMARY_READY_POLL_SECONDS)

    def _select_hosts(self, bench_parameters):
        # Collocate the primary and its workers on the same machine.
        if bench_parameters.collocate:
            nodes = max(bench_parameters.nodes)

            # Ensure there are enough hosts.
            hosts = self.manager.hosts()
            if sum(len(x) for x in hosts.values()) < nodes:
                return []

            # Select hosts round-robin across regions. Do not use zip(*hosts):
            # it truncates to the smallest region and can silently return fewer
            # than nodes when AWS has an uneven region distribution.
            per_region = [list(ips) for ips in hosts.values()]
            ordered = []
            index = 0
            while len(ordered) < nodes:
                progressed = False
                for ips in per_region:
                    if index < len(ips):
                        ordered.append(ips[index])
                        progressed = True
                        if len(ordered) == nodes:
                            break
                if not progressed:
                    break
                index += 1
            return ordered if len(ordered) == nodes else []

        # Spawn the primary and each worker on a different machine. Each
        # authority runs in a single data center.
        else:
            primaries = max(bench_parameters.nodes)

            # Ensure there are enough hosts.
            hosts = self.manager.hosts()
            if len(hosts.keys()) < primaries:
                return []
            for ips in hosts.values():
                if len(ips) < bench_parameters.workers + 1:
                    return []

            # Ensure the primary and its workers are in the same region.
            selected = []
            for region in list(hosts.keys())[:primaries]:
                ips = list(hosts[region])[:bench_parameters.workers + 1]
                selected.append(ips)
            return selected

    def _background_run(self, host, command, log_file):
        name = splitext(basename(log_file))[0]
        cmd = f'tmux new -d -s "{name}" "{command} |& tee {log_file}"'
        last_error = None
        for attempt in range(1, self.SSH_RETRIES + 1):
            try:
                c = Connection(host, user=self.settings.ssh_user, connect_kwargs=self.connect)
                output = c.run(cmd, hide=True)
                self._check_stderr(output)
                return
            except (SSHException, OSError, EOFError) as e:
                last_error = e
                if attempt == self.SSH_RETRIES:
                    break
                Print.warn(
                    f'SSH transient error on {host} while starting {name} '
                    f'(attempt {attempt}/{self.SSH_RETRIES}): {e}'
                )
                sleep(self.SSH_RETRY_DELAY_SECONDS)

        raise ExecutionError(
            f'Failed to start remote command on {host} after '
            f'{self.SSH_RETRIES} attempts: {last_error}'
        )

    def _background_run_many(self, jobs, label):
        assert isinstance(jobs, list)
        assert isinstance(label, str) and label
        if not jobs:
            return

        Print.info(f'Booting {label} on {len(jobs)} host(s)...')
        workers = min(len(jobs), 16)
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futures = [
                pool.submit(self._background_run, host, cmd, log_file)
                for host, cmd, log_file in jobs
            ]
            for f in as_completed(futures):
                f.result()

    @staticmethod
    def _active_hosts(committee, faults):
        hosts = set()
        for address in committee.primary_addresses(faults):
            hosts.add(Committee.ip(address))
        for addresses in committee.workers_addresses(faults):
            for _, address in addresses:
                hosts.add(Committee.ip(address))
        return list(hosts)

    def _update(self, hosts, collocate):
        if collocate:
            ips = list(set(hosts))
        else:
            ips = list(set([x for y in hosts for x in y]))

        Print.info(
            f'Updating {len(ips)} machines (branch "{self.settings.branch}")...'
        )
        repo = self.settings.repo_name
        workspace_discovery = (
            f'if [ -d "{repo}/NovelDAG/node" ]; then '
            f'echo "{repo}/NovelDAG"; '
            f'elif [ -d "{repo}/node" ]; then '
            f'echo "{repo}"; '
            'else '
            'echo "Cannot locate node crate directory" >&2; '
            'exit 1; '
            'fi'
        )
        cmd = [
            f'(cd {repo} && git fetch -f)',
            f'(cd {repo} && git checkout -f {self.settings.branch})',
            f'(cd {repo} && git pull -f)',
            'source $HOME/.cargo/env',
            f'WORKSPACE_DIR=$({workspace_discovery})',
            f'(cd "$WORKSPACE_DIR/node" && {CommandMaker.compile()})',
            'rm -f node benchmark_client',
            'ln -s "./$WORKSPACE_DIR/target/release/node" node',
            'ln -s "./$WORKSPACE_DIR/target/release/benchmark_client" benchmark_client',
            'test -x ./node',
            'test -x ./benchmark_client',
        ]
        g = Group(*ips, user=self.settings.ssh_user, connect_kwargs=self.connect)
        g.run(' && '.join(cmd), hide=True)

    def _config(self, hosts, node_parameters, bench_parameters):
        Print.info('Generating configuration files...')

        # Cleanup all local configuration files.
        cmd = CommandMaker.cleanup()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Recompile the latest code.
        cmd = CommandMaker.compile().split()
        subprocess.run(cmd, check=True, cwd=PathMaker.node_crate_path())

        # Create alias for the client and nodes binary.
        cmd = CommandMaker.alias_binaries(PathMaker.binary_path())
        subprocess.run([cmd], shell=True)

        # Generate configuration files.
        keys = []
        key_files = [PathMaker.key_file(i) for i in range(len(hosts))]
        for filename in key_files:
            cmd = CommandMaker.generate_key(filename).split()
            subprocess.run(cmd, check=True)
            keys += [Key.from_file(filename)]

        names = [x.name for x in keys]

        if bench_parameters.collocate:
            workers = bench_parameters.workers
            addresses = OrderedDict(
                (x, [y] * (workers + 1)) for x, y in zip(names, hosts)
            )
        else:
            addresses = OrderedDict(
                (x, y) for x, y in zip(names, hosts)
            )
        committee = Committee(addresses, self.settings.base_port)
        committee.print(PathMaker.committee_file())

        node_parameters.print(PathMaker.parameters_file())

        # Cleanup all nodes and upload configuration files.
        names = names[:len(names)-bench_parameters.faults]
        progress = progress_bar(names, prefix='Uploading config files:')
        for i, name in enumerate(progress):
            for ip in committee.ips(name):
                c = Connection(ip, user=self.settings.ssh_user, connect_kwargs=self.connect)
                c.run(f'{CommandMaker.cleanup()} || true', hide=True)
                c.put(PathMaker.committee_file(), '.')
                c.put(PathMaker.key_file(i), '.')
                c.put(PathMaker.parameters_file(), '.')
                c.run('mkdir -p logs && cp .committee.json .parameters.json logs/ && for f in .node-*.json; do cp "$f" logs/; done', hide=True)

        return committee

    def _run_single(
        self,
        rate,
        committee,
        bench_parameters,
        debug=False,
        clean_logs=True,
    ):
        faults = bench_parameters.faults

        # Kill any potentially unfinished run and (optionally) delete logs.
        hosts = committee.ips()
        self.kill(hosts=hosts, delete_logs=clean_logs)

        workers_addresses = committee.workers_addresses(faults)
        active_workers = sum(len(addresses) for addresses in workers_addresses)
        if active_workers == 0:
            raise BenchError('No active workers available to inject transactions')
        rate_share = ceil(rate / active_workers)

        # Start workers first so transaction endpoints and worker-to-primary
        # connectors are up before primaries start proposing. Each phase is
        # started in parallel to avoid a multi-region serial SSH skew.
        worker_jobs = []
        for i, addresses in enumerate(workers_addresses):
            for (id, address) in addresses:
                host = Committee.ip(address)
                cmd = CommandMaker.run_worker(
                    PathMaker.key_file(i),
                    PathMaker.committee_file(),
                    PathMaker.db_path(i, id),
                    PathMaker.parameters_file(),
                    id,
                    debug=debug
                )
                worker_jobs.append((host, cmd, PathMaker.worker_log_file(i, id)))
        self._background_run_many(worker_jobs, 'workers')

        primary_addresses = committee.primary_addresses(faults)
        primary_jobs = []
        for i, address in enumerate(primary_addresses):
            host = Committee.ip(address)
            cmd = CommandMaker.run_primary(
                PathMaker.key_file(i),
                PathMaker.committee_file(),
                PathMaker.db_path(i),
                PathMaker.parameters_file(),
                debug=debug
            )
            primary_jobs.append((host, cmd, PathMaker.primary_log_file(i)))
        self._background_run_many(primary_jobs, 'primaries')
        self._wait_for_primary_ports(primary_addresses)

        # Delay before starting clients (let the P2P mesh stabilize).
        if bench_parameters.benchmark_delay > 0:
            Print.info(f'Waiting {bench_parameters.benchmark_delay}s for P2P mesh to stabilize...')
            sleep(bench_parameters.benchmark_delay)

        # Run the clients last. They still wait for workers to be ready, but at
        # this point the consensus mesh has already had a chance to connect.
        client_jobs = []
        client_timeout = bench_parameters.duration
        for i, addresses in enumerate(workers_addresses):
            for (id, address) in addresses:
                host = Committee.ip(address)
                cmd = CommandMaker.run_client(
                    address,
                    bench_parameters.tx_size,
                    rate_share,
                    [x for y in workers_addresses for _, x in y]
                )
                cmd = f'timeout --kill-after=5s {client_timeout}s {cmd}'
                client_jobs.append((host, cmd, PathMaker.client_log_file(i, id)))
        self._background_run_many(client_jobs, 'clients')

        # Wait for the configured wall-clock duration. Keep this progress bar
        # time-based; SSH probes here can make short runs look much longer than
        # the configured benchmark duration.
        duration = bench_parameters.duration
        deadline = time.monotonic() + duration
        progress_steps = max(1, duration)
        prefix = f'Running benchmark ({duration} sec):'
        for step in progress_bar(range(progress_steps), prefix=prefix):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                break
            sleep(min(1, remaining))
        self.kill(hosts=hosts, delete_logs=False)

    def _logs(self, committee, faults):
        # Delete local logs (if any).
        cmd = CommandMaker.clean_logs()
        subprocess.run([cmd], shell=True, stderr=subprocess.DEVNULL)

        # Build download task list: (host, remote_path, local_path)
        tasks = []
        workers_addresses = committee.workers_addresses(faults)
        for i, addresses in enumerate(workers_addresses):
            for id, address in addresses:
                host = Committee.ip(address)
                tasks.append((host,
                    PathMaker.client_log_file(i, id),
                    PathMaker.client_log_file(i, id)))
                tasks.append((host,
                    PathMaker.worker_log_file(i, id),
                    PathMaker.worker_log_file(i, id)))

        primary_addresses = committee.primary_addresses(faults)
        for i, address in enumerate(primary_addresses):
            host = Committee.ip(address)
            tasks.append((host,
                PathMaker.primary_log_file(i),
                PathMaker.primary_log_file(i)))

        # Parallel download with retry and conservative concurrency.
        results = {'done': 0, 'errors': 0}
        lock = threading.Lock()

        def _download_one(host, remote, local):
            for attempt in range(1, self.SSH_RETRIES + 1):
                try:
                    c = Connection(host, user=self.settings.ssh_user, connect_kwargs=self.connect)
                    c.get(remote, local=local)
                    with lock:
                        results['done'] += 1
                    return
                except Exception as e:
                    if attempt == self.SSH_RETRIES:
                        with lock:
                            results['errors'] += 1
                        Print.warn(f'Failed to download {remote} from {host}: {e}')
                    else:
                        sleep(self.SSH_RETRY_DELAY_SECONDS)

        Print.info(f'Downloading logs from {len(tasks)} remote paths...')
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [
                pool.submit(_download_one, host, remote, local)
                for host, remote, local in tasks
            ]
            for f in as_completed(futures):
                f.result()  # surface any unexpected exceptions

        Print.info(
            f'Downloaded {results["done"]}/{len(tasks)} files'
            + (f' ({results["errors"]} errors)' if results["errors"] else '')
        )

        # Parse logs and return the parser.
        Print.info('Parsing logs and computing performance...')
        return LogParser.process(PathMaker.logs_path(), faults=faults)

    def _checkpoint_logs(self, hosts, batch_id, run_id):
        assert isinstance(hosts, list)
        assert isinstance(batch_id, str) and batch_id
        assert isinstance(run_id, str) and run_id
        if not hosts:
            return

        remote_dir = f'batch_logs/{batch_id}/{run_id}'
        cmd = [
            f'mkdir -p "{remote_dir}"',
            f'for f in "{PathMaker.logs_path()}"/*.log; do [ -f "$f" ] || continue; cp "$f" "{remote_dir}/"; done',
        ]
        g = Group(*hosts, user=self.settings.ssh_user, connect_kwargs=self.connect)
        g.run(' && '.join(cmd), hide=True)

    def _batch_download(self, hosts, protocol, rates):
        """Download all checkpointed logs from all hosts in parallel,
        organising them into per-rate local directories."""
        for r in rates:
            rmtree(join(PathMaker.logs_path(), f'rate-{r}'), ignore_errors=True)

        # Flatten hosts list.
        flat_hosts = []
        for h in hosts:
            if isinstance(h, list):
                flat_hosts.extend(h)
            else:
                flat_hosts.append(h)

        # Build download task list: (host, remote_path, local_path)
        tasks = []
        for host in flat_hosts:
            for r in rates:
                # Find all checkpoint dirs for this rate (handles multiple runs).
                result = None
                for attempt in range(1, self.SSH_RETRIES + 1):
                    try:
                        c = Connection(
                            host,
                            user=self.settings.ssh_user,
                            connect_kwargs=self.connect,
                        )
                        result = c.run(
                            f'find batch_logs/{protocol} -path "*/r{r}-run*/*.log" 2>/dev/null || true',
                            hide=True,
                            warn=True,
                        )
                        break
                    except Exception as e:
                        if attempt == self.SSH_RETRIES:
                            Print.warn(
                                f'Failed to scan checkpoint logs on {host} '
                                f'for rate={r}: {e}'
                            )
                        else:
                            sleep(self.SSH_RETRY_DELAY_SECONDS)
                if result is None:
                    continue
                if not result.stdout.strip():
                    continue
                for log_file in result.stdout.strip().split('\n'):
                    run_dir = Path(log_file).parent.name
                    local_dir = join(PathMaker.logs_path(), f'rate-{r}', run_dir)
                    Path(local_dir).mkdir(parents=True, exist_ok=True)
                    log_name = basename(log_file)
                    tasks.append((host, log_file, join(local_dir, log_name)))

        if not tasks:
            Print.warn('No checkpointed logs found on any host')
            return

        # Parallel download with retry and conservative concurrency.
        results = {'done': 0, 'errors': 0}
        lock = threading.Lock()

        def _download_one(host, remote, local):
            for attempt in range(1, self.SSH_RETRIES + 1):
                try:
                    c = Connection(host, user=self.settings.ssh_user, connect_kwargs=self.connect)
                    c.get(remote, local=local)
                    with lock:
                        results['done'] += 1
                    return
                except Exception as e:
                    if attempt == self.SSH_RETRIES:
                        with lock:
                            results['errors'] += 1
                        Print.warn(f'Failed to download {remote} from {host}: {e}')
                    else:
                        sleep(self.SSH_RETRY_DELAY_SECONDS)

        Print.info(
            f'Downloading {len(tasks)} log files from '
            f'{len(flat_hosts)} hosts in parallel...'
        )
        with ThreadPoolExecutor(max_workers=8) as pool:
            futures = [
                pool.submit(_download_one, host, remote, local)
                for host, remote, local in tasks
            ]
            for f in as_completed(futures):
                f.result()

        Print.info(
            f'Downloaded {results["done"]}/{len(tasks)} files'
            + (f' ({results["errors"]} errors)' if results["errors"] else '')
        )

    @staticmethod
    def _run_number_from_dir(run_dir):
        run_match = re.search(r'run(\d+)$', run_dir.name)
        return int(run_match.group(1)) if run_match else None

    @staticmethod
    def _load_fast_log_parser():
        script_path = Path(__file__).resolve().parent.parent / 'scripts' / 'fast_parse_logs.py'
        spec = importlib.util.spec_from_file_location('benchmark_fast_parse_logs', script_path)
        if spec is None or spec.loader is None:
            raise ParseError(f'Failed to load fast parser from {script_path}')
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _result_output_file(self, faults, nodes, bench_parameters, rate, run_dir, protocol):
        run_number = self._run_number_from_dir(run_dir)
        if run_number is not None:
            return PathMaker.run_result_file(
                faults,
                nodes,
                bench_parameters.workers,
                bench_parameters.collocate,
                rate,
                bench_parameters.tx_size,
                run_number,
                protocol,
            )
        return PathMaker.result_file(
            faults,
            nodes,
            bench_parameters.workers,
            bench_parameters.collocate,
            rate,
            bench_parameters.tx_size,
            protocol,
        )

    def _parse_run_logs(self, run_dir, faults, nodes, bench_parameters, rate, protocol):
        output_file = self._result_output_file(
            faults, nodes, bench_parameters, rate, run_dir, protocol
        )
        if protocol == 'wahoo':
            Print.info(f'Fast-parsing Wahoo logs in {run_dir}')
            fast_parser = self._load_fast_log_parser()
            parsed = fast_parser.parse_directory(Path(run_dir))
            result = fast_parser.format_result(parsed, faults)
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            Path(output_file).write_text(result)
            return

        logger = LogParser.process(str(run_dir), faults=faults)
        logger.print(output_file)

    def run_batch(self, bench_parameters_dict, node_parameters_dict, batch_id, debug=False):
        assert isinstance(debug, bool)
        assert isinstance(batch_id, str) and batch_id
        Print.heading(f'Starting remote batch benchmark (batch_id={batch_id})')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return

        try:
            self._update(selected_hosts, bench_parameters.collocate)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        try:
            committee = self._config(
                selected_hosts, node_parameters, bench_parameters
            )
        except (subprocess.SubprocessError, GroupException) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to configure nodes', e)

        for n in bench_parameters.nodes:
            committee_copy = deepcopy(committee)
            committee_copy.remove_nodes(committee.size() - n)

            for r in bench_parameters.rate:
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')

                for i in range(bench_parameters.runs):
                    run_id = f'f{bench_parameters.faults}-n{n}-r{r}-run{i+1}'
                    Print.heading(f'Run {i+1}/{bench_parameters.runs} [{run_id}]')
                    try:
                        self._run_single(
                            r,
                            committee_copy,
                            bench_parameters,
                            debug,
                        )
                        self._checkpoint_logs(
                            committee_copy.ips(),
                            batch_id,
                            run_id,
                        )
                    except (BenchError, subprocess.SubprocessError, GroupException, ParseError, ExecutionError) as e:
                        self.kill(hosts=selected_hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        if isinstance(e, BenchError):
                            Print.error(e)
                        else:
                            Print.error(BenchError('Batch benchmark run failed', e))
                        continue

    def collect_batch(self, batch_id, output_directory='batch_downloads'):
        assert isinstance(batch_id, str) and batch_id
        assert isinstance(output_directory, str) and output_directory

        hosts = self.manager.hosts(flat=True)
        if not hosts:
            Print.warn('There are no instances available to collect logs from')
            return

        output_path = Path(output_directory).expanduser().resolve()
        output_path.mkdir(parents=True, exist_ok=True)

        Print.info(
            f'Downloading archived logs for batch_id={batch_id} into {output_path}'
        )
        progress = progress_bar(hosts, prefix='Downloading archived logs:')
        for host in progress:
            host_tag = host.replace('.', '-')
            remote_archive = f'/tmp/{batch_id}-{host_tag}.tar.gz'
            local_archive = output_path / f'{host_tag}.tar.gz'
            c = Connection(host, user=self.settings.ssh_user, connect_kwargs=self.connect)
            result = c.run(
                f'test -d batch_logs/{batch_id} && '
                f'tar -czf {remote_archive} -C batch_logs {batch_id}',
                hide=True,
                warn=True,
            )
            if not result.ok:
                Print.warn(
                    f'No batch logs found on host {host} for batch_id={batch_id}'
                )
                continue

            c.get(remote_archive, local=str(local_archive))
            c.run(f'rm -f {remote_archive}', hide=True, warn=True)

    def run(self, bench_parameters_dict, node_parameters_dict, debug=False):
        assert isinstance(debug, bool)
        Print.heading('Starting remote benchmark')
        try:
            bench_parameters = BenchParameters(bench_parameters_dict)
            node_parameters = NodeParameters(node_parameters_dict)
        except ConfigError as e:
            raise BenchError('Invalid nodes or bench parameters', e)

        # Select which hosts to use.
        selected_hosts = self._select_hosts(bench_parameters)
        if not selected_hosts:
            Print.warn('There are not enough instances available')
            return

        # Update nodes.
        try:
            self._update(selected_hosts, bench_parameters.collocate)
        except (GroupException, ExecutionError) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to update nodes', e)

        # Upload all configuration files.
        try:
            committee = self._config(
                selected_hosts, node_parameters, bench_parameters
            )
        except (subprocess.SubprocessError, GroupException) as e:
            e = FabricError(e) if isinstance(e, GroupException) else e
            raise BenchError('Failed to configure nodes', e)

        # Remove archived logs from previous invocations. Checkpoint directory
        # names are deterministic (r<rate>-run<n>), so stale logs from a faulty
        # node can otherwise be downloaded and parsed as part of the new run.
        try:
            flat_hosts = []
            for h in selected_hosts:
                if isinstance(h, list):
                    flat_hosts.extend(h)
                else:
                    flat_hosts.append(h)
            if flat_hosts:
                protocol = node_parameters.json['dag_protocol']
                Group(*flat_hosts, user=self.settings.ssh_user, connect_kwargs=self.connect).run(
                    f'rm -rf batch_logs/{protocol}',
                    hide=True,
                    warn=True,
                )
        except GroupException as e:
            raise BenchError('Failed to clean archived benchmark logs', FabricError(e))

        # Run benchmarks: execute each rate then checkpoint logs on remote.
        for n in bench_parameters.nodes:
            committee_copy = deepcopy(committee)
            committee_copy.remove_nodes(committee.size() - n)

            for r in bench_parameters.rate:
                Print.heading(f'\nRunning {n} nodes (input rate: {r:,} tx/s)')

                for i in range(bench_parameters.runs):
                    Print.heading(f'Run {i+1}/{bench_parameters.runs}')
                    try:
                        Path(
                            PathMaker.run_result_file(
                                bench_parameters.faults,
                                n,
                                bench_parameters.workers,
                                bench_parameters.collocate,
                                r,
                                bench_parameters.tx_size,
                                i + 1,
                                node_parameters.json['dag_protocol'],
                            )
                        ).unlink(missing_ok=True)
                        self._run_single(
                            r, committee_copy, bench_parameters, debug,
                            clean_logs=True,
                        )

                        checkpoint_id = f'r{r}-run{i+1}'
                        self._checkpoint_logs(
                            self._active_hosts(committee_copy, bench_parameters.faults),
                            node_parameters.json['dag_protocol'],
                            checkpoint_id,
                        )
                    except (subprocess.SubprocessError, GroupException, ParseError, ExecutionError) as e:
                        self.kill(hosts=selected_hosts)
                        if isinstance(e, GroupException):
                            e = FabricError(e)
                        Print.error(BenchError('Benchmark failed', e))
                        continue

        # Batch-download all checkpointed logs in parallel.
        self._batch_download(
            selected_hosts,
            node_parameters.json['dag_protocol'],
            bench_parameters.rate,
        )

        # Parse each rate's logs and write result files.
        faults = bench_parameters.faults
        protocol = node_parameters.json['dag_protocol']
        for n in bench_parameters.nodes:
            for r in bench_parameters.rate:
                rate_logs_dir = join(PathMaker.logs_path(), f'rate-{r}')
                if not Path(rate_logs_dir).exists():
                    Print.warn(f'No logs found for rate={r}')
                    continue
                run_dirs = sorted(
                    x for x in Path(rate_logs_dir).iterdir()
                    if x.is_dir()
                )
                if not run_dirs:
                    run_dirs = [Path(rate_logs_dir)]
                for run_dir in run_dirs:
                    self._parse_run_logs(
                        run_dir, faults, n, bench_parameters, r, protocol
                    )
