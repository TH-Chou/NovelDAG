# Fix fabfile.py: 移除中间下载, 新增 run_all + download_csv_logs tasks
# Fix settings.py: 密钥路径

import re

# ============================================================
# 1. Fix fabfile.py
# ============================================================
with open('benchmark/scripts/fabfile.py') as f:
    c = f.read()

# 1a. 在 sweep_dag_rates 循环内移除 download_results，收集路径到列表
c = c.replace(
    '        bench.download_results()\n        base_results = bench.result_path()',
    '        # 结果保留云端，延迟到全部跑完再批量下载\n        result_paths.append(bench.result_path())'
)

# 1b. 在 sweep_dag_rates 开头初始化 result_paths 列表
c = c.replace(
    '    try:\n        results = {p: [] for p in protocol_list}\n',
    '    try:\n        results = {p: [] for p in protocol_list}\n        result_paths = []  # 云端结果路径，批下载使用\n'
)

# 1c. 在 sweep_dag_rates 末尾的 for rate 循环后添加批量下载和打印提示
old_rate_loop_end = '''            Print.heading(f'DAG rate sweep done. Results saved to {output_csv}')

    except BenchError as e:
        Print.error(e)'''

new_rate_loop_end = '''            Print.heading(f'DAG rate sweep done. Results saved to {output_csv}')
            Print.info(f'All results on cloud: {result_paths}')
            Print.info(f'Run "fab download-csv-logs" to fetch CSV + primary logs')

    except BenchError as e:
        Print.error(e)'''

c = c.replace(old_rate_loop_end, new_rate_loop_end)

# 1d. 新增 download_csv_logs task
download_task = '''
@task
def download_csv_logs(ctx, remote_dir='~/results', local_dir='benchmark/logs/results'):
    ''' Download only CSV files and main logs (primary-*.log, worker-*.log, client-*.log)
        from cloud instances. Run after all benchmarks complete. '''
    from benchmark.remote import RemoteBench
    from benchmark.config import PathMaker
    from fabric import Connection
    import os, json, glob

    Print.heading('Downloading CSV + main logs from cloud...')

    # Reconstruct RemoteBench to get instance info
    node_params = {
        'header_size': 1_000, 'max_header_delay': 200_000, 'gc_depth': 50,
        'sync_retry_delay': 10_000, 'sync_retry_nodes': 3,
        'batch_size': 500_000, 'max_batch_delay': 200,
    }
    bench_params = {
        'nodes': 10, 'rate': 60_000, 'tx_size': 512, 'faults': 0,
        'duration': 30, 'runs': 1,
    }

    os.makedirs(local_dir, exist_ok=True)

    try:
        bench = RemoteBench(bench_params, node_params)
        instances = bench._get_instances()
        if not instances:
            Print.error('No running instances found. Run benchmarks first.')
            return

        for i, inst in enumerate(instances):
            ip = inst.public_ip_address
            Print.info(f'Fetching from node {i} ({ip})...')

            node_local = os.path.join(local_dir, f'node-{i}')
            os.makedirs(node_local, exist_ok=True)

            # Only download CSV and primary/worker/client logs
            patterns = ['*.csv', 'primary-*.log', 'worker-*.log', 'client-*.log']
            for pat in patterns:
                try:
                    bench._scp_from(ip, f'{remote_dir}/{pat}', node_local)
                except Exception:
                    pass  # Skip missing files

        # Aggregate CSVs
        csv_files = []
        for root, _, files in os.walk(local_dir):
            for f in files:
                if f.endswith('.csv'):
                    csv_files.append(os.path.join(root, f))

        if csv_files:
            Print.heading(f'Downloaded {len(csv_files)} CSV files to {local_dir}')
            for cf in sorted(csv_files):
                print(f'  {cf}')
        else:
            Print.warn('No CSV files found.')

    finally:
        if bench:
            bench._terminate()

    Print.heading('Download complete.')
'''

# Insert before the final chart functions
c = re.sub(
    r'(@task\ndef plot_dag_sweep)',
    download_task + r'\n\n\1',
    c
)

# 1e. 新增 run_all task
run_all_task = '''
@task
def run_all(
    ctx,
    duration=30,
    nodes='10,20,50',
    faults='0,1,3',
    workers=1,
    tx_size=512,
    runs=5,
    rates='60000,100000,140000,180000,220000,260000,300000,340000',
    rates_delay_sweep='40000,60000,80000,100000,120000,140000,160000,180000,200000',
    protocols='noveldag',
    consensus='round_robin',
    output_dir='benchmark/logs/results',
    debug=True,
):
    ''' Batch run all benchmark configurations. Results stay on cloud; download at end.

        Example:
          fab run-all                          # full matrix
          fab run-all:nodes=10,faults=0    # quick subset
          fab run-all:nodes=10,runs=3      # 3 runs per point (fast test)
    '''
    import itertools

    node_list = [int(x) for x in nodes.split(',')]
    fault_list = [int(x) for x in faults.split(',')]
    rate_list = [int(x) for x in rates.split(',')]
    rate_delay_list = [int(x) for x in rates_delay_sweep.split(',')]
    proto_list = [p.strip() for p in protocols.split(',')]

    Print.heading(f'=== BATCH RUN ===')
    Print.info(f'Nodes: {node_list}')
    Print.info(f'Faults: {fault_list}')
    Print.info(f'Rates (standard): {rate_list}')
    Print.info(f'Rates (delay sweep): {rate_delay_list}')
    Print.info(f'Protocols: {proto_list}')
    Print.info(f'Runs per point: {runs}')
    Print.info(f'Results stay on cloud, download at end.')

    for n in node_list:
        for f in fault_list:
            if f >= n:
                Print.warn(f'Skipping n={n}, f={f} (faults >= nodes)')
                continue

            Print.heading(f'=== n={n}, f={f} ===')

            # Standard rate sweep (Fig 1+2+3)
            csv_path = f'{output_dir}/sweep_n{n}_f{f}.csv'
            sweep_dag_rates(
                ctx,
                duration=duration,
                debug=debug,
                nodes=n,
                faults=f,
                workers=workers,
                tx_size=tx_size,
                runs=runs,
                rate_start=rate_list[0],
                rate_step=rate_list[1] - rate_list[0] if len(rate_list) > 1 else 30000,
                rate_end=rate_list[-1],
                protocols=','.join(proto_list),
                consensus=consensus,
                output_csv=csv_path,
            )

            # Delay sweep (Fig 4) — only for n=10
            if n == 10:
                csv_delay_path = f'{output_dir}/delay_sweep_n{n}_f{f}.csv'
                sweep_dag_rates(
                    ctx,
                    duration=duration,
                    debug=debug,
                    nodes=n,
                    faults=f,
                    workers=workers,
                    tx_size=tx_size,
                    runs=runs,
                    rate_start=rate_delay_list[0],
                    rate_step=rate_delay_list[1] - rate_delay_list[0] if len(rate_delay_list) > 1 else 20000,
                    rate_end=rate_delay_list[-1],
                    protocols=','.join(proto_list),
                    consensus=consensus,
                    output_csv=csv_delay_path,
                )

    Print.heading('=== ALL BENCHMARKS COMPLETE ===')
    Print.info(f'Run "fab download-csv-logs" to fetch results.')
'''

c = re.sub(
    r'(@task\ndef download_csv_logs)',
    run_all_task + r'\n\n\1',
    c
)

with open('benchmark/scripts/fabfile.py', 'w') as f:
    f.write(c)

print('fabfile.py: DONE')

# ============================================================
# 2. Fix settings.py
# ============================================================
with open('benchmark/benchmark/settings.py') as f:
    s = f.read()

s = s.replace(
    "key_path = Path.home() / '.ssh' / 'aws'",
    "key_path = Path.home() / '.ssh' / 'gcp_dag_rsa'"
)

with open('benchmark/benchmark/settings.py', 'w') as f:
    f.write(s)

print('settings.py: DONE')
