# Copyright(C) Facebook, Inc. and its affiliates.
import csv
import os
import sys
from pathlib import Path
# Ensure benchmark package is importable regardless of CWD
_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT))
os.chdir(_ROOT)
from collections import defaultdict
from fabric import task

from benchmark.local import LocalBench
from benchmark.logs import ParseError, LogParser
from benchmark.utils import Print, BenchError, PathMaker


@task
def local(ctx, debug=True, protocol='round_robin', dag_protocol='noveldag', rate=50_000):
    ''' Run benchmarks on localhost '''
    if protocol not in ('round_robin', 'common_coin'):
        raise BenchError('Invalid protocol: must be round_robin or common_coin')

    bench_params = {
        'faults': 0,
        'nodes': 4,
        'workers': 1,
        'rate': int(rate),
        'tx_size': 512,
        'duration': 20,
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
        'consensus_protocol': protocol,
        'dag_protocol': dag_protocol,
    }
    try:
        ret = LocalBench(bench_params, node_params).run(debug)
        print(ret.result())
    except BenchError as e:
        Print.error(e)


@task
def compare_consensus(ctx, duration=60, debug=True):
    ''' Compare round_robin vs common_coin on localhost '''
    bench_params = {
        'faults': 0,
        'nodes': 4,
        'workers': 1,
        'rate': 50_000,
        'tx_size': 512,
        'duration': int(duration),
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
    }

    try:
        protocols = ['round_robin', 'common_coin']
        results = {}
        for protocol in protocols:
            Print.heading(
                f'Running local benchmark with protocol={protocol} '
                f'for {bench_params["duration"]}s'
            )
            current = dict(node_params)
            current['consensus_protocol'] = protocol
            parser = LocalBench(bench_params, current).run(debug)
            print(parser.result())
            results[protocol] = parser.metrics()

        Print.heading('Consensus protocol comparison')
        print('-----------------------------------------')
        for protocol in protocols:
            metrics = results[protocol]
            print(
                f'{protocol}: '
                f'consensus_tps={round(metrics["consensus_tps"]):,} tx/s, '
                f'consensus_latency={round(metrics["consensus_latency_ms"]):,} ms, '
                f'end_to_end_tps={round(metrics["end_to_end_tps"]):,} tx/s, '
                f'end_to_end_latency={round(metrics["end_to_end_latency_ms"]):,} ms'
            )
        print('-----------------------------------------')

    except BenchError as e:
        Print.error(e)


@task
def compare_consensus_groups(
    ctx,
    duration=60,
    debug=True,
    rate=120_000,
    rounds=10,
    output_csv='csv_plots/consensus_fault_comparison_avg.csv',
    output_runs_csv='csv_plots/consensus_fault_comparison_runs.csv',
):
    ''' Compare protocols on grouped faults with 16 nodes (faults=0,1,2,3,4,5), averaged over rounds '''
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
    }

    protocols = ['round_robin', 'common_coin']
    faults_group = [0, 1, 2, 3, 4, 5]
    results = {protocol: {} for protocol in protocols}
    runs_rows = []
    rounds = int(rounds)
    if rounds <= 0:
        raise BenchError('rounds must be greater than 0')

    try:
        for fault in faults_group:
            bench_params = {
                'faults': fault,
                'nodes': 16,
                'workers': 1,
                'rate': int(rate),
                'tx_size': 512,
                'duration': int(duration),
            }
            for protocol in protocols:
                metrics_runs = []
                for run in range(rounds):
                    Print.heading(
                        f'Run {run + 1}/{rounds} | nodes=16 faults={fault} '
                        f'protocol={protocol} duration={bench_params["duration"]}s'
                    )
                    current = dict(node_params)
                    current['consensus_protocol'] = protocol
                    parser = LocalBench(bench_params, current).run(debug)
                    print(parser.result())
                    metrics = parser.metrics()
                    metrics_runs.append(metrics)
                    runs_rows.append(
                        [
                            run + 1,
                            fault,
                            protocol,
                            f'{metrics["consensus_tps"]:.6f}',
                            f'{metrics["consensus_latency_ms"]:.6f}',
                            f'{metrics["end_to_end_tps"]:.6f}',
                            f'{metrics["end_to_end_latency_ms"]:.6f}',
                        ]
                    )

                results[protocol][fault] = {
                    'consensus_protocol': protocol,
                    'consensus_tps': sum(
                        x['consensus_tps'] for x in metrics_runs
                    ) / rounds,
                    'consensus_latency_ms': sum(
                        x['consensus_latency_ms'] for x in metrics_runs
                    ) / rounds,
                    'end_to_end_tps': sum(
                        x['end_to_end_tps'] for x in metrics_runs
                    ) / rounds,
                    'end_to_end_latency_ms': sum(
                        x['end_to_end_latency_ms'] for x in metrics_runs
                    ) / rounds,
                }

        Print.heading(
            f'Grouped consensus comparison (nodes=16, averaged over {rounds} runs)'
        )
        print('------------------------------------------------------------')
        for fault in faults_group:
            for protocol in protocols:
                metrics = results[protocol][fault]
                print(
                    f'faults={fault}, protocol={protocol}: '
                    f'consensus_tps={round(metrics["consensus_tps"]):,} tx/s, '
                    f'consensus_latency={round(metrics["consensus_latency_ms"]):,} ms, '
                    f'end_to_end_tps={round(metrics["end_to_end_tps"]):,} tx/s, '
                    f'end_to_end_latency={round(metrics["end_to_end_latency_ms"]):,} ms'
                )
        print('------------------------------------------------------------')

        with open(output_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    'fault',
                    'protocol',
                    'consensus_tps',
                    'consensus_latency_ms',
                    'end_to_end_tps',
                    'end_to_end_latency_ms',
                    'rounds',
                ]
            )
            for fault in faults_group:
                for protocol in protocols:
                    metrics = results[protocol][fault]
                    writer.writerow(
                        [
                            fault,
                            protocol,
                            f'{metrics["consensus_tps"]:.6f}',
                            f'{metrics["consensus_latency_ms"]:.6f}',
                            f'{metrics["end_to_end_tps"]:.6f}',
                            f'{metrics["end_to_end_latency_ms"]:.6f}',
                            rounds,
                        ]
                    )
        print(f'Averaged metrics exported to {output_csv}')

        with open(output_runs_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    'run',
                    'fault',
                    'protocol',
                    'consensus_tps',
                    'consensus_latency_ms',
                    'end_to_end_tps',
                    'end_to_end_latency_ms',
                ]
            )
            writer.writerows(runs_rows)
        print(f'Per-run metrics exported to {output_runs_csv}')

    except BenchError as e:
        Print.error(e)


@task
def compare_consensus_rates_zero_fault(
    ctx,
    duration=30,
    debug=True,
    rounds=2,
    nodes=16,
    rate_start=30_000,
    rate_step=30_000,
    rate_end=240_000,
    output_csv='csv_plots/consensus_rate_comparison_avg.csv',
    output_runs_csv='csv_plots/consensus_rate_comparison_runs.csv',
):
    ''' Compare consensus TPS vs injection rate at faults=0 for round_robin and common_coin '''
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
    }

    rounds = int(rounds)
    nodes = int(nodes)
    rate_start = int(rate_start)
    rate_step = int(rate_step)
    rate_end = int(rate_end)
    if rounds <= 0:
        raise BenchError('rounds must be greater than 0')
    if nodes <= 0:
        raise BenchError('nodes must be greater than 0')
    if rate_start < 0 or rate_step <= 0 or rate_end < rate_start:
        raise BenchError('rate range is invalid')

    protocols = ['round_robin', 'common_coin']
    rates = list(range(rate_start, rate_end + 1, rate_step))
    results = {protocol: {} for protocol in protocols}
    runs_rows = []

    try:
        for rate in rates:
            bench_params = {
                'faults': 0,
                'nodes': nodes,
                'workers': 1,
                'rate': rate,
                'tx_size': 512,
                'duration': int(duration),
            }

            for protocol in protocols:
                metrics_runs = []
                for run in range(rounds):
                    Print.heading(
                        f'Run {run + 1}/{rounds} | nodes={nodes} faults=0 '
                        f'protocol={protocol} rate={rate} duration={bench_params["duration"]}s'
                    )
                    current = dict(node_params)
                    current['consensus_protocol'] = protocol
                    parser = LocalBench(bench_params, current).run(debug)
                    print(parser.result())

                    metrics = parser.metrics()
                    metrics_runs.append(metrics)
                    runs_rows.append(
                        [
                            run + 1,
                            rate,
                            protocol,
                            f'{metrics["consensus_tps"]:.6f}',
                            f'{metrics["consensus_latency_ms"]:.6f}',
                            f'{metrics["end_to_end_tps"]:.6f}',
                            f'{metrics["end_to_end_latency_ms"]:.6f}',
                        ]
                    )

                results[protocol][rate] = {
                    'consensus_protocol': protocol,
                    'consensus_tps': sum(
                        x['consensus_tps'] for x in metrics_runs
                    ) / rounds,
                    'consensus_latency_ms': sum(
                        x['consensus_latency_ms'] for x in metrics_runs
                    ) / rounds,
                    'end_to_end_tps': sum(
                        x['end_to_end_tps'] for x in metrics_runs
                    ) / rounds,
                    'end_to_end_latency_ms': sum(
                        x['end_to_end_latency_ms'] for x in metrics_runs
                    ) / rounds,
                }

        Print.heading(
            f'Consensus rate sweep at faults=0 (nodes={nodes}, averaged over {rounds} runs)'
        )
        print('------------------------------------------------------------')
        for rate in rates:
            for protocol in protocols:
                metrics = results[protocol][rate]
                print(
                    f'rate={rate:,}, protocol={protocol}: '
                    f'consensus_tps={round(metrics["consensus_tps"]):,} tx/s, '
                    f'consensus_latency={round(metrics["consensus_latency_ms"]):,} ms, '
                    f'end_to_end_tps={round(metrics["end_to_end_tps"]):,} tx/s, '
                    f'end_to_end_latency={round(metrics["end_to_end_latency_ms"]):,} ms'
                )
        print('------------------------------------------------------------')

        with open(output_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    'rate',
                    'protocol',
                    'consensus_tps',
                    'consensus_latency_ms',
                    'end_to_end_tps',
                    'end_to_end_latency_ms',
                    'rounds',
                ]
            )
            for rate in rates:
                for protocol in protocols:
                    metrics = results[protocol][rate]
                    writer.writerow(
                        [
                            rate,
                            protocol,
                            f'{metrics["consensus_tps"]:.6f}',
                            f'{metrics["consensus_latency_ms"]:.6f}',
                            f'{metrics["end_to_end_tps"]:.6f}',
                            f'{metrics["end_to_end_latency_ms"]:.6f}',
                            rounds,
                        ]
                    )
        print(f'Averaged metrics exported to {output_csv}')

        with open(output_runs_csv, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(
                [
                    'run',
                    'rate',
                    'protocol',
                    'consensus_tps',
                    'consensus_latency_ms',
                    'end_to_end_tps',
                    'end_to_end_latency_ms',
                ]
            )
            writer.writerows(runs_rows)
        print(f'Per-run metrics exported to {output_runs_csv}')

    except BenchError as e:
        Print.error(e)


@task
def plot_consensus_rates_zero_fault(
    ctx,
    csv_path='csv_plots/consensus_rate_comparison_avg.csv',
    out_png='csv_plots/consensus_tps_vs_rate_zero_fault_with_table.png',
    out_svg='csv_plots/consensus_tps_vs_rate_zero_fault_with_table.svg',
    out_latency_png='csv_plots/consensus_latency_vs_rate_zero_fault_with_table.png',
    out_latency_svg='csv_plots/consensus_latency_vs_rate_zero_fault_with_table.svg',
):
    ''' Plot consensus TPS and latency vs rate at faults=0 with summary tables above charts '''
    import matplotlib.pyplot as plt

    rows = []
    with open(csv_path, newline='') as f:
        for row in csv.DictReader(f):
            rows.append(
                {
                    'rate': int(row['rate']),
                    'protocol': row['protocol'],
                    'consensus_tps': float(row['consensus_tps']),
                    'consensus_latency_ms': float(row['consensus_latency_ms']),
                }
            )

    rates = sorted({x['rate'] for x in rows})
    protocols = ['round_robin', 'common_coin']
    colors = {'round_robin': '#1f77b4', 'common_coin': '#ff7f0e'}

    by_protocol_rate = {
        protocol: {x['rate']: x for x in rows if x['protocol'] == protocol}
        for protocol in protocols
    }

    def _plot_with_table(metric_key, title, y_label, col_labels, out_file_png, out_file_svg):
        fig = plt.figure(figsize=(12, 8))
        gs = fig.add_gridspec(2, 1, height_ratios=[1.15, 3.0])
        ax_table = fig.add_subplot(gs[0])
        ax_plot = fig.add_subplot(gs[1])

        for protocol in protocols:
            ys = [by_protocol_rate[protocol][rate][metric_key] for rate in rates]
            ax_plot.plot(
                rates,
                ys,
                marker='o',
                linewidth=2.5,
                markersize=6,
                label=protocol,
                color=colors[protocol],
            )

        ax_plot.set_title(title)
        ax_plot.set_xlabel('Injection Rate (tx/s)')
        ax_plot.set_ylabel(y_label)
        ax_plot.set_xticks(rates)
        ax_plot.grid(axis='y', alpha=0.25)
        ax_plot.legend()

        ax_table.axis('off')
        table_rows = [
            [
                f'{rate:,}',
                f'{by_protocol_rate["round_robin"][rate][metric_key]:,.0f}',
                f'{by_protocol_rate["common_coin"][rate][metric_key]:,.0f}',
            ]
            for rate in rates
        ]
        table = ax_table.table(
            cellText=table_rows,
            colLabels=col_labels,
            cellLoc='center',
            loc='center',
        )
        table.auto_set_font_size(False)
        table.set_fontsize(10)
        table.scale(1.0, 1.2)

        fig.tight_layout()
        fig.savefig(out_file_png, dpi=180)
        fig.savefig(out_file_svg)
        plt.close(fig)
        print(f'Plot exported to {out_file_png} and {out_file_svg}')

    _plot_with_table(
        'consensus_tps',
        'Consensus TPS vs Injection Rate (faults=0)',
        'Consensus TPS (tx/s)',
        ['Rate (tx/s)', 'round_robin TPS', 'common_coin TPS'],
        out_png,
        out_svg,
    )

    _plot_with_table(
        'consensus_latency_ms',
        'Consensus Latency vs Injection Rate (faults=0)',
        'Consensus Latency (ms)',
        ['Rate (tx/s)', 'round_robin Latency', 'common_coin Latency'],
        out_latency_png,
        out_latency_svg,
    )


@task
def create(ctx, nodes=2):
    ''' Create a testbed'''
    from benchmark.instance import InstanceManager

    try:
        InstanceManager.make().create_instances(nodes)
    except BenchError as e:
        Print.error(e)


@task
def destroy(ctx):
    ''' Destroy the testbed '''
    from benchmark.instance import InstanceManager

    try:
        InstanceManager.make().terminate_instances()
    except BenchError as e:
        Print.error(e)


@task
def start(ctx, max=2):
    ''' Start at most `max` machines per data center '''
    from benchmark.instance import InstanceManager

    try:
        InstanceManager.make().start_instances(max)
    except BenchError as e:
        Print.error(e)


@task
def stop(ctx):
    ''' Stop all machines '''
    from benchmark.instance import InstanceManager

    try:
        InstanceManager.make().stop_instances()
    except BenchError as e:
        Print.error(e)


@task
def info(ctx):
    ''' Display connect information about all the available machines '''
    from benchmark.instance import InstanceManager

    try:
        InstanceManager.make().print_info()
    except BenchError as e:
        Print.error(e)


@task
def install(ctx):
    ''' Install the codebase on all machines '''
    from benchmark.remote import Bench

    try:
        Bench(ctx).install()
    except BenchError as e:
        Print.error(e)


@task
def remote(
    ctx,
    debug=False,
    protocol='round_robin',
    dag_protocol='noveldag',
    faults=3,
    nodes=10,
    workers=1,
    rate=10_000,
    tx_size=512,
    duration=300,
    runs=2,
):
    ''' Run benchmarks on AWS '''
    from benchmark.remote import Bench

    if protocol not in ('round_robin', 'common_coin'):
        raise BenchError('Invalid protocol: must be round_robin or common_coin')

    bench_params = {
        'faults': int(faults),
        'nodes': [int(nodes)],
        'workers': int(workers),
        'collocate': True,
        'rate': [int(rate)],
        'tx_size': int(tx_size),
        'duration': int(duration),
        'runs': int(runs),
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
        'consensus_protocol': protocol,
        'dag_protocol': dag_protocol,
    }
    try:
        Bench(ctx).run(bench_params, node_params, debug)
    except BenchError as e:
        Print.error(e)


@task
def remote_run_batch(
    ctx,
    debug=False,
    protocol='round_robin',
    dag_protocol='noveldag',
    batch_id='default',
    faults=3,
    nodes=10,
    workers=1,
    rates='10000',
    tx_size=512,
    duration=300,
    runs=2,
):
    ''' Run benchmarks on AWS and keep logs on remote machines for later collection '''
    from benchmark.remote import Bench

    if protocol not in ('round_robin', 'common_coin'):
        raise BenchError('Invalid protocol: must be round_robin or common_coin')

    rate_values = [int(x.strip()) for x in str(rates).split(',') if x.strip()]
    if not rate_values:
        raise BenchError('Invalid rates: provide comma-separated integers')

    bench_params = {
        'faults': int(faults),
        'nodes': [int(nodes)],
        'workers': int(workers),
        'collocate': True,
        'rate': rate_values,
        'tx_size': int(tx_size),
        'duration': int(duration),
        'runs': int(runs),
    }
    node_params = {
        'header_size': 1_000,  # bytes
        'max_header_delay': 200,  # ms
        'gc_depth': 50,  # rounds
        'sync_retry_delay': 10_000,  # ms
        'sync_retry_nodes': 3,  # number of nodes
        'batch_size': 500_000,  # bytes
        'max_batch_delay': 200,  # ms
        'consensus_protocol': protocol,
        'dag_protocol': dag_protocol,
    }
    try:
        Bench(ctx).run_batch(bench_params, node_params, str(batch_id), debug)
    except BenchError as e:
        Print.error(e)


@task
def remote_collect_batch(ctx, batch_id='default', out_dir='batch_downloads'):
    ''' Download archived batch logs from all AWS machines in one shot '''
    from benchmark.remote import Bench

    try:
        Bench(ctx).collect_batch(str(batch_id), str(out_dir))
    except BenchError as e:
        Print.error(e)


@task
def plot(ctx):
    ''' Plot performance using the logs generated by "fab remote" '''
    from benchmark.plot import Ploter, PlotError

    plot_params = {
        'faults': [0],
        'nodes': [10, 20, 50],
        'workers': [1],
        'collocate': True,
        'tx_size': 512,
        'max_latency': [3_500, 4_500]
    }
    try:
        Ploter.plot(plot_params)
    except PlotError as e:
        Print.error(BenchError('Failed to plot performance', e))


@task
def kill(ctx):
    ''' Stop execution on all machines '''
    from benchmark.remote import Bench

    try:
        Bench(ctx).kill()
    except BenchError as e:
        Print.error(e)


@task
def logs(ctx):
    ''' Print a summary of the logs '''
    try:
        print(LogParser.process('./logs', faults='?').result())
    except ParseError as e:
        Print.error(BenchError('Failed to parse logs', e))


@task
def compare_dag_protocols(
    ctx,
    duration=60,
    debug=True,
    faults=0,
    nodes=10,
    workers=1,
    rate=120_000,
    tx_size=512,
    runs=2,
    consensus='round_robin',
    output_csv='csv_plots/dag_protocol_comparison.csv',
):
    ''' Compare narwhal, bullshark, noveldag on remote testbed at fixed rate '''
    from benchmark.remote import Bench

    protocols = ['narwhal', 'bullshark', 'noveldag']
    bench_params = {
        'faults': int(faults),
        'nodes': [int(nodes)],
        'workers': int(workers),
        'collocate': True,
        'rate': [int(rate)],
        'tx_size': int(tx_size),
        'duration': int(duration),
        'runs': int(runs),
    }

    rows = []
    try:
        for proto in protocols:
            Print.heading(
                f'Running {proto} | nodes={nodes} faults={faults} '
                f'rate={rate:,} duration={duration}s'
            )
            node_params = {
                'header_size': 1_000,
                'max_header_delay': 200,
                'gc_depth': 50,
                'sync_retry_delay': 10_000,
                'sync_retry_nodes': 3,
                'batch_size': 500_000,
                'max_batch_delay': 200,
                'consensus_protocol': consensus,
                'dag_protocol': proto,
            }
            bench = Bench(ctx)
            bench.run(bench_params, node_params, debug)

            # Parse result files produced by Bench.run()
            for run_i in range(1, int(runs) + 1):
                result_path = PathMaker.run_result_file(
                    int(faults), int(nodes), int(workers), True,
                    int(rate), int(tx_size), run_i, proto
                )
                try:
                    with open(result_path, 'r') as f:
                        text = f.read()
                    # Parse the LogParser result format
                    metrics = _parse_result_text(text)
                    rows.append({
                        'rate': rate,
                        'protocol': proto,
                        'run': run_i,
                        'consensus_tps': f'{metrics["consensus_tps"]:.2f}',
                        'consensus_latency_ms': f'{metrics["consensus_latency_ms"]:.2f}',
                        'end_to_end_tps': f'{metrics["end_to_end_tps"]:.2f}',
                        'end_to_end_latency_ms': f'{metrics["end_to_end_latency_ms"]:.2f}',
                    })
                except FileNotFoundError:
                    Print.warn(f'Result file not found: {result_path}')

        # Save CSV
        _write_metrics_csv(output_csv, rows)
        _print_dag_summary(rows, protocols)

    except BenchError as e:
        Print.error(e)


@task
def sweep_dag_rates(
    ctx,
    duration=30,
    debug=True,
    nodes=10,
    faults=3,
    workers=1,
    tx_size=512,
    runs=2,
    rate_start=60_000,
    rate_step=30_000,
    rate_end=300_000,
    protocols='narwhal,bullshark,noveldag',
    consensus='round_robin',
    output_csv='csv_plots/remote_dag_sweep.csv',
):
    ''' Rate sweep across all three DAG protocols on remote testbed '''
    from benchmark.remote import Bench

    protocol_list = [p.strip() for p in protocols.split(',')]
    rates = list(range(int(rate_start), int(rate_end) + 1, int(rate_step)))

    bench_params = {
        'faults': int(faults),
        'nodes': [int(nodes)],
        'workers': int(workers),
        'collocate': True,
        'rate': rates,
        'tx_size': int(tx_size),
        'duration': int(duration),
        'runs': int(runs),
    }

    all_rows = []
    total = len(protocol_list) * len(rates) * int(runs)
    current = 0

    try:
        for proto in protocol_list:
            Print.heading(
                f'Sweeping {proto} | nodes={nodes} faults={faults} '
                f'rates={rates[0]:,}..{rates[-1]:,} runs={runs}'
            )
            node_params = {
                'header_size': 1_000,
                'max_header_delay': 200,
                'gc_depth': 50,
                'sync_retry_delay': 10_000,
                'sync_retry_nodes': 3,
                'batch_size': 500_000,
                'max_batch_delay': 200,
                'consensus_protocol': consensus,
                'dag_protocol': proto,
            }
            bench = Bench(ctx)
            bench.run(bench_params, node_params, debug)

            # Collect results from individual result files
            for r in rates:
                for run_i in range(1, int(runs) + 1):
                    current += 1
                    result_path = PathMaker.run_result_file(
                        int(faults), int(nodes), int(workers), True,
                        r, int(tx_size), run_i, proto
                    )
                    try:
                        with open(result_path, 'r') as f:
                            text = f.read()
                        metrics = _parse_result_text(text)
                        all_rows.append({
                            'rate': r,
                            'protocol': proto,
                            'consensus_tps': f'{metrics["consensus_tps"]:.2f}',
                            'consensus_latency_ms': f'{metrics["consensus_latency_ms"]:.2f}',
                            'end_to_end_tps': f'{metrics["end_to_end_tps"]:.2f}',
                            'end_to_end_latency_ms': f'{metrics["end_to_end_latency_ms"]:.2f}',
                        })
                        Print.info(
                            f'  [{current}/{total}] rate={r:,} proto={proto} '
                            f'lat={metrics["consensus_latency_ms"]:.1f}ms '
                            f'tps={metrics["end_to_end_tps"]:,.0f}'
                        )
                    except FileNotFoundError:
                        Print.warn(f'  [{current}/{total}] Missing: {result_path}')

        # Save CSV
        _write_metrics_csv(output_csv, all_rows)
        _print_dag_summary(all_rows, protocol_list)

    except BenchError as e:
        Print.error(e)


@task
def plot_dag_sweep(
    ctx,
    csv_path='csv_plots/remote_dag_sweep.csv',
    out_dir='results',
):
    ''' Generate comparison charts from a dag sweep CSV '''
    import matplotlib.pyplot as plt

    protocols = []
    raw = {}
    try:
        with open(csv_path, newline='') as f:
            for row in csv.DictReader(f):
                proto = row['protocol']
                rate = int(float(row['rate']))
                tps = float(row['end_to_end_tps'])
                lat = float(row['consensus_latency_ms'])
                raw.setdefault(proto, []).append((rate, tps, lat))
    except FileNotFoundError:
        Print.error(BenchError(f'CSV not found: {csv_path}', FileNotFoundError()))
        return

    protocols = list(raw.keys())
    # Average per protocol per rate, sorted by rate
    data = {}
    for proto in protocols:
        by_rate = defaultdict(list)
        for rate, tps, lat in raw[proto]:
            by_rate[rate].append((tps, lat))
        rates = sorted(by_rate.keys())
        data[proto] = {
            'rates': rates,
            'tps': [sum(t for t, _ in by_rate[r]) / len(by_rate[r]) for r in rates],
            'lats': [sum(l for _, l in by_rate[r]) / len(by_rate[r]) for r in rates],
        }

    markers = {'narwhal': 's', 'bullshark': '^', 'noveldag': 'o'}
    colors = {'narwhal': '#2196F3', 'bullshark': '#4CAF50', 'noveldag': '#FF9800'}

    # Chart 1: Consensus Latency vs End-to-End Throughput
    fig, ax = plt.subplots(figsize=(12, 7))
    for proto in protocols:
        d = data[proto]
        ax.plot(d['tps'], d['lats'], color=colors.get(proto), marker=markers.get(proto),
                markersize=7, linewidth=1.8, label=proto.capitalize(), zorder=3)
        for r, t, l in zip(d['rates'], d['tps'], d['lats']):
            ax.annotate(f'{r//1000}k', (t, l), textcoords='offset points',
                        xytext=(5, 5), fontsize=6, alpha=0.7)
    ax.set_xlabel('End-to-End Throughput (tx/s)')
    ax.set_ylabel('Consensus Latency (ms)')
    ax.set_title('Consensus Latency vs Achieved Throughput')
    ax.set_ylim(bottom=0)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    latency_png = f'{out_dir}/remote_latency_vs_tps.png'
    fig.savefig(latency_png, dpi=120)
    plt.close(fig)
    print(f'Saved: {latency_png}')

    # Chart 2: TPS vs Input Rate
    fig, ax = plt.subplots(figsize=(12, 7))
    max_rate = 0
    for proto in protocols:
        d = data[proto]
        ax.plot(d['rates'], d['tps'], color=colors.get(proto), marker=markers.get(proto),
                markersize=7, linewidth=1.8, label=proto.capitalize(), zorder=3)
        max_rate = max(max_rate, max(d['rates']))
    ax.plot([0, max_rate], [0, max_rate], 'k--', alpha=0.3, linewidth=1, label='Ideal')
    ax.set_xlabel('Input Rate (tx/s)')
    ax.set_ylabel('End-to-End Throughput (tx/s)')
    ax.set_title('Throughput vs Input Rate')
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    tps_png = f'{out_dir}/remote_tps_vs_rate.png'
    fig.savefig(tps_png, dpi=120)
    plt.close(fig)
    print(f'Saved: {tps_png}')

    Print.heading('Charts generated')


@task
def full_dag_bench(
    ctx,
    duration=30,
    nodes=10,
    faults=3,
    workers=1,
    tx_size=512,
    runs=2,
    rate_start=60_000,
    rate_step=30_000,
    rate_end=300_000,
    protocols='narwhal,bullshark,noveldag',
    consensus='round_robin',
    output_csv='csv_plots/remote_dag_sweep.csv',
    debug=True,
):
    ''' All-in-one: sweep all three DAG protocols on cloud, then generate charts '''
    Print.heading('=== Phase 1/2: Rate Sweep ===')
    sweep_dag_rates(
        ctx,
        duration=duration,
        debug=debug,
        nodes=nodes,
        faults=faults,
        workers=workers,
        tx_size=tx_size,
        runs=runs,
        rate_start=rate_start,
        rate_step=rate_step,
        rate_end=rate_end,
        protocols=protocols,
        consensus=consensus,
        output_csv=output_csv,
    )

    Print.heading('=== Phase 2/2: Generate Charts ===')
    plot_dag_sweep(ctx, csv_path=output_csv, out_dir='results')


def _parse_result_text(text):
    ''' Parse a LogParser result text into a metrics dict. '''
    import re
    metrics = {
        'consensus_tps': 0.0,
        'consensus_latency_ms': 0.0,
        'end_to_end_tps': 0.0,
        'end_to_end_latency_ms': 0.0,
    }
    for line in text.split('\n'):
        m = re.match(r'\s*Consensus TPS:\s*([\d,]+(?:\.\d+)?)', line)
        if m:
            metrics['consensus_tps'] = float(m.group(1).replace(',', ''))
        m = re.match(r'\s*Consensus latency:\s*([\d,]+(?:\.\d+)?)\s*ms', line)
        if m:
            metrics['consensus_latency_ms'] = float(m.group(1).replace(',', ''))
        m = re.match(r'\s*End-to-end TPS:\s*([\d,]+(?:\.\d+)?)', line)
        if m:
            metrics['end_to_end_tps'] = float(m.group(1).replace(',', ''))
        m = re.match(r'\s*End-to-end latency:\s*([\d,]+(?:\.\d+)?)\s*ms', line)
        if m:
            metrics['end_to_end_latency_ms'] = float(m.group(1).replace(',', ''))
    return metrics


def _write_metrics_csv(path, rows):
    ''' Write benchmark rows to CSV. '''
    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                'rate', 'protocol',
                'consensus_tps', 'consensus_latency_ms',
                'end_to_end_tps', 'end_to_end_latency_ms',
            ],
        )
        writer.writeheader()
        writer.writerows(rows)
    print(f'Saved {len(rows)} rows to {path}')


def _print_dag_summary(rows, protocols):
    ''' Print consensus latency and TPS summary table. '''
    by_proto_rate = defaultdict(lambda: defaultdict(list))
    for row in rows:
        r = int(float(row['rate']))
        by_proto_rate[row['protocol']][r].append(row)

    rates = sorted({int(float(r['rate'])) for r in rows})

    Print.heading('\n=== Consensus Latency (ms) ===')
    header = f"{'Rate':>10}"
    for p in protocols:
        header += f' {p:>12}'
    print(header)
    print('-' * (10 + 13 * len(protocols)))
    for r in rates:
        line = f'{r:>10,}'
        for p in protocols:
            items = by_proto_rate.get(p, {}).get(r, [])
            if items:
                avg = sum(float(x['consensus_latency_ms']) for x in items) / len(items)
                line += f' {avg:>12.1f}'
            else:
                line += f' {"N/A":>12}'
        print(line)

    Print.heading('\n=== End-to-End TPS ===')
    header = f"{'Rate':>10}"
    for p in protocols:
        header += f' {p:>12}'
    print(header)
    print('-' * (10 + 13 * len(protocols)))
    for r in rates:
        line = f'{r:>10,}'
        for p in protocols:
            items = by_proto_rate.get(p, {}).get(r, [])
            if items:
                avg = sum(float(x['end_to_end_tps']) for x in items) / len(items)
                line += f' {avg:>12.0f}'
            else:
                line += f' {"N/A":>12}'
        print(line)


# ═══════════════════════════════════════════════════════════════
# Paper figure sweep helpers
# ═══════════════════════════════════════════════════════════════

def _run_dag_sweep(
    ctx,
    bench_params,
    node_params,
    protocol_list,
    output_csv,
    debug,
):
    """Run a rate sweep across DAG protocols for a single (nodes, faults)."""
    from benchmark.remote import Bench

    rate_values = bench_params['rate']
    if isinstance(rate_values, (int, float)):
        rate_values = [int(rate_values)]
    else:
        rate_values = [int(x) for x in rate_values]

    all_rows = []
    total = len(protocol_list) * len(rate_values) * bench_params['runs']
    current = 0

    for proto in protocol_list:
        Print.heading(
            f'Sweeping {proto} | nodes={bench_params["nodes"][0]} '
            f'faults={bench_params["faults"]} '
            f'rates={rate_values[0]:,}..{rate_values[-1]:,} '
            f'runs={bench_params["runs"]}'
        )
        current_node = dict(node_params)
        current_node['dag_protocol'] = proto
        bench = Bench(ctx)
        bench.run(bench_params, current_node, debug)

        # Collect results from individual result files
        n_nodes = bench_params['nodes'][0]
        for r in rate_values:
            for run_i in range(1, bench_params['runs'] + 1):
                current += 1
                result_path = PathMaker.run_result_file(
                    bench_params['faults'], n_nodes, bench_params['workers'],
                    bench_params['collocate'],
                    r, bench_params['tx_size'], run_i, proto,
                )
                try:
                    with open(result_path, 'r') as f:
                        text = f.read()
                    metrics = _parse_result_text(text)
                    all_rows.append({
                        'rate': r,
                        'nodes': n_nodes,
                        'faults': bench_params['faults'],
                        'protocol': proto,
                        'consensus_tps': f'{metrics["consensus_tps"]:.2f}',
                        'consensus_latency_ms': f'{metrics["consensus_latency_ms"]:.2f}',
                        'end_to_end_tps': f'{metrics["end_to_end_tps"]:.2f}',
                        'end_to_end_latency_ms': f'{metrics["end_to_end_latency_ms"]:.2f}',
                    })
                    Print.info(
                        f'  [{current}/{total}] rate={r:,} proto={proto} '
                        f'lat={metrics["consensus_latency_ms"]:.1f}ms '
                        f'tps={metrics["end_to_end_tps"]:,.0f}'
                    )
                except FileNotFoundError:
                    Print.warn(f'  [{current}/{total}] Missing: {result_path}')

    # Save CSV with nodes and faults columns
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                'rate', 'nodes', 'faults', 'protocol',
                'consensus_tps', 'consensus_latency_ms',
                'end_to_end_tps', 'end_to_end_latency_ms',
            ],
        )
        writer.writeheader()
        writer.writerows(all_rows)
    print(f'Saved {len(all_rows)} rows to {output_csv}')

    _print_dag_summary(all_rows, protocol_list)


# ═══════════════════════════════════════════════════════════════
# Figure 1 & 2: DAG protocol comparison across node counts
#   - Protocols: NovelDAG, Narwhal (Tusk), Wahoo
#   - Nodes: 10, 20, 50
#   - Faults: 0
#   - 500KB max block size (= batch_size 500_000), 512B tx
# ═══════════════════════════════════════════════════════════════

@task
def paper_fig1_fig2(
    ctx,
    duration=30,
    debug=True,
    nodes='10,20,50',
    faults=0,
    workers=1,
    tx_size=512,
    runs=2,
    rate_start=60_000,
    rate_step=30_000,
    rate_end=300_000,
    protocols='noveldag,narwhal,wahoo',
    consensus='round_robin',
    output_csv='csv_plots/paper_fig1_fig2.csv',
):
    ''' Figure 1 & 2: Rate sweep across node counts (10, 20, 50), faults=0.

    Compares NovelDAG, Narwhal (Tusk), and Wahoo.
    - WAN measurements (AWS remote testbed)
    - 500KB max block size, 512B transaction size
    '''
    node_list = [int(x.strip()) for x in str(nodes).split(',') if x.strip()]
    protocol_list = [p.strip() for p in str(protocols).split(',') if p.strip()]
    rate_list = list(range(int(rate_start), int(rate_end) + 1, int(rate_step)))

    for proto in protocol_list:
        if proto not in ('narwhal', 'bullshark', 'noveldag', 'wahoo'):
            raise BenchError(f'Invalid dag_protocol: {proto}')

    node_params = {
        'header_size': 1_000,
        'max_header_delay': 200,
        'gc_depth': 50,
        'sync_retry_delay': 10_000,
        'sync_retry_nodes': 3,
        'batch_size': 500_000,
        'max_batch_delay': 200,
        'consensus_protocol': consensus,
    }

    # Merge per-node-count CSVs
    all_rows = []
    for n in node_list:
        Print.heading(f'=== nodes={n} faults={faults} ===')
        partial_csv = f'{output_csv}.n{n}.tmp.csv'
        bench_params = {
            'faults': int(faults),
            'nodes': [n],
            'workers': int(workers),
            'collocate': True,
            'rate': rate_list,
            'tx_size': int(tx_size),
            'duration': int(duration),
            'runs': int(runs),
        }
        _run_dag_sweep(ctx, bench_params, node_params, protocol_list,
                        partial_csv, debug)

        try:
            with open(partial_csv, newline='') as f:
                for row in csv.DictReader(f):
                    all_rows.append(row)
        except FileNotFoundError:
            Print.warn(f'Partial CSV not found: {partial_csv}')

    # Write merged CSV
    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                'rate', 'nodes', 'faults', 'protocol',
                'consensus_tps', 'consensus_latency_ms',
                'end_to_end_tps', 'end_to_end_latency_ms',
            ],
        )
        writer.writeheader()
        writer.writerows(all_rows)
    print(f'Merged {len(all_rows)} rows to {output_csv}')
    _print_dag_summary(all_rows, protocol_list)


# ═══════════════════════════════════════════════════════════════
# Figure 3: DAG protocol comparison under crash faults
#   - Protocols: NovelDAG, Narwhal (Tusk), Wahoo
#   - Nodes: 10
#   - Faults: 0, 1, 3
#   - 500KB max block size, 512B tx
# ═══════════════════════════════════════════════════════════════

@task
def paper_fig3(
    ctx,
    duration=30,
    debug=True,
    nodes=10,
    faults='0,1,3',
    workers=1,
    tx_size=512,
    runs=2,
    rate_start=60_000,
    rate_step=30_000,
    rate_end=300_000,
    protocols='noveldag,narwhal,wahoo',
    consensus='round_robin',
    output_csv='csv_plots/paper_fig3.csv',
):
    ''' Figure 3: Rate sweep across crash faults (0, 1, 3), nodes=10.

    Compares NovelDAG, Narwhal (Tusk), and Wahoo.
    - WAN measurements (AWS remote testbed)
    - 500KB max block size, 512B transaction size
    '''
    fault_list = [int(x.strip()) for x in str(faults).split(',') if x.strip()]
    protocol_list = [p.strip() for p in str(protocols).split(',') if p.strip()]
    rate_list = list(range(int(rate_start), int(rate_end) + 1, int(rate_step)))

    for proto in protocol_list:
        if proto not in ('narwhal', 'bullshark', 'noveldag', 'wahoo'):
            raise BenchError(f'Invalid dag_protocol: {proto}')

    node_params = {
        'header_size': 1_000,
        'max_header_delay': 200,
        'gc_depth': 50,
        'sync_retry_delay': 10_000,
        'sync_retry_nodes': 3,
        'batch_size': 500_000,
        'max_batch_delay': 200,
        'consensus_protocol': consensus,
    }

    all_rows = []
    for f in fault_list:
        Print.heading(f'=== nodes={nodes} faults={f} ===')
        partial_csv = f'{output_csv}.f{f}.tmp.csv'
        bench_params = {
            'faults': f,
            'nodes': [int(nodes)],
            'workers': int(workers),
            'collocate': True,
            'rate': rate_list,
            'tx_size': int(tx_size),
            'duration': int(duration),
            'runs': int(runs),
        }
        _run_dag_sweep(ctx, bench_params, node_params, protocol_list,
                        partial_csv, debug)

        try:
            with open(partial_csv, newline='') as fh:
                for row in csv.DictReader(fh):
                    all_rows.append(row)
        except FileNotFoundError:
            Print.warn(f'Partial CSV not found: {partial_csv}')

    with open(output_csv, 'w', newline='') as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                'rate', 'nodes', 'faults', 'protocol',
                'consensus_tps', 'consensus_latency_ms',
                'end_to_end_tps', 'end_to_end_latency_ms',
            ],
        )
        writer.writeheader()
        writer.writerows(all_rows)
    print(f'Merged {len(all_rows)} rows to {output_csv}')
    _print_dag_summary(all_rows, protocol_list)


# ═══════════════════════════════════════════════════════════════
# Paper figure plotting
# ═══════════════════════════════════════════════════════════════

@task
def paper_plot_fig1(
    ctx,
    csv_path='csv_plots/paper_fig1_fig2.csv',
    out_dir='results',
):
    ''' Figure 1: Throughput-latency scatter per node count.

    One panel per node count (10, 20, 50). Each panel overlays
    NovelDAG, Narwhal, Wahoo with rate annotations. '''
    import matplotlib.pyplot as plt

    rows = _read_paper_csv(csv_path)
    protocols = sorted({r['protocol'] for r in rows})
    node_list = sorted({int(r['nodes']) for r in rows})

    markers = {'narwhal': 's', 'noveldag': 'o', 'wahoo': '^'}
    colors = {'narwhal': '#2196F3', 'noveldag': '#FF9800', 'wahoo': '#4CAF50'}

    fig, axes = plt.subplots(1, len(node_list), figsize=(6 * len(node_list), 5.5))
    if len(node_list) == 1:
        axes = [axes]

    for ax, n in zip(axes, node_list):
        subset = [r for r in rows if int(r['nodes']) == n]
        for proto in protocols:
            pts = [r for r in subset if r['protocol'] == proto]
            if not pts:
                continue
            xs = [float(r['end_to_end_tps']) for r in pts]
            ys = [float(r['consensus_latency_ms']) for r in pts]
            ax.plot(xs, ys, color=colors.get(proto), marker=markers.get(proto),
                    markersize=7, linewidth=1.8, label=proto.capitalize(), zorder=3)
            for r_pt in pts:
                ax.annotate(f'{int(r_pt["rate"])//1000}k',
                            (float(r_pt['end_to_end_tps']),
                             float(r_pt['consensus_latency_ms'])),
                            textcoords='offset points', xytext=(5, 5),
                            fontsize=6, alpha=0.7)
        ax.set_title(f'n = {n}')
        ax.set_xlabel('End-to-End Throughput (tx/s)')
        if ax is axes[0]:
            ax.set_ylabel('Consensus Latency (ms)')
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=8)

    fig.suptitle('Figure 1: Throughput--Latency under WAN (faults=0)',
                 fontweight='bold')
    fig.tight_layout()
    png = f'{out_dir}/paper_fig1_tps_latency.png'
    fig.savefig(png, dpi=180)
    plt.close(fig)
    print(f'Saved: {png}')


@task
def paper_plot_fig2(
    ctx,
    csv_path='csv_plots/paper_fig1_fig2.csv',
    out_dir='results',
    max_latency_ms=5_000,
):
    ''' Figure 2: Maximum throughput per protocol keeping latency < 5s.

    Bar chart grouped by node count (10, 20, 50). '''
    import matplotlib.pyplot as plt

    rows = _read_paper_csv(csv_path)
    protocols = sorted({r['protocol'] for r in rows})
    node_list = sorted({int(r['nodes']) for r in rows})

    colors = {'narwhal': '#2196F3', 'noveldag': '#FF9800', 'wahoo': '#4CAF50'}

    # For each (protocol, nodes), find max TPS where latency <= max_latency_ms
    best = {p: [] for p in protocols}
    for n in node_list:
        for proto in protocols:
            pts = [r for r in rows
                   if int(r['nodes']) == n and r['protocol'] == proto
                   and float(r['consensus_latency_ms']) <= max_latency_ms]
            max_tps = max((float(r['end_to_end_tps']) for r in pts), default=0)
            best[proto].append(max_tps)

    fig, ax = plt.subplots(figsize=(10, 5))
    x = range(len(node_list))
    width = 0.25
    for i, proto in enumerate(protocols):
        bars = ax.bar([xi + i * width for xi in x], best[proto], width,
                       label=proto.capitalize(),
                       color=colors.get(proto, '#999'),
                       edgecolor='black', linewidth=0.5)
        for bar, val in zip(bars, best[proto]):
            if val > 0:
                ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 500,
                        f'{val:,.0f}', ha='center', va='bottom', fontsize=8)

    ax.set_xticks([xi + width for xi in x])
    ax.set_xticklabels([str(n) for n in node_list])
    ax.set_xlabel('Committee Size')
    ax.set_ylabel('Max End-to-End Throughput (tx/s)')
    ax.set_title(f'Figure 2: Max Throughput (latency < {max_latency_ms // 1000}s, faults=0)')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

    fig.tight_layout()
    png = f'{out_dir}/paper_fig2_max_tps.png'
    fig.savefig(png, dpi=180)
    plt.close(fig)
    print(f'Saved: {png}')


@task
def paper_plot_fig3(
    ctx,
    csv_path='csv_plots/paper_fig3.csv',
    out_dir='results',
):
    ''' Figure 3: Throughput-latency under crash faults (0, 1, 3).

    One panel per fault count. Each panel overlays the three protocols. '''
    import matplotlib.pyplot as plt

    rows = _read_paper_csv(csv_path)
    protocols = sorted({r['protocol'] for r in rows})
    fault_list = sorted({int(r['faults']) for r in rows})

    markers = {'narwhal': 's', 'noveldag': 'o', 'wahoo': '^'}
    colors = {'narwhal': '#2196F3', 'noveldag': '#FF9800', 'wahoo': '#4CAF50'}

    fig, axes = plt.subplots(1, len(fault_list), figsize=(6 * len(fault_list), 5.5))
    if len(fault_list) == 1:
        axes = [axes]

    for ax, f in zip(axes, fault_list):
        subset = [r for r in rows if int(r['faults']) == f]
        for proto in protocols:
            pts = [r for r in subset if r['protocol'] == proto]
            if not pts:
                continue
            xs = [float(r['end_to_end_tps']) for r in pts]
            ys = [float(r['consensus_latency_ms']) for r in pts]
            ax.plot(xs, ys, color=colors.get(proto), marker=markers.get(proto),
                    markersize=7, linewidth=1.8, label=proto.capitalize(), zorder=3)
            for r_pt in pts:
                ax.annotate(f'{int(r_pt["rate"])//1000}k',
                            (float(r_pt['end_to_end_tps']),
                             float(r_pt['consensus_latency_ms'])),
                            textcoords='offset points', xytext=(5, 5),
                            fontsize=6, alpha=0.7)
        ax.set_title(f'f = {f}')
        ax.set_xlabel('End-to-End Throughput (tx/s)')
        if ax is axes[0]:
            ax.set_ylabel('Consensus Latency (ms)')
        ax.grid(True, alpha=0.3)
        ax.legend(fontsize=8)

    fig.suptitle('Figure 3: Throughput--Latency under Crash Faults (n = 10)',
                 fontweight='bold')
    fig.tight_layout()
    png = f'{out_dir}/paper_fig3_faults.png'
    fig.savefig(png, dpi=180)
    plt.close(fig)
    print(f'Saved: {png}')


@task
def paper_plot_all(ctx, out_dir='results'):
    ''' Run all three paper plots in sequence. '''
    paper_plot_fig1(ctx, out_dir=out_dir)
    paper_plot_fig2(ctx, out_dir=out_dir)
    paper_plot_fig3(ctx, out_dir=out_dir)
    Print.heading('All paper figures saved to ' + out_dir)


def _read_paper_csv(path):
    """Read a paper sweep CSV and return list of dicts with numeric types."""
    rows = []
    with open(path, newline='') as f:
        for row in csv.DictReader(f):
            rows.append(row)
    return rows
