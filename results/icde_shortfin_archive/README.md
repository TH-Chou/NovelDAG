# ICDE Shortfin Result Archive

This directory stores lightweight experiment data for the `icde_shortfin_archive` branch. It intentionally keeps only CSV data and small text summaries. Full logs, RocksDB directories, checkpoints, plots, and tmux runtime artifacts are not archived here.

## Layout

```text
results/icde_shortfin_archive/
  README.md
  manifest.csv
  local/
    csv/
      *.csv
  cloud/
    cloud_wan_summary.csv
    summaries/
      bench-*.txt
```

## Manifest

`manifest.csv` lists every archived data file with:

```text
environment,path,bytes,sha256
```

Use it to check which source files were archived and to verify that copied data did not change.

## Local CSV Files

Local files are copied from ignored benchmark output under:

```text
benchmark/csv_plots/
benchmark/csv_plots/csv_plots/
```

They are archived under:

```text
results/icde_shortfin_archive/local/csv/
```

Most local CSV files are self-describing and include the experimental parameters in the header:

```text
run,protocol,faults,delay_ms,rate,consensus_tps,consensus_latency_ms,end_to_end_tps,end_to_end_latency_ms
```

Some RTT files use:

```text
run,protocol,faults,rtt_ms,one_way_delay_ms,rate,consensus_tps,consensus_latency_ms,end_to_end_tps,end_to_end_latency_ms
```

Older sweep files may use:

```text
protocol,faults,delay_ms,rate,run,tps,latency_ms
```

### Local File Index

| File | Description |
| --- | --- |
| `bench_runs.csv` | Minimal single-run local parser output. It has metric columns only, so parameters must be inferred from the command history rather than the file itself. |
| `runner_fix_probe_runs.csv` | Local smoke probe after fixing the benchmark runner; Shortfin, 4 nodes, f=0, rate 60k. |
| `sailfin_smoke_compare2_runs.csv` | Local Shortfin vs Sailfin smoke matrix, 4 nodes, f=0, rates 60k/150k/250k, one run per point, duration 12s. |
| `shortfin_f0_250k_supp_runs.csv` | Local Shortfin f=0 250k supplementary run. |
| `sailfin_f0_250k_fastpath_probe_runs.csv` | Local Sailfin f=0 250k probe. The filename is historical; current `sailfin.rs` no longer uses the old edge-voted fast path. |
| `shortfin_sailfin_f0_250k_runs.csv` | Local Shortfin/Sailfin f=0 250k comparison, multiple runs. |
| `shortfin_sailfin_f0_250k_after_fastlog_runs.csv` | Local Shortfin/Sailfin f=0 250k comparison after benchmark logging fixes. |
| `shortfin_sailfin_f0_330k_runs.csv` | Local Shortfin/Sailfin f=0 330k stress probe. Rows with all-zero metrics indicate failed or unparseable runs. |
| `shortfin_sailfin_f0_330k_d30_runs.csv` | Local Shortfin/Sailfin f=0 330k duration-30 stress probe. All-zero rows indicate failed or unparseable runs. |
| `local_rtt_sweep_280k_runs.csv` | Local RTT sweep at 280k offered load. `delay_ms` is one-way dummynet delay. |
| `local_rtt_sweep_280k_runs_with_rtt.csv` | Same local RTT sweep with explicit `rtt_ms = 2 * one_way_delay_ms`. |
| `rtt_sweep_results.csv` | Older local sweep format with `tps` and `latency_ms` columns. |

## Cloud / WAN Data

Cloud summary files are copied from ignored benchmark output under:

```text
benchmark/logs/results/
```

They are archived under:

```text
results/icde_shortfin_archive/cloud/summaries/
```

These text summaries follow the legacy filename format:

```text
bench-FAULTS-NODES-WORKERS-COLLOCATE-INPUT_RATE-TX_SIZE-PROTOCOL-runRUN.txt
```

Where:

| Field | Meaning |
| --- | --- |
| `FAULTS` | Number of crash/dead nodes configured in the benchmark. |
| `NODES` | Committee size. |
| `WORKERS` | Workers per node. |
| `COLLOCATE` | Whether primary and worker are colocated. |
| `INPUT_RATE` | Offered transaction rate. |
| `TX_SIZE` | Transaction size in bytes. |
| `PROTOCOL` | Protocol label used by that run. |
| `RUN` | Run index. |

The parsed cloud CSV is:

```text
results/icde_shortfin_archive/cloud/cloud_wan_summary.csv
```

It extracts both parameters and metrics from the text summaries:

```text
environment,source_file,protocol,run,faults,nodes,workers,collocate,rate,input_rate,tx_size,execution_time_s,header_size,max_header_delay_ms,gc_depth_rounds,sync_retry_delay_ms,sync_retry_nodes,batch_size,max_batch_delay_ms,consensus_tps,consensus_latency_ms,end_to_end_tps,end_to_end_latency_ms,consensus_bps,end_to_end_bps
```

### Cloud Labels

The historical cloud summaries use `noveldag` as a protocol label. In this archive, treat `noveldag` as the historical Shortfin/NovelDAG implementation label, not the current `sailfin` rolling-discovery variant.

The archived cloud summaries currently contain:

```text
faults = 1
nodes = 10
workers = 1
tx_size = 512
protocols = narwhal, noveldag, wahoo
rates = 30000, 60000, 90000, 120000, 150000, 180000, 210000, 240000
```

Only the combinations present in `cloud_wan_summary.csv` are archived.

## Notes On Interpretation

- Local data is useful for smoke tests, runner validation, and rough Shortfin/Sailfin comparisons.
- Cloud/WAN summaries are closer to paper-style data, but this archive contains only compact summaries, not full raw logs.
- All-zero metric rows in local CSV files indicate failed, overloaded, or unparseable runs and should not be averaged as successful measurements.
- CSV headers should be considered authoritative when they include parameter columns.
- For files without parameter columns, use this README and the original filename/context.

## Related Documentation

See:

```text
docs/icde-shortfin-archive.md
docs/benchmark-runbook.md
docs/experiment-data-notes.md
docs/sailfin-rolling-discovery.md
```
