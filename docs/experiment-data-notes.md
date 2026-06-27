# Experiment Data Notes

This document records the benchmark data layout and metric definitions for the Shortfin / historical NovelDAG result archive.

The archived data files for this branch live under:

```text
results/icde_shortfin_archive/
```

That directory separates local CSV data from cloud/WAN summaries and includes its own README plus a SHA-256 manifest.

## Metrics

The benchmark parser reports:

| Metric | Meaning |
| --- | --- |
| `consensus_tps` | Throughput computed from committed batch bytes divided by transaction size and consensus duration. |
| `consensus_latency_ms` | Batch proposal time to consensus commit time. |
| `end_to_end_tps` | Throughput measured from client start time to final commit time. |
| `end_to_end_latency_ms` | Client sampled transaction send time to commit time. |

Most archived local CSV files use:

```text
run,protocol,faults,delay_ms,rate,consensus_tps,consensus_latency_ms,end_to_end_tps,end_to_end_latency_ms
```

The normalized cloud summary uses:

```text
environment,source_file,protocol,raw_protocol,run,faults,nodes,workers,collocate,rate,input_rate,tx_size,execution_time_s,header_size,max_header_delay_ms,gc_depth_rounds,sync_retry_delay_ms,sync_retry_nodes,batch_size,max_batch_delay_ms,consensus_tps,consensus_latency_ms,end_to_end_tps,end_to_end_latency_ms,consensus_bps,end_to_end_bps
```

## Local Data

Local CSV files are under:

```text
results/icde_shortfin_archive/local/csv/
```

Important local files:

| File | Meaning |
| --- | --- |
| `shortfin_smoke_runs.csv` | Shortfin-only smoke rows, 4 nodes, f=0, 60k/150k/250k offered load. |
| `shortfin_f0_250k_runs.csv` | Shortfin-only f=0 250k multi-run data. |
| `shortfin_f0_250k_after_fastlog_runs.csv` | Shortfin-only f=0 250k data after benchmark logging fixes. |
| `shortfin_f0_330k_runs.csv` | Shortfin-only f=0 330k stress probe. |
| `shortfin_f0_330k_d30_runs.csv` | Shortfin-only f=0 330k duration-30 stress probe. |
| `local_rtt_sweep_280k_runs.csv` | Local RTT sweep at 280k offered load. Historical `noveldag` rows are Shortfin/NovelDAG. |
| `local_rtt_sweep_280k_runs_with_rtt.csv` | Same RTT sweep with explicit RTT field. |
| `rtt_sweep_results.csv` | Older local sweep format. Historical `noveldag` rows are Shortfin/NovelDAG. |

Rows with all-zero metrics are failed or unparseable runs and should not be averaged as successful measurements.

## Cloud/WAN Data

Cloud summary files are under:

```text
results/icde_shortfin_archive/cloud/summaries/
```

A parsed CSV is available at:

```text
results/icde_shortfin_archive/cloud/cloud_wan_summary.csv
```

The historical cloud files use `noveldag` in filenames. In `cloud_wan_summary.csv`, those rows are normalized as:

```text
raw_protocol = noveldag
protocol = shortfin
```

Archived cloud summary shape:

```text
faults = 1
nodes = 10
workers = 1
tx_size = 512
protocols = narwhal, shortfin, wahoo
raw protocol labels = narwhal, noveldag, wahoo
rates = 30000, 60000, 90000, 120000, 150000, 180000, 210000, 240000
```

## Reproducibility Checklist

Before running new experiments:

```bash
git status --short
cargo build --release --features benchmark
tmux kill-server || true
```

Record:

```text
git branch
git rev-parse HEAD
benchmark command
config YAML
cloud settings file, if remote
CSV output path
plot output path
```

For cloud runs, also archive:

```text
settings.json or settings.gcp.json with private paths redacted
logs/results/*.txt
csv_plots/*.csv
plots/*.png or *.pdf
```
