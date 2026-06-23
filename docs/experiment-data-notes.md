# Experiment Data Notes

This document records the benchmark data layout, metric definitions, and the latest local smoke comparison between Shortfin and Sailfin.

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

The CSV schema is:

```text
run,protocol,faults,delay_ms,rate,consensus_tps,consensus_latency_ms,end_to_end_tps,end_to_end_latency_ms
```

## Current Local Smoke Data

Command:

```bash
cd benchmark
python3 scripts/run_bench.py --mode local run \
  --protocols shortfin,sailfin \
  --rates 60000,150000,250000 \
  --faults 0 \
  --delays 0 \
  --runs 1 \
  --duration 12 \
  --nodes 4 \
  --output-prefix sailfin_smoke_compare2 \
  --fresh
```

Output CSV:

```text
benchmark/csv_plots/sailfin_smoke_compare2_runs.csv
```

Raw rows:

| run | protocol | faults | delay_ms | rate | consensus_tps | consensus_latency_ms | end_to_end_tps | end_to_end_latency_ms |
| ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | shortfin | 0 | 0 | 60000 | 53239.624153 | 2641.594424 | 49555.592910 | 3188.050947 |
| 1 | shortfin | 0 | 0 | 150000 | 136500.000603 | 2315.999986 | 125818.544272 | 2795.116857 |
| 1 | shortfin | 0 | 0 | 250000 | 133947.252416 | 1962.782789 | 128039.656943 | 2417.294608 |
| 1 | sailfin | 0 | 0 | 60000 | 51002.432871 | 2626.547326 | 49309.903548 | 3159.501325 |
| 1 | sailfin | 0 | 0 | 150000 | 137054.278619 | 2148.326368 | 132300.534661 | 2603.275561 |
| 1 | sailfin | 0 | 0 | 250000 | 146796.013221 | 2062.556170 | 138265.298285 | 2520.935814 |

Relative Sailfin vs Shortfin:

| rate | consensus TPS delta | consensus latency delta | end-to-end TPS delta | end-to-end latency delta |
| ---: | ---: | ---: | ---: | ---: |
| 60000 | -4.20% | -0.57% | -0.50% | -0.90% |
| 150000 | +0.41% | -7.24% | +5.15% | -6.86% |
| 250000 | +9.59% | +5.08% | +7.99% | +4.29% |

## Interpretation

This is a smoke test, not a publication-grade data point:

- It uses only one run per point.
- It runs locally with 4 nodes and no artificial RTT.
- It is sensitive to local CPU scheduling, tmux process startup, and RocksDB/cache state.
- It is useful as a sanity check that Sailfin still runs and does not obviously regress throughput.

Initial observation:

- Sailfin is close to Shortfin at low offered load.
- At `150k`, Sailfin slightly improves throughput and latency in this single run.
- At `250k`, Sailfin improves throughput but has slightly higher latency.

For paper-quality data, use at least:

```text
10 or 20 nodes
3 or more runs per point
multiple offered loads around saturation
WAN or controlled RTT settings
outlier policy recorded in the config
```

## Existing Data Files

Common generated data locations:

```text
benchmark/csv_plots/
benchmark/plots/
benchmark/logs/results/
```

Examples currently used by the plotting workflow include:

```text
benchmark/csv_plots/local_rtt_sweep_280k_runs.csv
benchmark/csv_plots/local_rtt_sweep_280k_runs_with_rtt.csv
benchmark/csv_plots/shortfin_sailfin_f0_250k_after_fastlog_runs.csv
benchmark/csv_plots/sailfin_smoke_compare2_runs.csv
```

## Reproducibility Checklist

Before running:

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

After running:

```bash
python3 scripts/run_bench.py plot --csv csv_plots/<file>.csv --chart-type all
```

For cloud runs, also archive:

```text
settings.json or settings.gcp.json with private paths redacted
logs/results/*.txt
csv_plots/*.csv
plots/*.png or *.pdf
```
