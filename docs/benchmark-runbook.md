# Benchmark Runbook

This document explains how to build, run, parse, and archive NovelDAG experiments locally and on cloud machines.

## Prerequisites

Install:

```text
Rust toolchain
Python 3
tmux
clang / C++ build tools
Fabric Python dependencies
```

From the repository root:

```bash
cargo check -p consensus
cargo test -p consensus --lib
```

From `benchmark/`:

```bash
python3 -m pip install -r requirements.txt
```

## Benchmark Entry Points

There are three main entry points:

| Entry Point | Path | Best Use |
| --- | --- | --- |
| Fabric tasks | `benchmark/fabfile.py` and `benchmark/scripts/fabfile.py` | Single local/remote runs and legacy paper workflows. |
| Unified CLI | `benchmark/scripts/run_bench.py` | Matrix runs, parse, plot, local/cloud orchestration. |
| TUI wrapper | `benchmark/scripts/dagtest-TUI` | Interactive or direct CLI benchmark control. |

## Local Single Run With Fabric

```bash
cd benchmark
fab local --dag-protocol=shortfin --rate=50000 --duration=20
fab local --dag-protocol=sailfin --rate=50000 --duration=20
```

Useful parameters:

| Parameter | Meaning |
| --- | --- |
| `--dag-protocol` | `narwhal`, `bullshark`, `shortfin`, `sailfin`, or `wahoo`. |
| `--rate` | Offered load in transactions per second. |
| `--duration` | Timed benchmark duration in seconds. |
| `--faults` | Number of omitted/faulty nodes. |
| `--nodes` | Committee size. |
| `--tx-size` | Transaction size in bytes. |

## Local Matrix Run With `run_bench.py`

Dry-run a matrix:

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
  --dry-run
```

Run the matrix:

```bash
python3 scripts/run_bench.py --mode local run \
  --protocols shortfin,sailfin \
  --rates 60000,150000,250000 \
  --faults 0 \
  --delays 0 \
  --runs 1 \
  --duration 12 \
  --nodes 4 \
  --output-prefix sailfin_smoke_compare \
  --fresh
```

The local runner writes CSV data to:

```text
benchmark/csv_plots/{output-prefix}_runs.csv
```

## `dagtest-TUI` Direct CLI

The TUI can be used interactively:

```bash
cd benchmark
python3 scripts/dagtest-TUI
```

It can also be driven as a normal CLI:

```bash
python3 scripts/dagtest-TUI run \
  --mode local \
  --protocols shortfin,sailfin \
  --rates 60000,150000,250000 \
  --faults 0 \
  --delays 0 \
  --runs 1 \
  --duration 12 \
  --nodes 4 \
  --fresh
```

Common aliases:

| Option | Alias |
| --- | --- |
| `--protocols` | `--protocol` |
| `--rates` | `--rate` |
| `--faults` | `--byzantine` |
| `--runs` | `--groups` |
| `--delays` | `--delay` |

## YAML Configs

Config files live in:

```text
benchmark/scripts/configs/
```

Important files:

| File | Purpose |
| --- | --- |
| `smoke.yaml` | Quick single-protocol smoke test. |
| `full_sweep.yaml` | Narwhal/Shortfin/Wahoo rate/fault sweeps. |
| `local_rtt_sweep_280k.yaml` | Local dummynet RTT sweep at fixed offered load. |

Example:

```bash
python3 scripts/run_bench.py --mode local run \
  --config scripts/configs/full_sweep.yaml \
  --group d0 \
  --fresh
```

## Local RTT / Delay Runs

Local delay is configured through macOS `pfctl` and `dnctl`. The configured delay is one-way, so `--delays 100` corresponds approximately to RTT `200 ms`.

Example:

```bash
python3 scripts/run_bench.py --mode local run \
  --protocols narwhal,shortfin,wahoo \
  --rates 280000 \
  --faults 0,1,3 \
  --delays 0,50,100,150,200 \
  --runs 1 \
  --duration 30 \
  --nodes 10 \
  --fresh
```

There is also a helper:

```bash
bash scripts/run_local_rtt_sweep_280k.sh
```

## Cloud Settings

AWS settings:

```text
benchmark/settings.json
```

GCP settings:

```text
benchmark/settings.gcp.json
```

Before reproducing this archive remotely, set:

```json
"repo": {
  "name": "NovelDAG",
  "url": "https://github.com/TH-Chou/NovelDAG.git",
  "branch": "icde_shortfin_archive"
}
```

## Cloud Lifecycle With Fabric

From `benchmark/`:

```bash
fab create --nodes=2
fab info
fab install
fab remote --dag-protocol=shortfin --nodes=10 --faults=0 --rate=100000 --duration=60 --runs=1
fab remote --dag-protocol=sailfin --nodes=10 --faults=0 --rate=100000 --duration=60 --runs=1
fab kill
fab stop
fab start --max=10
fab destroy
```

The cloud runner uses `tmux` on each remote machine. Logs are collected back into `benchmark/logs/` or batch download directories depending on the task.

## Cloud Matrix With Unified CLI

AWS:

```bash
python3 scripts/run_bench.py --mode aws \
  --settings settings.json \
  run --config scripts/configs/full_sweep.yaml \
  --group all_smoke
```

GCP:

```bash
python3 scripts/run_bench.py --mode gcp \
  --settings settings.gcp.json \
  run --config scripts/configs/full_sweep.yaml \
  --group all_smoke
```

## Logs And Output Layout

```text
benchmark/
  logs/
    primary-{i}.log
    worker-{i}-{j}.log
    client-{i}-{j}.log
    .committee.json
    .parameters.json
    .node-{i}.json
    results/
      bench-{faults}-{nodes}-{workers}-{collocate}-{rate}-{tx}-{protocol}-run{N}.txt
  csv_plots/
    *_runs.csv
    aggregated.csv
  plots/
    *.png
    *.pdf
  .checkpoints/
    *_checkpoint.json
```

Do not commit `.checkpoints/`, `logs/`, or generated database directories unless intentionally archiving a specific raw run.

## Parser Requirements

The local benchmark parser expects `info`-level log lines from clients, workers, and primaries. The local runner sets:

```bash
RUST_LOG=info
```

for each spawned `tmux` job. This prevents an inherited shell value like `RUST_LOG=warn` from hiding required lines such as:

```text
Transactions size: ...
Transactions rate: ...
Created ...
Committed ...
Batch ... contains ...
```

The unified local runner also forces Python multiprocessing to use `fork` inside its subprocess. This avoids Python 3.14 failures when parsing logs from a process launched through `python -c`.

