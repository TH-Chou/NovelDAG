# Benchmark Runbook

This document explains how to build, run, parse, and archive NovelDAG experiments locally and on cloud machines.

## Reproducibility Status

The benchmark workflow is highly reproducible. The repository contains the
protocol implementations, local runners, GCP/AWS cloud runners, paper-oriented
sweep tasks, parsers, and archived summary data. The remaining non-repository
inputs are normal private environment settings: cloud credentials, SSH key
paths, GCP project access, and the exact controller machine setup.

The current GCP settings describe the WAN testbed used by the archived
experiments:

```text
settings file: benchmark/settings.gcp.json
provider: gcp
project: noveldag-496906
zones: asia-east1-a, asia-southeast1-a, us-east1-b, us-west1-a, europe-west1-b
machine type: n2-standard-2
image: ubuntu-2204-lts
disk: 100GB pd-ssd
ssh user: ubuntu
base port: 5000
```

Before a new cloud reproduction, update the private fields in
`settings.gcp.json`: the SSH key path, local GCP authentication, project
permissions, and `repo.branch`. For this archive, `repo.branch` should be
`icde_shortfin_archive`.

Use a Unix-like controller environment for running the benchmark scripts. The
local and cloud orchestration invoke commands such as `tmux`, `rm`, `ln`, and
`bash`. Native Windows PowerShell is useful for repository management, but the
benchmark control path is intended for macOS, Linux, or WSL. Local RTT/delay
runs specifically use macOS `pfctl` and `dnctl`.

## Prerequisites

Install:

```text
Rust toolchain
Python 3
tmux
clang / C++ build tools
Fabric Python dependencies
gcloud CLI, only for GCP cloud runs
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
fab local --dag-protocol=shortfin --rate=50000
```

The current `fab local` task is a quick fixed-shape smoke test: 4 nodes,
`f=0`, 1 worker, 512-byte transactions, and a 20-second timed run. It exposes
only `--dag-protocol`, `--protocol`, `--rate`, and `--debug`. Use
`run_bench.py` for configurable local matrices.

Useful exposed parameters:

| Parameter | Meaning |
| --- | --- |
| `--dag-protocol` | `narwhal`, `bullshark`, `shortfin`, or `wahoo`. |
| `--rate` | Offered load in transactions per second. |
| `--protocol` | `round_robin` or `common_coin`. |
| `--debug` | Enable more verbose node logs. |

## Local Matrix Run With `run_bench.py`

Dry-run a matrix:

```bash
cd benchmark
python3 scripts/run_bench.py --mode local run \
  --protocols shortfin,narwhal,wahoo \
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
  --protocols shortfin,narwhal,wahoo \
  --rates 60000,150000,250000 \
  --faults 0 \
  --delays 0 \
  --runs 1 \
  --duration 12 \
  --nodes 4 \
  --output-prefix shortfin_archive_smoke \
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
  --protocols shortfin,narwhal,wahoo \
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

The checked-in GCP example currently records the original WAN topology, but it
also contains machine-local values. Confirm these fields before running:

| Field | Current role | What to check |
| --- | --- | --- |
| `key.path` | Controller SSH private key path. | Replace with the key path on the controller machine; the matching `.pub` file is used when creating GCP VMs. |
| `key.user` / `instances.ssh_user` | Remote SSH user. | Keep `ubuntu` for the Ubuntu image unless the image changes. |
| `instances.project` | GCP project. | The controller must be authenticated and authorized for `noveldag-496906` or a replacement project. |
| `instances.zones` | WAN placement. | Current zones span Asia, US, and Europe; `fab create --nodes=2` creates up to 10 VMs total. |
| `instances.type` | VM size. | Current archived setting is `n2-standard-2`. |
| `repo.branch` | Remote code version. | Use `icde_shortfin_archive` for this branch archive. |

The GCP instance manager creates a firewall rule allowing SSH and TCP
`5000-7000`, creates Ubuntu 22.04 instances with 100GB SSD boot disks, and
labels them with the configured instance name for later discovery.

## Cloud Lifecycle With Fabric

From `benchmark/`:

```bash
fab create --settings=settings.gcp.json --nodes=2
fab info --settings=settings.gcp.json
fab install --settings=settings.gcp.json
fab remote --settings=settings.gcp.json --dag-protocol=shortfin --nodes=10 --faults=0 --rate=100000 --duration=30 --runs=1
fab kill --settings=settings.gcp.json
fab stop --settings=settings.gcp.json
fab start --settings=settings.gcp.json --max=2
fab destroy --settings=settings.gcp.json
```

`--nodes` in `fab create` is per configured zone/region. With the current five
GCP zones, `--nodes=2` creates a 10-machine testbed and `--nodes=4` creates a
20-machine testbed.

The cloud runner uses `tmux` on each remote machine. It fetches and checks out
the configured branch, builds `cargo build --release --features benchmark`,
uploads committee/key/parameter files, starts workers before primaries, waits
for primary ports, starts clients, checkpoints logs on the remote machines, then
downloads and parses them into `benchmark/logs/results/`.

Benchmark duration is capped by `BenchParameters.MAX_DURATION_SECONDS = 50`.
Use `--duration=30` for the paper helpers or at most `--duration=50` unless the
code is intentionally changed.

## Paper WAN Sweeps With Fabric

The paper-oriented cloud tasks are implemented in `benchmark/scripts/fabfile.py`.
They run the same cloud deployment path as `fab remote` and then write summary
CSV files.

Figure 1/2 style WAN sweep:

```bash
fab paper-fig1-fig2 --settings=settings.gcp.json --nodes=10,20 --runs=2 --duration=30
```

By default this task uses protocols `shortfin,narwhal,wahoo`, rates
`30000..240000` with step `30000`, `faults=0`, 512-byte transactions, and
500KB batches. The script default also includes `nodes=50`; pass `--nodes=10,20`
when reproducing only the currently archived 10- and 20-node data.

Figure 3 style crash-fault sweep:

```bash
fab paper-fig3 --settings=settings.gcp.json --nodes=10 --faults=0,1,3 --runs=2 --duration=30
```

Plotting helper:

```bash
fab paper-plot-all
```

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

Cloud mode in `run_bench.py` ignores local dummynet delay labels. Use it for
cloud rate/fault/protocol matrices, not for local RTT emulation.

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

