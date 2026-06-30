# ICDE Shortfin Archive

This branch archives the Shortfin-family implementation and benchmark workflow used for the ICDE-oriented experiments. It is intended to make the repository understandable without relying on chat history.

## What Is Included

- Shortfin baseline consensus in `consensus/src/shortfin.rs`.
- Unified protocol selection through `dag_protocol` in `config/src/lib.rs`.
- Local and cloud benchmark runners under `benchmark/`.
- Shortfin / historical NovelDAG result archive and parser notes.

## Documentation Map

| Document | Purpose |
| --- | --- |
| [Project Structure](project-structure.md) | Crates, modules, protocol entry points, and data flow. |
| [Benchmark Runbook](benchmark-runbook.md) | Build, local runs, RTT runs, cloud runs, TUI/CLI usage, outputs. |
| [Experiment Data Notes](experiment-data-notes.md) | Metrics, CSV layout, smoke-test results, and data interpretation. |

The result archive committed with this branch is:

```text
paperdata/
```

Use `paperdata/README.md` as the primary map from manuscript figures to
archived CSV and summary text files.

## Reproducibility Status

This archive is intended to be highly reproducible. The repository contains the
implementation, benchmark orchestration scripts, paper-oriented cloud sweep
tasks, parsers, plotting/data helpers, and summary-level archived data. The
expected missing pieces are private environment settings rather than research
logic: SSH private keys, GCP authentication, GCP project permissions, and the
controller machine's tool installation.

The tracked GCP settings file records the archived WAN testbed shape:

```text
benchmark/settings.gcp.json
provider: gcp
project: noveldag-496906
zones: asia-east1-a, asia-southeast1-a, us-east1-b, us-west1-a, europe-west1-b
machine type: n2-standard-2
image: ubuntu-2204-lts
disk: 100GB
ssh user: ubuntu
```

For detailed local, RTT, and GCP reproduction commands, see
`docs/benchmark-runbook.md`.

## Current Branch Purpose

The branch name used for this archive is:

```bash
icde_shortfin_archive
```

For cloud runs, make sure the cloud settings point to the branch that should be deployed. The tracked examples currently live in:

```text
benchmark/settings.json
benchmark/settings.gcp.json
```

Set the `repo.branch` field to `icde_shortfin_archive` when reproducing this archive on remote machines.

## Quick Sanity Commands

From the repository root:

```bash
cargo check -p consensus
cargo check -p consensus --features benchmark
cargo test -p consensus --lib
```

From `benchmark/`:

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

## Important Local Notes

The local benchmark parser needs benchmark-level `info` logs. The local runner sets `RUST_LOG=info` for spawned `tmux` jobs so an inherited shell value such as `RUST_LOG=warn` does not hide client and primary log lines required by the parser.

The matrix runner also sets Python multiprocessing to `fork` inside its local subprocess. This avoids Python 3.14 spawn-mode failures when `run_bench.py` launches benchmark jobs through `python -c`.
