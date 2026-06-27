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
results/icde_shortfin_archive/
```

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
