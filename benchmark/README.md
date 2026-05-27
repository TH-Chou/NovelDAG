# Benchmark Workflow

## Quick Reference

```bash
cd benchmark
pip install -r requirements.txt
fab local --dag-protocol=shortfin --rate=50000 --duration=20
fab remote --dag-protocol=shortfin --nodes=10 --faults=1 --rate=10000 --duration=20 --runs=1
```

## Entry Points

There are **three ways** to run benchmarks, from simplest to most flexible:

### 1. Fabric CLI (`fab`) — Quick single-config runs

The stable entrypoint is `benchmark/fabfile.py`, which wraps `benchmark/scripts/fabfile.py`.

```bash
# Local
fab local --dag-protocol=shortfin --rate=50000 --duration=20

# Cloud
fab create --nodes=2          # launch EC2 instances
fab info                      # list all instances
fab install                   # deploy code + compile
fab remote --rate=50000 --nodes=10 --faults=1 --runs=2 --duration=60

# Protocol comparison
fab compare-consensus-groups --rate=50000 --rounds=5
fab compare-dag-protocols --rate=50000 --faults=1 --nodes=10
fab sweep-dag-rates --rate-start=30000 --rate-step=30000 --rate-end=210000
fab full-dag-bench            # sweep + plot in one shot

# Paper figures
fab paper-fig1-fig2 --nodes=10,20,50 --runs=2
fab paper-fig3 --faults=0,1,3 --nodes=10 --runs=2
fab paper-plot-all
```

#### Key `fab remote` Parameters

| Parameter | Default | Description |
|-----------|---------|-------------|
| `--rate` | 10,000 | Injection rate (tx/s) |
| `--nodes` | 10 | Committee size |
| `--faults` | 3 | Byzantine faults |
| `--workers` | 1 | Workers per node |
| `--tx-size` | 512 | Transaction size (bytes) |
| `--duration` | 300 | Benchmark duration (seconds) |
| `--runs` | 2 | Runs per config |
| `--dag-protocol` | shortfin | narwhal / bullshark / shortfin / sailfin / wahoo |
| `--protocol` | round_robin | round_robin / common_coin (leader election) |
| `--benchmark-delay` | 0 | Delay before timed phase (seconds, for P2P warmup) |
| `--max-header-delay` | 200 | Proposer timer interval (ms, increase for cross-region) |

### 2. `dagtest-TUI` — Unified benchmark console

```bash
cd benchmark
python scripts/dagtest-TUI
python scripts/dagtest-TUI run --mode local --protocol shortfin --rates 60000 --faults 0 --delays 0 --runs 1
python scripts/dagtest-TUI full --mode local --protocols narwhal,shortfin,sailfin,wahoo --rates 60000,130000 --faults 0,1 --delays 0,100 --runs 3
python scripts/dagtest-TUI run --mode aws --settings settings.json --config scripts/configs/full_sweep.yaml --group all_smoke
```

`dagtest-TUI` works both as an interactive terminal UI and as a non-interactive CLI. Use `--runs` (or `--groups`) for the number of repetitions per matrix point, `--faults`/`--byzantine` for Byzantine node counts, `--delays` for local one-way dummynet delay in ms, and `--mode local|aws|gcp` for local vs cloud execution.

### 3. Unified CLI (`run_bench.py`) — Matrix runner with YAML configs

```bash
cd benchmark
python scripts/run_bench.py --mode local run --config scripts/configs/smoke.yaml
python scripts/run_bench.py --mode aws --settings settings.json run --config scripts/configs/full_sweep.yaml
```

**Subcommands shared by `dagtest-TUI` and `run_bench.py`:**
| Command | Purpose |
|---------|---------|
| `run` | Execute benchmarks (local or cloud) |
| `collect` | Download cloud logs |
| `parse` | Parse logs → CSV |
| `plot` | Generate charts from CSV |
| `full` | run → collect → parse → plot |

**Dry-run mode:** `--dry-run` prints the resolved test matrix without executing anything.

```bash
python scripts/run_bench.py --mode local run --config scripts/configs/smoke.yaml --dry-run
# Output: protocol, faults, delay, rate, run_index for each point
```

### 4. Pipeline Orchestrator (`run_bench_pipeline.py`) — Full automation

The engine behind `run_bench.py`. Provides:

- **Local mode:** `run_local(points, ...)` — iterates config points, configures dummynet delays, spawns subprocess, checkpoints progress
- **Remote mode:** `run_remote(points, ...)` — delegates to `Bench.run_batch()` with checkpointing
- **Parsing:** `parse_and_aggregate(points, ...)` — `LogParser.process()` per directory, outlier rejection, per-run + aggregated CSV
- **Plotting:** `generate_plots(csv_path, ...)` — TPS-Latency scatter + TPS distribution box plot

## Test Matrix Config (YAML)

Config files live in `scripts/configs/`. The system performs a Cartesian product of protocols × rates × faults × delays × runs.

**Example (`smoke.yaml`):**
```yaml
groups:
  smoke:
    protocols: [shortfin]
    rates: [60000]
    faults: [0]
    runs: 1
    bench:
      nodes: 4
      duration: 15
```

**Full config reference (`full_sweep.yaml`):**
```yaml
groups:
  d0:        # 0ms delay: 3 protocols × 5 rates × 3 faults
  d100:      # 100ms delay: 3 protocols × 4 rates × 3 faults
  rtt_sweep: # 3 protocols × 4 rates × 3 faults × 2 delays
```

Each resolved point: `{bench, node, delay, protocol, faults, rate, run_index, group, label}`.

## Cloud Lifecycle

```bash
fab create --nodes=2      # Launch instances (2 per region × 5 regions = 10 total)
fab info                  # List all instances with SSH commands
fab install               # Clone repo + compile on all machines
fab remote ...            # Run benchmarks
fab kill                  # Kill all processes
fab stop                  # Stop instances (preserve data)
fab start --max=10        # Restart stopped instances
fab destroy               # Terminate all instances
```

Configuration in `benchmark/settings.json`:
```json
{
  "key": { "name": "dag-key", "path": "/Users/.../.ssh/dag-key.pem" },
  "port": 5000,
  "repo": { "name": "NovelDAG", "url": "https://github.com/...", "branch": "unify-three-protocols" },
  "instances": { "type": "t3.medium", "regions": ["us-east-1", "..."] },
  "provider": "aws"
}
```

## Result File Layout

All paths generated by `benchmark/utils.py:PathMaker`:

```
benchmark/
  logs/
    primary-{i}.log          # Primary node i logs
    worker-{i}-{j}.log       # Worker j of node i logs
    client-{i}-{j}.log       # Client j of node i logs
    .committee.json           # Committee config
    .parameters.json          # Node parameters
    .node-{i}.json            # Keypair for node i
    results/
      bench-{f}-{n}-{w}-{collocate}-{rate}-{tx}-{dag}-run{N}.txt   # Per-run summary
  csv_plots/
    {name}.csv               # Aggregated CSV data
  plots/
    {name}.pdf / {name}.png  # Generated charts
```

## Log Parsing & Metrics

`benchmark/logs.py:LogParser` reads all log files in a directory:

1. **Client logs** — extract transaction timestamps
2. **Primary logs** — extract consensus commit events (`DIAG_COMMIT_LATENCY`)
3. **Worker logs** — extract batch formation events

**Output metrics:**
| Metric | Source |
|--------|--------|
| Consensus TPS / BPS | `DIAG_COMMIT_LATENCY` events from primaries |
| Consensus latency | Time from certificate receipt to commit |
| End-to-end TPS / BPS | Client transaction timestamps |
| End-to-end latency | Client submit → commit |

## Plotting

Three plotting paths:

**1. Fabric plot task:**
```bash
fab plot   # After a remote run, plots from logs/
```

**2. CSV-based plot (from pipeline):**
```bash
python scripts/run_bench.py plot --csv csv_plots/rtt_sweep_results.csv --chart-type all
```

`benchmark/plot.py:Ploter` generates:
- **Latency vs Throughput** scatter with error bars
- **TPS vs Variable** (nodes/workers/rate) bar charts

## Package Reference

| Module | Purpose |
|--------|---------|
| `benchmark/config.py` | Data classes: `Committee`, `NodeParameters`, `BenchParameters`, `PlotParameters` |
| `benchmark/commands.py` | Shell command builder: `CommandMaker` (compile, run, kill, cleanup) |
| `benchmark/utils.py` | Path generation: `PathMaker`; logging: `Print`; error types |
| `benchmark/logs.py` | Log parsing: `LogParser.process(dir, faults)` → metrics |
| `benchmark/aggregate.py` | Multi-run aggregation: `LogAggregator` |
| `benchmark/plot.py` | Matplotlib plotting: `Ploter` |
| `benchmark/local.py` | Local runner: `LocalBench` |
| `benchmark/remote.py` | Cloud runner: `Bench` (Fabric SSH + tmux) |
| `benchmark/instance.py` | Cloud VMs: `AWSInstanceManager`, `GCPInstanceManager`, `InstanceManager` |
| `benchmark/settings.py` | Settings loader: `Settings.load(filename)` |

## Scripts Reference (`scripts/`)

| Script | Purpose |
|--------|---------|
| `fabfile.py` | 28+ Fabric tasks (local, remote, paper, compare, sweep, plot) |
| `dagtest-TUI` | Main benchmark console; interactive TUI or direct CLI |
| `run_bench_tui.py` | Python implementation behind `dagtest-TUI` |
| `run_bench.py` | Unified CLI with subcommands `run/collect/parse/plot/full` |
| `run_bench_pipeline.py` | Orchestrator: local/remote execution, parsing, CSV, plotting |
| `run_bench_config.py` | YAML config loader + Cartesian product resolver |
| `run_bench_checks.py` | Pre-flight checks (binaries, tools, cloud CLI) |
| `run_bench_outliers.py` | Outlier rejection: middle-N or std-dev methods |
| `fast_parse_logs.py` | Fast parser used by cloud Wahoo runs |
| `run_local_rtt_sweep_280k.sh` | Local dummynet RTT sweep helper |

## Dry-Run / Test Modes

```bash
# Print resolved matrix without executing
python scripts/run_bench.py --mode local run --config scripts/configs/smoke.yaml --dry-run

# Quick validation (4 nodes, 15s, 1 run, 0 faults)
python scripts/run_bench.py --mode local run --config scripts/configs/smoke.yaml

# Interactive TUI wizard
python scripts/dagtest-TUI
```

## Cross-Region Tuning

When running across multiple AWS regions with high latency:

```bash
fab remote --benchmark-delay=50 --max-header-delay=2000 --duration=60 ...
```

- `--benchmark-delay=N` — wait N seconds after boot for P2P mesh to stabilize before starting timer
- `--max-header-delay=N` — increase proposer timer to tolerate cross-region QC propagation (default 200ms)
