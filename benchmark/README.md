# Benchmark Workflow

Run Fabric commands from the benchmark root:

```bash
cd benchmark
fab --list
fab local
fab info
```

`benchmark/fabfile.py` is the stable Fabric entrypoint. The implementation
still lives in `benchmark/scripts/fabfile.py`; the wrapper keeps both old and
new invocation styles working.

## Local

```bash
cd benchmark
pip install -r requirements.txt
fab local --dag-protocol=noveldag --rate=50000 --duration=20
```

Local runs write runtime files under `benchmark/logs/`.

## AWS

Configure `benchmark/settings.json`, then use:

```bash
cd benchmark
fab create --nodes=2
fab info
fab install
fab remote --dag-protocol=noveldag --nodes=10 --faults=1 --rate=10000 --duration=20 --runs=1
fab kill
fab stop
```

`benchmark/settings.json` is the canonical cloud configuration. The legacy
`benchmark/scripts/settings.json` copy has been removed to avoid drift.

For 10/20/50-node experiments across the default five AWS regions, create or
start enough machines per region:

```bash
fab create --nodes=10     # 10 per region = 50 total
fab start --max=10        # start up to 10 stopped machines per region
fab info
```

The remote runner selects nodes round-robin across regions. If fewer than the
requested number of running machines are available, it stops before uploading
configs instead of silently running a smaller committee.

Paper-style 10/20/50 runs use the Fabric paper tasks:

```bash
fab paper-fig1-fig2 --nodes=10,20,50 --faults=0 --runs=2 --duration=30
fab paper-fig3 --nodes=10 --faults=0,1,3 --runs=2 --duration=30
```

Each run is archived separately under remote `batch_logs/` and downloaded to
per-rate/per-run local directories, so repeated runs do not overwrite one
another.

## Unified CLI

The matrix runner lives under `benchmark/scripts`:

```bash
python benchmark/scripts/run_bench.py --mode local run --config scripts/configs/smoke.yaml
python benchmark/scripts/run_bench.py --mode aws --settings settings.json run --config scripts/configs/smoke.yaml
```

For `argparse`, global flags such as `--mode` and `--settings` must appear
before the subcommand (`run`, `collect`, `full`, ...).
