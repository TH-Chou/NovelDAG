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

The settings loader also accepts `benchmark/scripts/settings.json` for
backward compatibility, but `benchmark/settings.json` is the canonical file.

## Unified CLI

The matrix runner lives under `benchmark/scripts`:

```bash
python benchmark/scripts/run_bench.py --mode local run --config benchmark/scripts/configs/smoke.yaml
python benchmark/scripts/run_bench.py --mode aws --settings benchmark/settings.json run --config benchmark/scripts/configs/smoke.yaml
```

For `argparse`, global flags such as `--mode` and `--settings` must appear
before the subcommand (`run`, `collect`, `full`, ...).
