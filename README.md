# Novel DAG

[![build status](https://img.shields.io/github/actions/workflow/status/asonnino/narwhal/rust.yml?branch=master&logo=github&style=flat-square)](https://github.com/asonnino/narwhal/actions)
[![rustc](https://img.shields.io/badge/rustc-1.51+-blue?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![python](https://img.shields.io/badge/python-3.9-blue?style=flat-square&logo=python&logoColor=white)](https://www.python.org/downloads/release/python-390/)
[![license](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](LICENSE)

This repository is a research-oriented DAG consensus codebase derived from Narwhal/Tusk and adapted for protocol experimentation. **Three DAG consensus protocols — Narwhal, Bullshark, and NovelDAG — are now unified in a single workspace**, selected at runtime via configuration.

- Implementation language: Rust
- Benchmark/automation scripts: Python + Fabric
- Core crates: `primary`, `consensus`, `worker`, `node`, `network`, `crypto`, `store`, `config`

The code is intended for experimentation and benchmarking, not production deployment.

## Three Protocols, One Codebase

Narwhal, Bullshark, and NovelDAG share the same crates. Protocol-specific logic is isolated in the consensus layer:

| Protocol | Consensus module | Leader rule | Commit rule |
| --- | --- | --- | --- |
| Narwhal | [consensus/src/narwhal.rs](consensus/src/narwhal.rs) | Elected at round `r-2` | f+1 support from `r-1` children, linked-path ordering |
| Bullshark | [consensus/src/bullshark.rs](consensus/src/bullshark.rs) | Elected at round `r` | f+1 support from `r+1` children, linked-path ordering |
| NovelDAG | [consensus/src/noveldag.rs](consensus/src/noveldag.rs) | Elected at round `r-3` | Same-author b3→b2→b1 chain with embedded QC links, pipelined commits |
| Wahoo | [primary/src/wahoo/](primary/src/wahoo/) (full state machine) + [consensus/src/wahoo.rs](consensus/src/wahoo.rs) (passthrough) | Even-round Elect: 2f+1 BLS partial sigs recover a coin selecting the leader of the previous (odd) round | `leader[r] ∧ done[r][leader] ∧ dag[r][leader]` at odd rounds, then transitive ancestor commit. 1:1 functional port of [Wahoo-main/wahoo/](Wahoo-main/wahoo/) (Go); see file-level mapping comments in `primary/src/wahoo/mod.rs`. |

Both **RoundRobin** and **CommonCoin** leader election modes are supported independently of the DAG protocol via the `consensus_protocol` parameter.

### Runtime selection

In your `parameters.json` (or equivalent settings), set:

```json
{
  "dag_protocol": "noveldag",
  "consensus_protocol": "common_coin"
}
```

- `dag_protocol`: `"narwhal"` | `"bullshark"` | `"noveldag"` | `"wahoo"` (default: `"noveldag"`)
- `consensus_protocol`: `"round_robin"` | `"common_coin"` (default: `"round_robin"`)

The `Header`, `Certificate`, and `Vote` wire formats use NovelDAG's extended structure (with `parents_2`, `embedded_qc`, `coin_share`, `voter_round`) as the universal format. Narwhal/Bullshark modes leave extension fields at their default/empty values, so those three protocols share the same message schemas.

**Wahoo deviates from the unified Header/Vote/Certificate schema** because the Go reference uses a different structural model (parity-dependent block tags, Ready/Done/Elect/ReVote messages, fast-path odd rounds). The Wahoo port keeps the same external interface (`dag_protocol = "wahoo"`, same `parameters.json`, same `tx_output` certificate stream) but internally bypasses the Narwhal-style Core/Proposer/Synchronizer pipeline. Wire-level Wahoo traffic is multiplexed through `PrimaryMessage::Wahoo(WahooMessage)`. See `primary/src/wahoo/messages.rs` for the schema and `Wahoo-main/wahoo/data_struct.go` for the Go source it matches 1:1.

## Quick Start

### 1) Prerequisites

- Rust toolchain (`cargo`, `rustc`)
- Python 3 + `pip`
- `clang` (required by RocksDB)
- `tmux` (used by benchmark scripts)

### 2) Build and test

From repository root:

```bash
cargo build
cargo test
```

### 3) Run local benchmark

```bash
cd benchmark
pip install -r requirements.txt
fab local
```

You can also run protocol comparison tasks from `benchmark/fabfile.py`, for example:

```bash
fab compare-consensus-groups --duration=30 --rounds=5 --rate=50000 --output-csv=results/my_avg.csv --output-runs-csv=results/my_runs.csv
```

## Run a node manually

`node` provides the executable entrypoint.

```bash
cargo run -p node -- --help
```

Key subcommands:

- `generate_keys`
- `run primary`
- `run worker --id <INT>`

See `node/src/main.rs` for full CLI flags and required config files.

## Docs

- Benchmark guide: `benchmark/README.md`
- Primary module notes: `primary/README.md`
- Worker module notes: `worker/README.md`
- Protocol notes for this repo: `new DAG structure.md`

## Cloud Benchmarks

Cloud benchmark configuration lives in `benchmark/settings.json`, and the
stable Fabric entrypoint is `benchmark/fabfile.py`.

```bash
cd benchmark
fab info
fab install
fab remote --dag-protocol=noveldag --nodes=10 --faults=0 --rate=10000 --duration=20 --runs=1
```

For 10/20/50-node AWS sweeps and the paper figure commands, see
`benchmark/README.md`.

## License

This software is licensed as [Apache 2.0](LICENSE).
