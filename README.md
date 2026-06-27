# NovelDAG

[![build status](https://img.shields.io/github/actions/workflow/status/asonnino/narwhal/rust.yml?branch=master&logo=github&style=flat-square)](https://github.com/asonnino/narwhal/actions)
[![rustc](https://img.shields.io/badge/rustc-1.51+-blue?style=flat-square&logo=rust)](https://www.rust-lang.org)
[![python](https://img.shields.io/badge/python-3.9-blue?style=flat-square&logo=python&logoColor=white)](https://www.python.org/downloads/release/python-390/)
[![license](https://img.shields.io/badge/license-Apache-blue.svg?style=flat-square)](LICENSE)

Research-oriented DAG consensus codebase derived from Narwhal/Tusk, adapted for protocol experimentation. **Five DAG consensus protocols — Narwhal, Bullshark, Shortfin, Sailfin, and Wahoo — unified in a single workspace**, selected at runtime via configuration.

- **Language:** Rust (consensus), Python + Fabric (benchmarks)
- **License:** Apache 2.0

---

## Code Structure

```
NovelDAG/
  node/              # Binary entrypoint (main, benchmark_client)
  primary/           # Primary state machine: proposer, synchronizer, certificate waiter
    src/wahoo/       # Wahoo protocol state machine (full port from Go reference)
  consensus/         # Protocol consensus modules
    src/shortfin.rs  # Shortfin: 4-round wave, b3→b2→b1 leader chain, pipelined commits
    src/sailfin.rs   # Sailfin: experimental Shortfin-family variant
    src/narwhal.rs   # Narwhal: r-2 leader, f+1 support, linked-path ordering
    src/bullshark.rs # Bullshark: r leader, f+1 support, linked-path ordering
    src/wahoo.rs     # Wahoo passthrough (commit decisions made in primary)
  worker/            # Transaction batching worker
  network/           # P2P transport (simple + reliable sender, receiver)
  crypto/            # BLS signatures, hashing, key generation
  store/             # RocksDB-backed persistent storage
  config/            # Wire-format types: Header, Certificate, Vote, Parameters
  benchmark/         # Python benchmark suite (see benchmark/README.md)
```

### Key Crates

| Crate | Role |
|-------|------|
| `node` | CLI entrypoint: `generate_keys`, `run primary`, `run worker --id N` |
| `primary` | Manages certificates: broadcasts headers, collects votes, forms QCs |
| `worker` | Batches client transactions → sends to primary |
| `consensus` | Protocol-specific commit logic; receives certificates from primary, outputs ordered sequence |
| `network` | Async P2P with reliable delivery, keep-alive, auto-reconnect |
| `crypto` | BLS threshold signatures, `Digest` (SHA-256), `PublicKey` |
| `store` | RocksDB key-value store for DAG persistence |
| `config` | `Header`, `Certificate`, `Vote`, `Parameters`, `Committee` — shared wire types |

---

## Five Protocols, One Codebase

Protocol-specific logic is isolated in the consensus layer:

| Protocol | Consensus module | Leader rule | Commit rule |
| --- | --- | --- | --- |
| Narwhal | [consensus/src/narwhal.rs](consensus/src/narwhal.rs) | Elected at round `r-2` | f+1 support from `r-1` children, linked-path ordering |
| Bullshark | [consensus/src/bullshark.rs](consensus/src/bullshark.rs) | Elected at round `r` | f+1 support from `r+1` children, linked-path ordering |
| Shortfin | [consensus/src/shortfin.rs](consensus/src/shortfin.rs) | Elected at round `r-3` | Same-author b3→b2→b1 chain with embedded QC links, pipelined commits |
| Sailfin | [consensus/src/sailfin.rs](consensus/src/sailfin.rs) | Experimental | Rolling-discovery Shortfin variant with conservative barrier finalization |
| Wahoo | [primary/src/wahoo/](primary/src/wahoo/) (state machine) + [consensus/src/wahoo.rs](consensus/src/wahoo.rs) (passthrough) | Even-round Elect: 2f+1 BLS partial sigs → coin for odd-round leader | `leader[r] ∧ done[r][leader] ∧ dag[r][leader]` at odd rounds, transitive ancestor commit. 1:1 port of [Go reference](Wahoo-main/wahoo/) |

Leader election modes (`consensus_protocol`): **RoundRobin** or **CommonCoin** — selectable independently of the DAG protocol.

### Runtime selection

In `parameters.json`:
```json
{
  "dag_protocol": "shortfin",
  "consensus_protocol": "common_coin"
}
```

- `dag_protocol`: `"narwhal"` | `"bullshark"` | `"shortfin"` | `"sailfin"` | `"wahoo"` (default: `"shortfin"`)
- `consensus_protocol`: `"round_robin"` | `"common_coin"` (default: `"round_robin"`)

The `Header`, `Certificate`, and `Vote` wire formats use the Shortfin-family extended structure (with `parents_2`, `embedded_qc`, `coin_share`, `voter_round`) as the universal format. Narwhal/Bullshark modes leave extension fields at their default/empty values.

**Wahoo** deviates from the unified schema — it uses its own message types (`PrimaryMessage::Wahoo(WahooMessage)`) internally, with parity-dependent block tags, Ready/Done/Elect/ReVote messages, and fast-path odd rounds. See `primary/src/wahoo/messages.rs`.

---

## Shortfin Protocol

### System Model

- **n** processes, up to **f** Byzantine faults, with `n ≥ 3f + 1`
- Authenticated point-to-point network (messages unforgeable, eventual delivery)
- Logical rounds `r = 0, 1, 2, ...`

### Block Structure

```
b = (id, proposer, round, parents₁, parents₂, qc)
```

- `parents₁`: all known blocks from round `r-1`
- `parents₂`: ≥2f+1 blocks from round `r-2`
- `qc`: proposer's own QC from round `r-1`

### DAG Construction

Each node maintains a local DAG `G = (V, E)`. Edges point only to earlier rounds (`r-1` or `r-2`), guaranteeing acyclicity.

**Block construction per round `r`:**
1. Connect to all received `r-1` blocks → `parents₁`
2. Connect to ≥2f+1 `r-2` blocks → `parents₂`
3. Embed `QC(bᵢ^{r-1})` (proposer's cert from previous round)
4. Broadcast → collect votes → form QC (≥2f+1 votes)

### Voting

Each node votes immediately upon receiving a valid block. A vote contains `(voter, round, target_block)`. A **QC (Quorum Certificate)** requires ≥2f+1 votes from distinct nodes on the same block.

### Round Advancement

A node advances from round `r` to `r+1` when:
1. Its own `r`-round block collects ≥2f+1 votes, **AND**
2. It receives ≥2f+1 blocks from round `r`

### Commit Rule (4-Round Wave)

Each wave spans 4 rounds. At every `r % 4 == 0`:

1. Elect leader of round `r-3` (via RoundRobin or CommonCoin)
2. Verify the **b₃ → b₂ → b₁** chain from the same author:
   - b₃: leader's block at `r-3`
   - b₂: same author at `r-2` (carries QC(b₃))
   - b₁: same author at `r-1` (carries QC(b₂))
3. Check that all QC votes have `round < r`
4. If satisfied → commit b₃, then recursively commit its causal ancestors

---

## Quick Start

### Prerequisites

- Rust toolchain (`cargo`, `rustc`)
- Python 3 + `pip`
- `clang` (required by RocksDB)
- `tmux` (used by benchmark scripts)

### Build & Test

```bash
cargo build
cargo test
```

### Run a Node Manually

```bash
cargo run -p node -- --help    # Full CLI
cargo run -p node -- generate_keys --filename keys.json
cargo run -p node -- run primary --keys keys.json --committee committee.json --store store --parameters parameters.json
cargo run -p node -- run worker --keys keys.json --committee committee.json --store store --parameters parameters.json --id 0
```

Config files: `committee.json`, `parameters.json`, `node-{i}.json` (keypair). Generated by benchmark scripts.

---

## Testing Approaches

### 1. Unit Tests

```bash
cargo test                          # All crates
cargo test -p consensus             # Consensus-specific
cargo test -p primary               # Primary-specific
```

### 2. Local Benchmark (Single Machine)

```bash
cd benchmark
pip install -r requirements.txt
fab local --dag-protocol=shortfin --rate=50000 --duration=20
```

Runs `n=4` nodes on localhost via tmux. Config files generated in `benchmark/logs/`. See `benchmark/README.md` for full parameter reference.

### 3. Protocol Comparison (Local)

```bash
fab compare-consensus-groups --duration=30 --rounds=5 --rate=50000
fab local --dag-protocol=shortfin --rate=50000
fab local --dag-protocol=narwhal --rate=50000
```

### 4. Dry-Run Matrix

```bash
python benchmark/scripts/run_bench.py --mode local run --config benchmark/scripts/configs/smoke.yaml --dry-run
```

Prints the resolved test matrix without executing anything.

### 5. Cloud Benchmark (AWS/GCP)

```bash
cd benchmark
fab create --nodes=2
fab info
fab install
fab remote --dag-protocol=shortfin --nodes=10 --faults=0 --rate=10000 --duration=20 --runs=1
fab kill
fab destroy
```

### 6. Pipeline (Full Automation)

```bash
python benchmark/scripts/run_bench.py --mode local run --config benchmark/scripts/configs/smoke.yaml
python benchmark/scripts/run_bench.py --mode aws --settings benchmark/settings.json run --config benchmark/scripts/configs/full_sweep.yaml
python benchmark/scripts/run_bench.py full --config benchmark/scripts/configs/smoke.yaml
```

Subcommands: `run` → `collect` (cloud) → `parse` → `plot`. Use `full` to chain all.

### 7. Interactive TUI

```bash
python benchmark/scripts/run_bench_tui.py
```

### 8. Paper Figures

```bash
fab paper-fig1-fig2 --nodes=10,20,50 --faults=0 --runs=2 --duration=30
fab paper-fig3 --nodes=10 --faults=0,1,3 --runs=2 --duration=30
fab paper-plot-all
```

---

## Docs

- ICDE Shortfin archive index: [docs/icde-shortfin-archive.md](docs/icde-shortfin-archive.md)
- Project structure: [docs/project-structure.md](docs/project-structure.md)
- Benchmark runbook: [docs/benchmark-runbook.md](docs/benchmark-runbook.md)
- Experiment data notes: [docs/experiment-data-notes.md](docs/experiment-data-notes.md)
- Sailfin rolling discovery notes: [docs/sailfin-rolling-discovery.md](docs/sailfin-rolling-discovery.md)
- **Benchmark guide:** [benchmark/README.md](benchmark/README.md) — full workflow, scripts, entry points, parameters, plotting
- Primary module notes: [primary/README.md](primary/README.md)
- Worker module notes: [worker/README.md](worker/README.md)
