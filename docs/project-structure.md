# Project Structure

NovelDAG is a Rust workspace with a Python benchmark suite. The main experiment matrix supports Narwhal/Tusk, Shortfin, Mahi-Mahi, and Wahoo.

## Top-Level Layout

```text
NovelDAG/
  node/        Rust binaries: node and benchmark_client
  primary/     primary DAG state machine, proposer, vote/QC handling
  worker/      transaction batching and worker networking
  consensus/   protocol-specific ordering and commit logic
  config/      shared config and wire-format types
  crypto/      digests, signatures, threshold coin helpers
  network/     TCP transport, senders, receivers
  store/       RocksDB-backed storage
  benchmark/   Python local/cloud experiment framework
  docs/        project and experiment documentation
```

## Rust Crates

| Crate | Main Role |
| --- | --- |
| `node` | CLI entrypoint. Runs primaries, workers, key generation, and the benchmark client. |
| `primary` | Builds DAG blocks, broadcasts headers, receives votes, forms certificates/QCs, and sends certificates to consensus. |
| `worker` | Receives client transactions, batches them, and sends batch digests to the local primary. |
| `consensus` | Orders certificates and emits committed certificates according to the selected DAG protocol. |
| `config` | Defines `Committee`, `Parameters`, `DagProtocol`, `ConsensusProtocol`, `Header`, `Certificate`, and `Vote`. |
| `crypto` | Hashing, public keys, signatures, BLS threshold coin helpers. |
| `network` | Reliable and simple senders plus async receivers. |
| `store` | RocksDB persistence layer. |

## Protocol Selection

The runtime protocol is selected by `Parameters.dag_protocol`:

```json
{
  "dag_protocol": "shortfin",
  "consensus_protocol": "round_robin"
}
```

Supported values:

```text
narwhal
shortfin
mahi_mahi
mahi_mahi_4
mahi_mahi_5
wahoo
```

The legacy `bullshark` implementation remains available outside the main experiment matrix.

Leader election is selected independently with `consensus_protocol`:

```text
round_robin
pseudo_random
common_coin
```

The selection is routed in `consensus/src/lib.rs`:

```text
DagProtocol::Narwhal   -> consensus/src/narwhal.rs
DagProtocol::Bullshark -> consensus/src/bullshark.rs
DagProtocol::Shortfin  -> consensus/src/shortfin.rs
DagProtocol::MahiMahi  -> consensus/src/mahi_mahi.rs
DagProtocol::Wahoo     -> consensus/src/wahoo.rs
```

Wahoo has additional primary-side logic in `primary/src/wahoo/`.

## Shared Leader Election

All protocols use `crypto/src/coin.rs` for authority ordering, leader mapping,
deterministic pseudorandom values, threshold-share generation and verification,
coin recovery, and bounded caches. Protocols only determine when their coin is
revealed and how it is transported:

| Protocol | Common-coin material |
| --- | --- |
| Narwhal/Tusk | Shares in even-round headers used to reveal the preceding leader. |
| Shortfin | Shares only in wave-boundary headers (`round % 4 == 0`). |
| Mahi-Mahi | Shares in every decision-capable block after startup because its waves overlap. |
| Wahoo | Shares in the existing even-round `Elect` messages. |

`pseudo_random` performs no coin communication. It derives a deterministic value
from the sorted committee and logical coin round through the same shared module.

## Shortfin-Family Data Path

The Shortfin-family path uses the extended universal header format:

```text
Header {
  author,
  round,
  payload,
  parents,
  parents_2,
  qc,
  coin_share,
  voter_round,
  ...
}
```

Important fields:

| Field | Meaning |
| --- | --- |
| `parents` | First-hop references to round `r-1`. |
| `parents_2` | Second-hop references to round `r-2`; Shortfin-family protocols require quorum coverage. |
| `qc` | Embedded quorum certificate for the author's previous-round block. |
| `coin_share` | Threshold coin share used by header-based common-coin leader election. |
| `voter_round` | Used to reject QCs whose votes are not earlier than the commit boundary. |

## Core Runtime Flow

1. Clients send transactions to workers.
2. Workers batch transactions and send batch digests to the local primary.
3. Primaries build headers once they have enough parents and payload.
4. Other primaries vote for valid headers.
5. A primary forms a certificate/QC after reaching quorum.
6. The consensus task receives certificates from the primary.
7. The selected consensus module orders certificates and sends committed certificates back to primary/output.

## Consensus Modules

| Module | Summary |
| --- | --- |
| `narwhal.rs` | Classic Narwhal/Tusk-style ordering over certified DAG certificates. |
| `bullshark.rs` | Bullshark-style leader ordering. |
| `shortfin.rs` | Baseline 4-round Shortfin wave with same-author embedded-QC leader chain. |
| `mahi_mahi.rs` | Mahi-Mahi uncertified-DAG ordering with direct/indirect commit, direct/indirect skip, longest-prefix decisions, deterministic linearization, and a four-stage variant. |

Mahi-Mahi keeps every observed equivocation in its consensus DAG and evaluates
votes through deterministic graph traversal. Quorums count distinct authors,
not block digests. Its incremental support, certificate, and link caches avoid
repeating full-DAG traversals, while weak links carry otherwise uncovered
history forward without repeatedly attaching the same old reference.
| `wahoo.rs` | Passthrough for Wahoo primary-side decisions. |

## Shortfin Baseline

Shortfin commits at deterministic wave boundaries:

```text
WAVE = 4
commit_round = r, where r % 4 == 0
leader_round = r - 3
```

The leader chain must satisfy:

```text
B0 at r-3 by leader L
B1 at r-2 by leader L, carrying QC(B0)
B2 at r-1 by leader L, carrying QC(B1)
```

All QC votes must have `voter_round < commit_round`.
