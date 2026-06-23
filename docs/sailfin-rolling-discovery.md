# Sailfin Rolling Discovery

This document describes the current Sailfin implementation archived in `consensus/src/sailfin.rs`.

## Status

Sailfin is an experimental Shortfin-family variant. The older edge-voted fast-commit approach has been removed. The current code implements a conservative rolling-discovery baseline.

The design goal is:

```text
discover certified anchors more frequently,
but only output through a Shortfin-safe finalization barrier
```

## Baseline Shortfin

Shortfin commits at wave boundaries:

```text
WAVE = 4
commit_round = r, where r % 4 == 0
leader_round = r - 3
```

For the selected leader `L`, the protocol checks:

```text
B0 = block by L at r - 3
B1 = block by L at r - 2, carrying QC(B0)
B2 = block by L at r - 1, carrying QC(B1)
```

The chain is valid only if the embedded QC links are structurally valid and all QC votes have `voter_round < commit_round`.

## Sailfin Idea

Sailfin adds rolling discovery:

1. On every newly received certificate, scan recent candidate slot rounds.
2. If a slot has a valid same-author embedded-QC chain, store the anchor as pending evidence.
3. Pending anchors do not affect output.
4. At the normal Shortfin barrier, use the deterministic barrier leader chain as the finalizing frontier.
5. Remove pending anchors that are causally covered by the finalizing frontier.

In short:

```text
rolling discovery, barrier finalization
```

This is deliberately more conservative than immediate rolling commit.

## Code Path

Main state:

```rust
let mut pending_anchors: HashMap<Round, PendingAnchor> = HashMap::new();
```

On each certificate:

```rust
discover_pending_anchors(consensus, round, &state, &mut pending_anchors);
```

At a Shortfin wave boundary:

```rust
let (b3, b2, b1) = verify_leader_chain(consensus, commit_round, &state)?;
let finalized_pending = finalized_pending_anchors(&pending_anchors, &[&b3, &b2, &b1], &state);
let sequence = collect_wave(commit_round, &state, validity_threshold, &[&b3, &b2, &b1]);
```

The output sequence still comes from `collect_wave`, so the committed prefix follows the Shortfin-safe path.

## Why Pending Anchors Are Not Output Immediately

The safety issue with fully rolling immediate commit is ordering. If boundary `r` discovers an anchor at `r-3`, a neighboring boundary `r+1` may discover an anchor at `r-2`. That later anchor was created before boundary `r` was known, so it is not guaranteed to causally reach the earlier committed anchor.

Immediate output would need an additional commit/skip or dominance proof. The current implementation avoids that proof burden.

## Safety Posture

The implementation keeps these invariants:

| Invariant | Meaning |
| --- | --- |
| QC uniqueness | A fixed `(author, round)` cannot have two conflicting certified blocks under honest voting. |
| Pending is non-final | Pending anchors are evidence only and cannot fork the output sequence. |
| Barrier finalization | Output is still gated by the deterministic Shortfin barrier leader chain. |
| Deterministic collection | `collect_wave` sorts selected certificates deterministically. |

## Liveness Posture

Sailfin inherits Shortfin's liveness path because the final output path remains the Shortfin wave barrier. Rolling discovery currently improves observability and evidence accounting, not the theoretical minimum commit depth.

## Diagnostics

When built with the `benchmark` feature, Sailfin logs:

```text
DIAG_SAILFIN_ROLLING_DISCOVER
DIAG_SAILFIN_ROLLING_FINALIZE
DIAG_COMMIT_CANDIDATE
DIAG_COMMIT_CHAIN_OK
DIAG_WAVE_BATCH
DIAG_COMMIT_LATENCY
```

Useful command:

```bash
RUST_LOG=info cargo build --release --features benchmark
```

Then run local benchmarks through the Python runner, which sets `RUST_LOG=info` for spawned tmux jobs.

## Unit Test

The relevant unit test is:

```text
consensus_tests::sailfin_waits_for_barrier_finalization
```

It verifies that Sailfin no longer commits before the barrier. Rounds `1..=2` do not produce output; after round `4`, the normal barrier commits.

Run:

```bash
cargo test -p consensus --lib sailfin_waits_for_barrier_finalization
```

## Current Limitations

- No immediate rolling finality.
- No multi-leader slots yet.
- Pending anchors are currently used for diagnostics/finalization bookkeeping, not as an alternate output path.
- The current smoke data should not be treated as a proof of performance improvement.

## Next Possible Steps

Reasonable follow-up experiments:

1. Add deterministic multi-leader slots at the `WAVE=4` barrier.
2. Measure pending anchor hit rate under WAN jitter and crash faults.
3. Add a safe dominance-based output rule for anchors covered by later finalized frontiers.
4. Compare Shortfin and Sailfin under `f=0`, `f=1`, and `f=3` on GCP.
5. Record `DIAG_SAILFIN_ROLLING_DISCOVER` and `DIAG_SAILFIN_ROLLING_FINALIZE` counts in CSV output.

