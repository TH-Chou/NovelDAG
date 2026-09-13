# Shortfin equivocation analysis (2026-09-13)

## Configuration

- 4 local authorities, 1 worker each, 1 Byzantine authority
- 30,000 tx/s offered load, 512-byte transactions, 60-second runs
- No injected network delay
- Equivocation payloads are transmitted and validated but excluded from useful TPS

## Root causes

1. The Byzantine primary refused certificate recovery as well as worker payload
   recovery. The intended attack only refuses worker payload pulls. A Shortfin
   replica could therefore wait a full 10-second parent retry interval.
2. Retried delivery of the same Shortfin vote could reach the aggregator twice.
3. Shortfin consensus stored only one `(author, round)` branch. A canonical block
   and a bubble variant overwrote one another, leaving exact parent references
   absent from causal traversal.
4. Wave collection filtered all blocks below the maximum committed round across
   authors. A valid block omitted from one wave could therefore never be emitted
   later, even when a later anchor exposed it.

## Changes

- `13244b8`: make retried vote delivery idempotent.
- `162d338`: keep primary certificate recovery available during equivocation and
  avoid empty retry broadcasts.
- `d8fb713`: store locally produced headers before advertising them.
- `d5e7a21`: retain exact Shortfin equivocation branches, count quorum and anchor
  weight by distinct author, search every leader branch for the QC chain, and
  commit late blocks using per-author frontiers.

## Results

| Protocol / condition | Consensus TPS | Consensus latency (ms) | E2E TPS | E2E latency (ms) |
|---|---:|---:|---:|---:|
| Shortfin, no fault | 29,384.55 | 3,385.48 | 28,931.94 | 3,994.18 |
| Shortfin, equivocation run 1 | 22,386.17 | 2,652.93 | 22,023.72 | 3,273.42 |
| Shortfin, equivocation run 2 | 22,385.36 | 2,980.44 | 21,958.59 | 3,568.29 |
| Mahi-Mahi, equivocation | 19,316.93 | 8,352.16 | 19,016.67 | 9,040.81 |

The two final Shortfin attack runs average 22,385.77 consensus TPS. The no-fault
run scaled by the three honest transaction sources is 22,038.41 TPS. The attack
therefore has no measurable additional throughput loss in this local test. It is
15.9% faster than the matched Mahi-Mahi attack run and has 66.3% lower consensus
latency.

Committed payload references are balanced after the fix: 403/403/402 across the
three honest authors in run 1 and 394/394/379 in run 2. Before the exact-branch
and per-author-frontier fix, the variant recipient frequently committed only
about half as many payload references as the other honest authors.

## Profiling notes

- Existing `DIAG_PROPOSER_*` and `DIAG_CORE_SIGNAL_BLOCKED` events were used; no
  new runtime instrumentation remains in the code.
- WSL exposed 16 logical CPUs. During a Shortfin attack run, each primary used
  about 1.3% CPU and workers used about 20% each, so primary CPU saturation was
  not the cause.
- Raw local logs are retained under `profile_output/` and ignored by Git.

## Verification

- `cargo test -p consensus -p primary -p worker --lib --no-fail-fast`
- `cargo check -p primary -p worker -p consensus -p config`
- Result: 15 consensus, 51 primary, and 8 worker tests passed.
