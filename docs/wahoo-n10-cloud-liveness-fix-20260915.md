# Wahoo 10-node cloud liveness fix (2026-09-15)

## Goal

Reproduce the existing 10-node Wahoo GCP point after removing redundant
recipient-side `Done` broadcasts, diagnose any remaining WAN liveness issue,
and obtain one valid no-fault measurement at 120k tx/s.

## Configuration

- GCP project: `shortfin`
- 10 `n2-standard-2` VMs, two VMs in each of five regions
- 25 GB `pd-ssd` per VM
- 0 Byzantine faults, one worker per authority, collocated deployment
- Round-robin leader selection, 512-byte transactions
- Offered rate: 120,000 tx/s
- Measured duration: 50 seconds after a 20-second mesh warm-up

## First cloud attempt

Commit `144bb490` included the redundant-`Done` fix but not the late-parent
fix. The run returned zero throughput. This was a real protocol stall rather
than a parser failure:

- all ten primaries stopped at round 17;
- every primary saw only five round-17 EPBC deliveries, below the 7-of-10
  round-advance threshold;
- no primary panic or fatal error occurred;
- the downloaded logs contained no committed payload.

The root cause was an arrival-order race. `handle_fast_block` retained an EPBC
proposal in `epbc_blocks` when one of its parents was not yet in the local DAG,
but it returned without voting and never reconsidered that proposal after the
parent arrived. WAN reordering could therefore permanently deprive an honest
proposer of the votes needed to form its delivery certificate.

## Fix

Commit `3537895e` incrementally retries next-round EPBC proposals whenever a
new parent block enters the DAG. The retry only emits the missing TS1/TF votes;
it does not deliver an uncertified block, create an extra proposal, or add a
new broadcast stage. Existing per-phase vote deduplication makes repeated
retry triggers harmless.

The GCP runner version check was also corrected in commit `144bb490`: image
provenance (`image_commit`) is now separate from the runtime source commit,
which defaults to the local `HEAD`.

## Validation

The focused Wahoo node suite passed 9/9 tests, including a new test where an
EPBC proposal arrives before its parent. A 10-node local run with approximately
200 ms RTT also completed twice rather than stalling:

| Attempt | Consensus TPS | Consensus latency | E2E TPS | E2E latency |
| --- | ---: | ---: | ---: | ---: |
| Initial | 12,821 | 7,045 ms | 12,087 | 10,302 ms |
| Retry | 21,678 | 6,125 ms | 20,808 | 8,759 ms |

These local throughput values are CPU-constrained because all ten authorities
share one host; they are liveness checks, not cloud-performance estimates.

The fixed GCP run produced:

| Consensus TPS | Consensus latency | E2E TPS | E2E latency |
| ---: | ---: | ---: | ---: |
| 110,159 | 1,773 ms | 107,160 | 2,539 ms |

All ten primaries committed 5,799 batch records. Their first-to-last commit
spans were about 50.1 seconds, their maximum observed rounds were 128-129, and
no panic or fatal error appeared. The point is therefore valid rather than a
partial pre-stall sample.

Against the recent 10-node diagnostic point (112,470 consensus TPS, 1,866 ms;
106,587 E2E TPS, 2,692 ms), the fixed run changes consensus TPS by -2.05%, E2E
TPS by +0.54%, consensus latency by -4.98%, and E2E latency by -5.68%. Against
the archived historical 115,172 consensus TPS point, throughput is 4.35%
lower. These differences are within the expected range for a single WAN run.

## State

- Successful runtime commit: `3537895e4e6cbb9375681c7f3692d6b4ef9fd960`
- All ten GCP VMs were verified `TERMINATED` after the run.
- The result is sufficient to show that Wahoo now produces valid 10-node WAN
  data. Paper-level curves should still use repeated measurements near the
  saturation knee.
