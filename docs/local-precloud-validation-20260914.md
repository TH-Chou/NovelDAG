# Local Pre-Cloud Validation (2026-09-14)

## Material Passport

- ID: `local-precloud-validation-20260914`
- Type: experiment result and reproducibility validation
- Status: verified locally; not publication-quality WAN evidence
- Code commit: `5fc71c0`
- Platform: WSL2 Ubuntu, repository and RocksDB data on WSL ext4

## Scope

- Four collocated authorities, one worker per authority, 512-byte transactions.
- Protocols: Narwhal/Tusk, Shortfin, Mahi-Mahi-5, and Wahoo.
- Fault modes: no fault, one silent authority, and one equivocating authority.
- Wide sweep: 5k, 15k, 30k, 60k, 120k, 180k, 240k, and 300k offered TPS.
- Each wide-sweep point is one 60-second run with no injected delay.
- A representative 30k point was also run with 100 ms one-way loopback delay,
  which is approximately 200 ms RTT.
- Protocol and rate order was permuted between groups. Fresh keys and databases
  were generated for each run.
- Long soak tests were intentionally excluded from this pass.

The silence runner redistributes the displayed offered rate across the three
active honest clients. The equivocation runner retains all four clients, but
all payloads authored by the Byzantine authority model invalid or duplicate
transactions and are excluded from useful TPS. For n=4 and f=1, useful input
under equivocation is therefore capped at 75% of the displayed offered rate.

## Correctness Fixes Included

Four fixes separate this study from the superseded local data:

1. Round-robin selection now rotates logical leader slots rather than applying
   physical protocol rounds directly. Strided schedules therefore cover the
   full committee, including under silence.
2. Wahoo vote aggregation binds every vote to the proposal digest, round, and
   author, preventing votes for equivocated branches from being mixed.
3. A Wahoo PBC certificate replaces an earlier uncertified variant from the
   same author and round, so only the certified branch is delivered.
4. Valid late Wahoo EPBC blocks still receive slow-path votes. Honest replicas
   retain one-digest voting, while delayed delivery no longer stalls at 2/3.

The old Shortfin 14.9k-18k equivocation measurements and all Wahoo measurements
from before these fixes are invalid and are excluded from the curated CSVs.

## Representative 30k Results, No Injected Delay

Consensus throughput is in transactions per second; latency is in milliseconds.

| Mode | Protocol | Consensus TPS | Consensus latency | Useful-input utilization |
| --- | --- | ---: | ---: | ---: |
| Normal | Shortfin | 29,531 | 2,943 | 98.4% |
| Normal | Narwhal/Tusk | 24,751 | 5,168 | 82.5% |
| Normal | Mahi-Mahi | 27,894 | 4,861 | 93.0% |
| Normal | Wahoo | 22,544 | 317 | 75.1% |
| Silence | Shortfin | 29,981 | 3,593 | 99.9% |
| Silence | Narwhal/Tusk | 28,584 | 4,660 | 95.3% |
| Silence | Mahi-Mahi | 28,080 | 4,515 | 93.6% |
| Silence | Wahoo | 30,097 | 26 | 100.3% |
| Equivocation | Shortfin | 22,442 | 2,473 | 99.7% |
| Equivocation | Narwhal/Tusk | 20,879 | 3,404 | 92.8% |
| Equivocation | Mahi-Mahi | 20,765 | 7,525 | 92.3% |
| Equivocation | Wahoo | 22,542* | 67* | 100.2%* |

`*` Wahoo's final 30k equivocation runs measured 15,172, 22,542, and
22,549 TPS. The table reports the median. The first run is a local topology and
startup outlier; all three remained live for the full interval. Fresh key order
changes which authority is the attacker and which branch reaches each peer, so
a one-run localhost result should not be treated as a precise estimate.

## Representative 30k Results, Approximately 200 ms RTT

| Mode | Protocol | Consensus TPS | Consensus latency | Useful-input utilization |
| --- | --- | ---: | ---: | ---: |
| Normal | Shortfin | 29,951 | 2,567 | 99.8% |
| Normal | Narwhal/Tusk | 28,105 | 7,311 | 93.7% |
| Normal | Mahi-Mahi | 27,468 | 4,746 | 91.6% |
| Normal | Wahoo | 29,678 | 1,735 | 98.9% |
| Silence | Shortfin | 29,798 | 3,577 | 99.3% |
| Silence | Narwhal/Tusk | 28,352 | 4,965 | 94.5% |
| Silence | Mahi-Mahi | 27,955 | 4,602 | 93.2% |
| Silence | Wahoo | 29,525 | 2,375 | 98.4% |
| Equivocation | Shortfin | 21,953 | 3,272 | 97.6% |
| Equivocation | Narwhal/Tusk | 21,277 | 5,046 | 94.6% |
| Equivocation | Mahi-Mahi | 20,609 | 6,598 | 91.6% |
| Equivocation | Wahoo | 22,094 | 1,642 | 98.2% |

The external netem rule was removed after the matrix. The curated CSV records
`delay_ms=100`, which is the one-way setting; the filenames state RTT 200.

## Wide-Sweep Peaks

These are local saturation probes, not WAN capacity claims. Collocation makes
the high-load curves non-monotonic because clients, workers, primaries,
RocksDB, and consensus share one host.

| Mode | Protocol | Best offered rate | Peak useful TPS | Consensus latency |
| --- | --- | ---: | ---: | ---: |
| Normal | Shortfin | 300k | 229,645 | 1,833 ms |
| Normal | Narwhal/Tusk | 180k | 153,016 | 2,703 ms |
| Normal | Mahi-Mahi | 180k | 164,508 | 2,466 ms |
| Normal | Wahoo | 240k | 115,560 | 158 ms |
| Silence | Shortfin | 240k | 212,445 | 1,377 ms |
| Silence | Narwhal/Tusk | 180k | 175,739 | 2,438 ms |
| Silence | Mahi-Mahi | 300k | 189,127 | 2,050 ms |
| Silence | Wahoo | 240k | 194,128 | 72 ms |
| Equivocation | Shortfin | 240k | 129,191 | 2,415 ms |
| Equivocation | Narwhal/Tusk | 300k | 137,077 | 2,419 ms |
| Equivocation | Mahi-Mahi | 300k | 136,856 | 2,603 ms |
| Equivocation | Wahoo | 240k | 128,981 | 165 ms |

The exact peak ordering is sensitive to local CPU scheduling. The useful result
is that every curve reaches saturation and every final point remains live; the
wide sweep identifies suitable cloud rates rather than a publishable ranking.

## Interpretation

Shortfin is strongest among the generic-proposer protocols at moderate normal
load. Piggybacking the QC on a later header avoids an independent certificate
broadcast, and at 30k it reaches 98-100% of input with lower latency than
Narwhal/Tusk and Mahi-Mahi.

Narwhal/Tusk pays the header-vote-certificate pipeline. Its strong certificate
filters equivocated branches, so attack throughput remains close to the honest
input cap, but its additional propagation stage gives consistently higher WAN
latency than Shortfin.

Mahi-Mahi avoids certificate broadcasts but retains uncertified equivocations
and evaluates a five-stage decision structure. Its attack throughput remains
healthy, but equivocation is most visible in latency: 7.53 seconds locally and
6.60 seconds at 200 ms RTT, versus Shortfin's 2.47 and 3.27 seconds.

Wahoo's fast path has the lowest latency. Its 75% normal throughput at zero
delay is a collocation artifact: it advances as soon as three authors arrive,
so the same fourth localhost author can repeatedly miss the parent set. With
realistic delay, arrival order varies and Wahoo reaches 98.9% of input. The
cloud experiment should therefore be preferred over the zero-delay number.

Silence does not make the protocols intrinsically faster. The local host runs
one fewer primary, worker, database, client, and connection set while still
meeting quorum. That resource release explains several silence points that are
faster than their no-fault controls.

## Validation

- `cargo test -p consensus -p primary -p worker -p crypto --lib --no-fail-fast`
  passed: 15 consensus, 54 primary, 8 worker, and 13 crypto tests.
- `cargo check --workspace --all-targets` passed.
- Three pre-existing Wahoo dead-code warnings remain; no new compile error or
  benchmark failure was observed.
- All curated matrices contain nonzero results for every requested protocol.
- No benchmark process or netem rule remained after the final run.

## Result Files

- `benchmark/csv_plots/local_precloud_normal_rtt0_60s_20260914_runs.csv`
- `benchmark/csv_plots/local_precloud_silence_rtt0_60s_20260914_runs.csv`
- `benchmark/csv_plots/local_precloud_equivocation_rtt0_60s_20260914_runs.csv`
- `benchmark/csv_plots/local_precloud_normal_rtt200_30k_60s_20260914_runs.csv`
- `benchmark/csv_plots/local_precloud_silence_rtt200_30k_60s_20260914_runs.csv`
- `benchmark/csv_plots/local_precloud_equivocation_rtt200_30k_60s_20260914_runs.csv`
- `benchmark/csv_plots/local_precloud_wahoo_equivocation_30k_repeats_60s_20260914_runs.csv`

## Figures

The following throughput-latency figures use the valid zero-delay wide-sweep
rows. Each marker is one 60-second measurement; lines connect points in
increasing offered-rate order. All three figures use the same linear axes.

- `benchmark/plots/precloud_20260914/local_precloud_normal_throughput_latency.pdf`
- `benchmark/plots/precloud_20260914/local_precloud_silence_throughput_latency.pdf`
- `benchmark/plots/precloud_20260914/local_precloud_equivocation_throughput_latency.pdf`

High-resolution PNG versions are stored beside the vector PDFs. The figures
can be regenerated with `benchmark/scripts/plot_precloud_latency_tps.py`.

## Pre-Cloud Recommendation

The implementation is ready for a low-cost cloud pilot. Use a small set of
rates around the local knee rather than copying the exact localhost peak. Run
one smoke point first, inspect full-interval commit spans, and only then launch
the wider matrix. Publication claims still require per-node deployment,
multiple runs, and uncertainty reporting.
