# Local Four-Protocol Fault Study (2026-09-13)

## Scope

- Four collocated nodes, one worker per node, 512-byte transactions.
- Round-robin leader election, no artificial network delay.
- Main sweep: one 20-second run at 5k, 15k, 30k, 45k, and 60k offered TPS.
- Protocols: Narwhal/Tusk (`narwhal`), Shortfin, Mahi-Mahi-5, and Wahoo.
- Fault cases: no fault, one silent authority, and one equivocating authority.
- These are local shape/regression measurements, not publication-quality
  estimates. They have no repetitions or confidence intervals.

## Equivocation workload

The faulty proposer broadcasts one canonical block first so its certified
chain can continue. It then creates `n-f` additional blocks with the same
payload and parents but different signed digests. Every honest peer receives a
different variant; Byzantine peers receive and vote for every variant.

The original sweep reused valid worker batches in the Byzantine canonical and
conflicting blocks, so its equivocation TPS counted Byzantine transactions as
useful output. That accounting is retained below only as a historical raw
committed-payload result. It must not be used as the attack's effective TPS.

The corrected workload keeps those payload bytes, batch synchronization,
signature checks, DAG insertion, voting, and ordering work. Every block from
an equivocating authority is instead marked as containing execution-invalid or
duplicate transactions. The marker is covered by the block digest and
signature. Consensus may still order the block so the Byzantine chain keeps
advancing, but benchmark commit logs exclude all of its batches from useful
TPS. With n=4 and f=1, useful input is therefore capped at 75% of the displayed
offered rate.

Honest replicas retain the first-vote rule and the normal `2f+1` certificate
threshold. Therefore only the canonical block can normally become fully
certified. The variants still force signature checks, dependency and payload
checks, vote generation by Byzantine peers, network framing, storage, and DAG
indexing. Mahi-Mahi retains the variants because equivocations are part of its
uncertified DAG semantics.

## Main sweep: consensus TPS

### No fault

| Offered TPS | Narwhal/Tusk | Shortfin | Mahi-Mahi | Wahoo |
| ---: | ---: | ---: | ---: | ---: |
| 5k | 3,813 | 0* | 3,975 | 4,708 |
| 15k | 12,301 | 14,571 | 12,665 | 11,217 |
| 30k | 21,034 | 25,609 | 23,840 | 22,596 |
| 45k | 29,594 | 37,160 | 32,955 | 33,882 |
| 60k | 34,643 | 36,934 | 47,490 | 38,689 |

### One silent authority

| Offered TPS | Narwhal/Tusk | Shortfin | Mahi-Mahi | Wahoo |
| ---: | ---: | ---: | ---: | ---: |
| 5k | 4,129 | 0* | 4,040 | 4,983 |
| 15k | 12,562 | 0* | 11,819 | 15,174 |
| 30k | 25,386 | 0* | 24,335 | 30,134 |
| 45k | 38,132 | 44,318 | 35,651 | 45,329 |
| 60k | 0** | 59,519 | 47,527 | 59,760 |

### One equivocating authority (legacy raw payload TPS, superseded)

| Offered TPS | Narwhal/Tusk | Shortfin | Mahi-Mahi | Wahoo |
| ---: | ---: | ---: | ---: | ---: |
| 5k | 4,053 | 4,829 | 3,875 | 3,734 |
| 15k | 11,530 | 13,051 | 9,737 | 11,242 |
| 30k | 25,324 | 24,975 | 22,365 | 22,506 |
| 45k | 31,592 | 42,304 | 34,042 | 33,668 |
| 60k | 38,886 | 44,165 | 44,743 | 42,056 |

### One equivocating authority (corrected useful TPS)

| Offered TPS | Honest input cap | Narwhal/Tusk | Shortfin | Mahi-Mahi | Wahoo |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 30k | 22,500 | 18,770 | 17,747 | 14,233 | 22,466 |
| 45k | 33,750 | 29,415 | 26,809 | 27,349 | 33,325 |
| 60k | 45,000 | 0*** | 0*** | 35,931 | 42,891 |

The corrected 30k and 45k points use one 30-second run, except the 30k point
uses 20 seconds. Relative to the legacy 30k accounting, useful TPS decreases
by 25.9% for Narwhal/Tusk, 28.9% for Shortfin, 36.4% for Mahi-Mahi, and 0.2%
for Wahoo. Wahoo was already throughput-limited near the three-honest-worker
input cap, so honest payload fills nearly all of its committed capacity; its
attack signal appears mainly in latency and resource use rather than a further
TPS reduction.

`*` Shortfin's four-round commit chain did not finish inside the 20-second
window at low input. A 45-second check measured 4,815 TPS without faults and
4,953 / 14,951 / 29,883 TPS with one silent node at 5k / 15k / 30k. The zeroes
are measurement-window artifacts, not liveness failures.

`**` Narwhal/Tusk with one silent node repeatedly failed to advance beyond
round 1 at 60k because the three active collocated clients and workers starved
the quorum-critical primary tasks. It reached 44,545 TPS at 50k and 49,427 TPS
at 55k. Treat 60k as the local-machine overload cliff.

`***` Under the heavier corrected equivocation workload, collocated Narwhal
and Shortfin did not finish a commit inside the 60k/30-second measurement
window. Both make progress at 45k, so these zeroes are another local CPU
scheduling/measurement-window cliff rather than evidence of protocol-level
liveness failure.

## High-load paired checks

These older 30-second checks compare active equivocation with four normally
active authorities, but their attack TPS includes Byzantine payload and is now
superseded for useful-throughput claims:

| Protocol | No-fault TPS | Equivocation TPS | TPS change | No-fault latency | Equivocation latency |
| --- | ---: | ---: | ---: | ---: | ---: |
| Narwhal/Tusk | 43,663 | 42,171 | -3.4% | 4,921 ms | 4,331 ms |
| Shortfin | 51,531 | 37,602 | -27.0% | 2,844 ms | 2,880 ms |
| Mahi-Mahi | 41,099 | 47,638 | +15.9%* | 5,025 ms | 5,180 ms |
| Wahoo | 43,227 | 38,849 | -10.1% | 96 ms | 213 ms |

`*` Mahi-Mahi's single paired TPS result is dominated by local variance. The
20-second broad sweep instead changed from 47,490 to 44,743 TPS (-5.8%). Its
more consistent attack signal is latency: at 45k, consensus latency rose from
4,773 ms to 8,644 ms (+81%).

## Interpretation

Shortfin is strongest through most of the normal 15k-45k range because its QC
is piggybacked on the next header instead of being independently broadcast.
Its shorter pipeline also gives roughly 2.5-3.7 second local consensus latency,
versus about 4-5 seconds for Narwhal/Tusk and Mahi-Mahi. At saturation,
equivocation is expensive for Shortfin because every signed variant traverses
reference checks, embedded-QC validation, payload state, and bubble/QC state;
the 30-second paired run lost 27% TPS.

Narwhal/Tusk pays the normal header-vote-certificate pipeline and therefore
trails Shortfin at moderate load. Its equivocation variants cannot become
certificates after honest replicas spend their one vote on the canonical
header, so most attack work stays in primary validation. This explains the
small 3.4% high-load paired loss.

Mahi-Mahi avoids vote and certificate broadcasts, but its five-stage decision
rule waits longer than Shortfin. It also intentionally retains multiple blocks
per author-round and evaluates deterministic support through the uncertified
DAG. The corrected 30k test reaches 14.2k useful TPS rather than the legacy
22.4k: Byzantine payload no longer masks the cost of retaining and traversing
the conflicting uncertified DAG.

Wahoo's local fast path advances without the one-second header timer used by
the generic proposer, explaining its very low localhost consensus latency.
Its PB/EPBC phases still generate multiple signed vote classes. Equivocation
therefore doubles the 60k paired latency and lowers TPS by about 10%, while the
canonical first-writer rule keeps the protocol advancing.

The apparent gains under silence are not protocol speedups. With only three
active collocated authorities, the machine runs fewer primaries, workers,
RocksDB instances, connections, and signatures while still meeting the
three-vote quorum. Per-node cloud deployment should remove most of this local
resource-release effect.

## Result files

- `benchmark/csv_plots/local_n4_four_protocols_f0_sweep_20260912_runs.csv`
- `benchmark/csv_plots/local_n4_four_protocols_silence_sweep_20260912_runs.csv`
- `benchmark/csv_plots/local_n4_four_protocols_equivocation_sweep_20260913_runs.csv`
- `benchmark/csv_plots/local_n4_shortfin_silence_lowrate_45s_20260913_runs.csv`
- `benchmark/csv_plots/local_n4_narwhal_silence_cliff_30s_20260913_runs.csv`
- `benchmark/csv_plots/local_n4_nsm_f0_highrate_30s_20260913_runs.csv`
- `benchmark/csv_plots/local_n4_nsm_equivocation_highrate_30s_20260913_runs.csv`
- `benchmark/csv_plots/local_n4_wahoo_f0_highrate_30s_20260913_runs.csv`
- `benchmark/csv_plots/local_n4_wahoo_equivocation_highrate_30s_20260913_runs.csv`
- `benchmark/csv_plots/local_n4_equivocation_invalid_tps_30k_20260913_runs.csv`
- `benchmark/csv_plots/local_n4_equivocation_invalid_tps_45k_20260913_runs.csv`
- `benchmark/csv_plots/local_n4_equivocation_invalid_tps_60k_20260913_runs.csv`
