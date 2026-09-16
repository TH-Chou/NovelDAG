# GCP 10-Node f=3 Silence Probe, 2026-09-17

This note records the first Mahi-Mahi point for the 10-node, 3-silent-node WAN setting.

## Setup

- Nodes: 10 total, two VMs per region across five GCP regions.
- Faults: 3 silent nodes, leaving 7 active primaries/workers/clients.
- Fault mode: `silence`.
- Input rate: 120,000 tx/s.
- Duration: 50 s.
- Protocol: Mahi-Mahi.
- Remote binary commit checked on VMs: `2814bbc2552a067aa16851e8920491e35ce3ab75`.

## Result

CSV:

- `benchmark/csv_plots/gcp-n10-f3-silence-mahi-r120k-50s-20260917.csv`

| Protocol | E2E TPS | E2E latency | Consensus TPS | Consensus latency | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| Mahi-Mahi | 105,852 | 6,498 ms | 108,921 | 5,275 ms | No panic lines; no client missed-target warnings. |

## Comparison

The current workspace does not contain a saved 10-node f=3 silence CSV for the other protocols, so this is not yet a same-mode four-protocol comparison.

Nearby 120K reference points:

| Setting | Protocol | E2E TPS | E2E latency | Notes |
| --- | --- | ---: | ---: | --- |
| f=0 | Mahi-Mahi | 107,629 | 4,933 ms | `gcp_pilot_n10_f0_r120k_50s_20260914.csv` |
| f=1 silence | Mahi-Mahi | 107,333 | 5,682 ms | `gcp_pilot_n10_f1_r120k_50s_20260914.csv` |
| f=3 silence | Mahi-Mahi | 105,852 | 6,498 ms | This run. |
| f=3 equivocation | Mahi-Mahi | 73,433 | 6,911 ms | Different fault mode; active Byzantine attack. |

This first f=3 silence point is directionally reasonable: throughput remains close to f=0/f=1 because silent faults remove both data producers and protocol participants, while latency increases modestly. It is much higher than the f=3 equivocation point because equivocation keeps Byzantine nodes active and deliberately consumes synchronization and payload resources.

## Caveat

The benchmark run completed and produced complete logs for 7 active primaries, workers, and clients. The wrapper failed after log download during the full-VM configuration hash check with `Committee/parameter hashes differ across nodes: 2 variants`. In silence mode only the 7 active nodes receive fresh run configuration; the 3 silent VMs can retain an older config, so this post-run check is stricter than the actual active testbed. The saved CSV marks `config_hash_verified=active_nodes_only`.
