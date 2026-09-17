# GCP 10-Node f=3 Silence Line, 2026-09-17

This note records the Mahi-Mahi line for the 10-node, 3-silent-node WAN setting.

## Setup

- Nodes: 10 total, two VMs per region across five GCP regions.
- Faults: 3 silent nodes, leaving 7 active primaries/workers/clients.
- Fault mode: `silence`.
- Input rates: 30,000 / 60,000 / 100,000 / 120,000 / 150,000 tx/s.
- Duration: 50 s per point, single run per point.
- Protocol: Mahi-Mahi.
- Remote binary commit checked on VMs: `2814bbc2552a067aa16851e8920491e35ce3ab75`.

## Results

Combined CSV:

- `benchmark/csv_plots/gcp-n10-f3-silence-mahi-line-20260917.csv`

Per-point CSVs:

- `benchmark/csv_plots/gcp-n10-f3-silence-mahi-r30k-50s-20260917.csv`
- `benchmark/csv_plots/gcp-n10-f3-silence-mahi-r60k-50s-20260917.csv`
- `benchmark/csv_plots/gcp-n10-f3-silence-mahi-r100k-50s-20260917.csv`
- `benchmark/csv_plots/gcp-n10-f3-silence-mahi-r120k-50s-20260917.csv`
- `benchmark/csv_plots/gcp-n10-f3-silence-mahi-r150k-50s-20260917.csv`

| Input rate | E2E TPS | E2E latency | Consensus TPS | Consensus latency | Log check |
| ---: | ---: | ---: | ---: | ---: | --- |
| 30K | 26,455 | 6,537 ms | 27,096 | 5,318 ms | Clean |
| 60K | 52,878 | 6,391 ms | 54,974 | 5,258 ms | Clean |
| 100K | 88,026 | 6,428 ms | 91,271 | 5,265 ms | Clean |
| 120K | 105,852 | 6,498 ms | 108,921 | 5,275 ms | Clean |
| 150K | 123,681 | 9,084 ms | 128,110 | 5,357 ms | Clean |

No panic lines or client missed-target warnings were observed in the 30K/60K/100K/150K logs. The earlier 120K probe also had no panic lines or missed-target warnings.

## Interpretation

The line is directionally reasonable. Up to 120K, E2E latency stays near 6.4-6.5 s while throughput tracks the offered load after accounting for 3 silent nodes. At 150K, throughput still increases, but E2E latency rises to about 9.1 s, which suggests this is entering the higher-load region.

Compared with the active equivocation setting, silence is much less damaging for Mahi-Mahi because silent nodes remove both data producers and protocol participants. Equivocation keeps Byzantine nodes active and deliberately spends synchronization and payload resources.

## Script Note

The wrapper now skips the full-VM configuration hash check in `silence` mode. Only active nodes receive fresh run configuration, while silent VMs can retain older `.committee.json` / `.parameters.json`; checking all stopped participants after the run can report false mismatches. Result CSVs mark this as `config_hash_verified=skipped_silence` for new runs.
