# Shortfin and Mahi-Mahi 10-node 200K GCP points, 2026-09-16

Goal:
- Test whether Shortfin and Mahi-Mahi still have headroom at 10 nodes by running one 200K tx/s point for each protocol.

Environment:
- GCP project: `shortfin`
- Nodes: 10 `n2-standard-2` VMs, 2 per region across `asia-east1`, `asia-southeast1`, `europe-west1`, `us-east1`, and `us-west1`
- Faults: 0
- Duration: 50s
- Benchmark delay: 20s
- Runs per point: 1
- Expected code commit for remote binaries: `2814bbc2552a067aa16851e8920491e35ce3ab75`

Results:

| Protocol | Input rate | Consensus TPS | Consensus latency | E2E TPS | E2E latency | Analyzer warning |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| Shortfin | 200K | 177,372 | 5.915s | 173,119 | 7.381s | clients missed target rate 527 times |
| Mahi-Mahi | 200K | 181,260 | 4.200s | 176,509 | 5.362s | clients missed target rate 197 times |

Artifacts:
- `benchmark/csv_plots/gcp-n10-f0-shortfin-r200k-50s-20260916.csv`
- `benchmark/csv_plots/gcp-n10-f0-mahi_mahi-r200k-50s-20260916.csv`
- Logs:
  - `benchmark/logs/gcp-n10-f0-shortfin-r200k-50s-20260916`
  - `benchmark/logs/gcp-n10-f0-mahi_mahi-r200k-50s-20260916`

Sanity checks:
- Config hash verification passed for both points.
- Log scan found no panic/fatal messages.
- All 10 GCP VMs were confirmed `TERMINATED` after the run.

Interpretation:
- 200K is already a pressure point for this 10-node setup, because both protocols missed the target input rate.
- Shortfin's 200K point is clearly over the clean operating region: throughput increased over previous points, but latency rose to 7.381s and the client missed-target count was high.
- Mahi-Mahi also missed target at 200K, but less severely; compared with its 180K point, E2E TPS increased from 162,533 to 176,509 and E2E latency rose from 5.013s to 5.362s.
