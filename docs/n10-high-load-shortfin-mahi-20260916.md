# Shortfin 190K and Mahi-Mahi 220K 10-node GCP points, 2026-09-16

Goal:
- Add two high-load 10-node single-run points:
  - Shortfin at 190K tx/s
  - Mahi-Mahi at 220K tx/s

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
| Shortfin | 190K | 149,850 | 4.478s | 144,622 | 5.517s | clients missed target rate 61 times |
| Mahi-Mahi | 220K | 181,026 | 4.592s | 175,740 | 5.840s | clients missed target rate 677 times |

Artifacts:
- `benchmark/csv_plots/gcp-n10-f0-shortfin-r190k-50s-20260916.csv`
- `benchmark/csv_plots/gcp-n10-f0-mahi_mahi-r220k-50s-20260916.csv`
- Logs:
  - `benchmark/logs/gcp-n10-f0-shortfin-r190k-50s-20260916`
  - `benchmark/logs/gcp-n10-f0-mahi_mahi-r220k-50s-20260916`

Sanity checks:
- Config hash verification passed for both points.
- Log scan found no panic/fatal messages.
- All 10 GCP VMs were confirmed `TERMINATED` after the run.

Interpretation:
- Shortfin 190K is lower than the previous 200K single-run point, despite fewer missed-target warnings. Treat it as a noisy high-load point, not a monotonic capacity estimate.
- Mahi-Mahi 220K has almost the same throughput as the previous 200K point but higher latency and many more missed-target warnings, so it is beyond the clean operating region.
