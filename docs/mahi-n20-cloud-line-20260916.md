# Mahi-Mahi 20-node GCP line, 2026-09-16

Goal:
- Complete the Mahi-Mahi 20-node, f=0 line at the same offered-load points as the existing legacy 20-node WAN data.
- Use one run per point.

Environment:
- GCP project: `shortfin`
- Nodes: 20 `n2-standard-2` VMs, 4 per region across `asia-east1`, `asia-southeast1`, `europe-west1`, `us-east1`, and `us-west1`
- Disk: 25GB `pd-ssd` per VM
- Remote binary/source commit verified on all 20 VMs: `2814bbc2552a067aa16851e8920491e35ce3ab75`
- Protocol: `mahi_mahi`
- Faults: 0
- Duration: 50s
- Benchmark delay: 20s
- Runs per point: 1

Results:

| Input rate | Consensus TPS | Consensus latency | E2E TPS | E2E latency | Client missed-target warnings | Note |
| ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 60K | 54,672 | 3.987s | 52,758 | 4.935s | 0 | clean |
| 120K | 110,181 | 3.990s | 108,061 | 5.014s | 0 | clean |
| 180K | 165,899 | 4.014s | 162,286 | 5.028s | 2 | near clean upper range |
| 200K | 180,156 | 4.049s | 176,104 | 5.109s | 45 | pressure point |
| 220K | 179,407 | 4.079s | 174,998 | 5.332s | 647 | overloaded |
| 240K | 180,787 | 4.180s | 176,659 | 5.723s | 1,303 | overloaded |
| 260K | 171,233 | 3.984s | 167,117 | 5.432s | 1,573 | overloaded |

Comparison against existing legacy 20-node f=0 E2E TPS:

| Input rate | Narwhal/Tusk | Shortfin legacy | Wahoo legacy | Mahi-Mahi current |
| ---: | ---: | ---: | ---: | ---: |
| 60K | 44,926 | 47,003 | 54,511 | 52,758 |
| 120K | 79,847 | 98,427 | 108,905 | 108,061 |
| 180K | 115,424 | 164,216 | 164,606 | 162,286 |
| 200K | 148,032 | 169,651 | 176,167 | 176,104 |
| 220K | 132,264 | 190,832 | 181,489 | 174,998 |
| 240K | n/a | 191,336 | 182,826 | 176,659 |
| 260K | n/a | 202,717 | 174,323 | 167,117 |

Sanity checks:
- Config hash verification passed for every point.
- Log scan found no panic/fatal messages.
- All 20 GCP VMs were confirmed `TERMINATED` after the run.

Interpretation:
- Mahi-Mahi scales cleanly through 180K and reaches about 176K E2E TPS around 200K.
- 220K and above are overloaded: throughput plateaus or falls while missed-target warnings rise sharply.
- The clean knee is around 180K-200K for this single-run 20-node setup.
- Relative to legacy baselines, Mahi-Mahi is close to Wahoo and Shortfin through 200K on throughput, but its latency stays around 5s and does not match Wahoo's lower-latency legacy points.

Artifacts:
- Summary CSV: `benchmark/csv_plots/gcp-n20-f0-mahi-line-20260916-summary.csv`
- Per-point CSVs:
  - `benchmark/csv_plots/gcp-n20-f0-mahi-line-20260916-r60000.csv`
  - `benchmark/csv_plots/gcp-n20-f0-mahi_mahi-r120k-50s-20260916.csv`
  - `benchmark/csv_plots/gcp-n20-f0-mahi-line-20260916-r180000.csv`
  - `benchmark/csv_plots/gcp-n20-f0-mahi-line-20260916-r200000.csv`
  - `benchmark/csv_plots/gcp-n20-f0-mahi-line-20260916-r220000.csv`
  - `benchmark/csv_plots/gcp-n20-f0-mahi-line-20260916-r240000.csv`
  - `benchmark/csv_plots/gcp-n20-f0-mahi-line-20260916-r260000.csv`
