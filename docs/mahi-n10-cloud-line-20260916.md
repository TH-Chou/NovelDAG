# Mahi-Mahi 10-node GCP line, 2026-09-16

Goal:
- Run a low-cost 10-node, f=0 Mahi-Mahi throughput/latency line on GCP.
- Use one run per point, as requested.

Environment:
- GCP project: `shortfin`
- Nodes: 10 `n2-standard-2` VMs, 2 per region across `asia-east1`, `asia-southeast1`, `europe-west1`, `us-east1`, and `us-west1`
- Disk: 25GB `pd-ssd` per VM
- NovelDAG commit checked before each run: `2814bbc2552a067aa16851e8920491e35ce3ab75`
- Protocol: `mahi_mahi`
- Faults: 0
- Duration: 50s
- Benchmark delay: 20s
- Runs per point: 1

Results:

| Input rate | Consensus TPS | Consensus latency | E2E TPS | E2E latency | Notes |
| ---: | ---: | ---: | ---: | ---: | --- |
| 30K | 27,899 | 4.012s | 27,134 | 4.986s | single run |
| 60K | 55,950 | 3.895s | 54,240 | 4.898s | single run |
| 90K | 83,889 | 3.935s | 81,583 | 4.909s | single run |
| 120K | 111,712 | 3.980s | 107,629 | 4.933s | reused earlier pilot point |
| 150K | 139,849 | 4.109s | 136,331 | 5.057s | single run |
| 180K | 167,140 | 3.976s | 162,533 | 5.013s | client analyzer reported 6 missed-target warnings |

Artifacts:
- Summary CSV: `benchmark/csv_plots/gcp-n10-f0-mahi-line-20260916-summary.csv`
- Plot script: `benchmark/scripts/plot_gcp_n10_mahi_e2e.py`
- Figure PDF: `benchmark/figures/gcp_n10_f0_mahi_e2e_tps_latency_20260916.pdf`
- Figure PNG: `benchmark/figures/gcp_n10_f0_mahi_e2e_tps_latency_20260916.png`
- Per-point CSVs:
  - `benchmark/csv_plots/gcp-n10-f0-mahi-line-20260916-r30000.csv`
  - `benchmark/csv_plots/gcp-n10-f0-mahi-line-20260916-r60000.csv`
  - `benchmark/csv_plots/gcp-n10-f0-mahi-line-20260916-r90000.csv`
  - `benchmark/csv_plots/gcp-n10-f0-mahi-line-20260916-r150000.csv`
  - `benchmark/csv_plots/gcp-n10-f0-mahi-line-20260916-r180000.csv`

Sanity checks:
- All per-point config hash checks passed.
- Log scan found no panic/fatal messages in the new run logs.
- The 180K point had 6 client missed-target warnings, so it should be treated as near the current single-run pressure boundary rather than a clean underloaded point.
- All 10 GCP VMs were confirmed `TERMINATED` after the run.

Interpretation:
- Mahi-Mahi scales almost linearly from 30K to 180K requested load in this 10-node f=0 run.
- End-to-end latency stays near 5s across the tested range, so this run does not show a sharp latency knee yet.
- Because each point is single-run, this is suitable for selecting follow-up load points and checking rough reproducibility, not for final statistically stable paper numbers.
