# GCP 10-Node f=3 Attack Comparison, 2026-09-17

This note combines the 10-node f=3 silence/crash data and f=3 equivocation data for quick gap inspection.

## Artifacts

- Combined data: `benchmark/csv_plots/gcp-n10-f3-attacks-combined-20260917.csv`
- Missing-rate table: `benchmark/csv_plots/gcp-n10-f3-attacks-missing-rates-20260917.csv`
- Figure: `benchmark/plots/gcp_20260917/gcp-n10-f3-attacks-e2e-tps-latency-20260917.png`
- Vector figure: `benchmark/plots/gcp_20260917/gcp-n10-f3-attacks-e2e-tps-latency-20260917.pdf`

## Data Sources

- Silence data:
  - Shortfin, Narwhal/Tusk, and Wahoo come from `paperdata/csv/wan/wan_n10_all_faults_results.csv`.
  - Mahi-Mahi comes from `benchmark/csv_plots/gcp-n10-f3-silence-mahi-line-20260917.csv`.
  - The new Shortfin 200K point comes from `benchmark/csv_plots/gcp-n10-f3-silence-shortfin-mahi-r200k-50s-20260917.csv`.
- Equivocation data:
  - Shortfin and Mahi-Mahi come from `benchmark/csv_plots/gcp-n10-f3-equiv-shortfin-mahi-r*.csv`.
  - Wahoo and Narwhal/Tusk come from `benchmark/csv_plots/gcp-n10-f3-equiv-wahoo-narwhal-r*.csv` and `benchmark/csv_plots/gcp-n10-f3-equiv-narwhal-r*.csv`.

## Missing Rates

Rates are compared against the union of observed rates within each attack mode.

| Attack | Protocol | Available rates | Missing rates |
| --- | --- | --- | --- |
| Silence | Shortfin | 30K, 60K, 90K, 120K, 150K, 180K, 200K | 100K, 140K |
| Silence | Narwhal/Tusk | 30K, 60K, 90K, 120K, 150K, 180K | 100K, 140K, 200K |
| Silence | Wahoo | 30K, 60K, 90K, 120K, 150K, 180K | 100K, 140K, 200K |
| Silence | Mahi-Mahi | 30K, 60K, 100K, 120K, 140K, 150K, 180K, 200K | 90K |
| Equivocation | Shortfin | 30K, 100K, 110K, 120K, 130K, 140K, 150K | none |
| Equivocation | Narwhal/Tusk | 30K, 100K, 120K, 150K | 110K, 130K, 140K |
| Equivocation | Wahoo | 30K, 100K | 110K, 120K, 130K, 140K, 150K |
| Equivocation | Mahi-Mahi | 30K, 100K, 110K, 120K, 130K, 140K, 150K | none |

## Notes

- The Mahi-Mahi silence 180K point has 152 client missed-target warnings and should be treated as an overload/reference point.
- The silence 200K points have many client missed-target warnings: Shortfin 341 and Mahi-Mahi 193. Treat both as overload/reference points.
- The Shortfin equivocation 150K point has very high E2E latency and is also best read as an overloaded pressure point.
