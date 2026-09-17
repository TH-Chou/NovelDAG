# GCP 10-Node f=3 Silence Combined Data, 2026-09-17

This note collects the old 10-node, 3-silent/crash-node WAN data, the new Mahi-Mahi f=3 silence line, and the new Shortfin 200K f=3 silence point into one machine-readable CSV.

## Combined CSV

- `benchmark/csv_plots/gcp-n10-f3-silence-combined-20260917.csv`

## Sources

- Old Narwhal, Shortfin, and Wahoo data comes from `paperdata/csv/wan/wan_n10_all_faults_results.csv`, filtered by `nodes=10` and `faults=3`.
- The historical protocol label `noveldag` is normalized to `shortfin`; `raw_protocol` preserves the original label.
- New Mahi-Mahi data comes from `benchmark/csv_plots/gcp-n10-f3-silence-mahi-line-20260917.csv`.
- The new Shortfin 200K point comes from `benchmark/csv_plots/gcp-n10-f3-silence-shortfin-mahi-r200k-50s-20260917.csv`.

## Rows Included

| Protocol | Rates |
| --- | --- |
| Shortfin | 30K, 60K, 90K, 120K, 150K, 180K, 200K |
| Narwhal/Tusk | 30K, 60K, 90K, 120K, 150K, 180K |
| Wahoo | 30K, 60K, 90K, 120K, 150K, 180K |
| Mahi-Mahi | 30K, 60K, 100K, 120K, 140K, 150K, 180K, 200K |

## Notes

- The old `paperdata` rows are historical crash/silent-fault WAN results and do not include a `duration_s` field in the normalized CSV.
- The new Mahi-Mahi 140K point has 2 client missed-target warnings.
- The new Mahi-Mahi 180K point has 152 client missed-target warnings and should be treated as an overload/reference point rather than a clean stable offered-load point.
- The new Shortfin 200K point has 341 client missed-target warnings, and the new Mahi-Mahi 200K point has 193. Both should be treated as overload/reference points.
