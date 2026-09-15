# GCP 50-node no-fault load line, 2026-09-15

This note records the latest 50-node GCP no-fault runs before shrinking the testbed back to a 10-node Wahoo-debug configuration. No protocol code was changed for this record.

## Environment

- Project: `shortfin`
- Nodes: 50 replicas, 5 regions, 10 VMs per region during the run
- VM type: `n2-standard-2`
- Boot disk: 25GB `pd-ssd`
- Faults: 0
- Duration per point: 40s
- Protocols in the usable 50-node line: `mahi_mahi`, `narwhal`, `shortfin`
- Wahoo is excluded from this line because its 50-node run previously produced an invalid near-zero-throughput result and still needs debugging.

## Result files

- Raw per-rate summaries:
  - `benchmark/csv_plots/gcp-n50-f0-r30000-40s-20260915-line3.csv`
  - `benchmark/csv_plots/gcp-n50-f0-r60000-40s-20260915-line3.csv`
  - `benchmark/csv_plots/gcp-n50-f0-r90000-40s-20260915-line3.csv`
  - `benchmark/csv_plots/gcp-n50-f0-r120k-40s-20260915.csv`
  - `benchmark/csv_plots/gcp-n50-f0-r130000-40s--line3.csv`
  - `benchmark/csv_plots/gcp-n50-f0-r150000-40s-20260915-line3.csv`
  - `benchmark/csv_plots/gcp-n50-f0-r150000-40s--mahi-fill.csv`
- Merged summary: `benchmark/csv_plots/gcp-n50-f0-line3-e2e-merged-with130-20260915.csv`
- Plot: `benchmark/csv_plots/gcp-n50-f0-line3-e2e-tps-latency-with130-20260915.png`

## End-to-end summary

| Protocol | Rate | E2E TPS | E2E latency ms | Note |
| --- | ---: | ---: | ---: | --- |
| Mahi-Mahi | 30000 | 24320 | 5056 | usable |
| Mahi-Mahi | 60000 | 47399 | 5532 | usable |
| Mahi-Mahi | 90000 | 71842 | 5102 | usable |
| Mahi-Mahi | 120000 | 92248 | 5420 | usable |
| Mahi-Mahi | 130000 | 103714 | 5384 | high-load point; plausible but should be repeated before paper use |
| Mahi-Mahi | 150000 | 72047 | 5622 | unstable/overload diagnostic; client missed target several times |
| Narwhal/Tusk | 30000 | 17670 | 6364 | usable but under target at low load |
| Narwhal/Tusk | 60000 | 45874 | 5716 | usable |
| Narwhal/Tusk | 90000 | 65501 | 6054 | usable |
| Narwhal/Tusk | 120000 | 93205 | 6083 | usable |
| Narwhal/Tusk | 130000 | 89915 | 6821 | high-load point; client missed target often |
| Narwhal/Tusk | 150000 | 104999 | 5857 | high-load point; should be repeated before paper use |
| Shortfin | 30000 | 22191 | 4952 | usable |
| Shortfin | 60000 | 49167 | 5440 | usable |
| Shortfin | 90000 | 65040 | 6090 | usable |
| Shortfin | 120000 | 98912 | 5673 | most credible high-load comparison point |
| Shortfin | 130000 | 66931 | 7790 | suspicious; do not use without rerun |
| Shortfin | 150000 | 105982 | 12263 | overload/high-latency diagnostic; should be repeated before paper use |

## Conclusions

- The 50-node environment can run Mahi-Mahi, Narwhal/Tusk, and Shortfin and produces plausible data through roughly the 120k input-rate range.
- Around and above 130k, the measurements become noisy. Client missed-target warnings and non-monotonic throughput suggest that the system is entering an overload or cloud-network instability region.
- The 120k point is currently the cleanest high-load comparison: Shortfin has the highest E2E TPS among the three, Narwhal/Tusk is close, and Mahi-Mahi is lower.
- The 130k Shortfin point is inconsistent with both its 120k and 150k points and should be discarded or rerun before any claim.
- The 150k Mahi-Mahi fill point is also unstable and should be treated as a diagnostic, not as a main result.
- Wahoo remains the next debugging target. The cloud testbed has been reduced to a stopped 10-node configuration for lower-cost investigation.
