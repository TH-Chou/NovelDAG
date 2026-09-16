# GCP 10-Node Equivocation Probe, 2026-09-16

This probe tests the active equivocation attack on the 10-node GCP WAN testbed.
It is intended as an initial load sweep before running a wider attack curve.

## Setup

- Nodes: 10, two VMs per region across five GCP regions.
- Byzantine replicas: 3.
- Fault mode: `equivocation`.
- Input rates: 30,000 / 100,000 / 110,000 / 120,000 / 130,000 / 140,000 / 150,000 tx/s.
- Duration: 50 s.
- Protocols: Shortfin and Mahi-Mahi.
- Remote binary commit checked on VMs: `2814bbc2552a067aa16851e8920491e35ce3ab75`.
- Local benchmark-script fixes used for the run:
  - `d8404ca` supports active remote fault modes.
  - `cc6dee4` passes the GCP settings path as an absolute path.
  - `8a2db08` reparses active-fault logs with live committee size.

## Result

CSVs:

- `benchmark/csv_plots/gcp-n10-f3-equiv-shortfin-mahi-r30k-50s-20260916.csv`
- `benchmark/csv_plots/gcp-n10-f3-equiv-shortfin-mahi-r100k-50s-20260916.csv`
- `benchmark/csv_plots/gcp-n10-f3-equiv-shortfin-mahi-r110k-50s-20260916.csv`
- `benchmark/csv_plots/gcp-n10-f3-equiv-shortfin-mahi-r120k-50s-20260916.csv`
- `benchmark/csv_plots/gcp-n10-f3-equiv-shortfin-mahi-r130k-50s-20260916.csv`
- `benchmark/csv_plots/gcp-n10-f3-equiv-shortfin-mahi-r140k-50s-20260916.csv`
- `benchmark/csv_plots/gcp-n10-f3-equiv-shortfin-mahi-r150k-50s-20260916.csv`

| Rate | Protocol | E2E TPS | E2E latency | Consensus TPS | Consensus latency | Notes |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 30K | Shortfin | 19,240 | 4,433 ms | 19,985 | 3,435 ms | No client missed-target warnings. |
| 30K | Mahi-Mahi | 18,449 | 6,873 ms | 18,835 | 5,921 ms | 239 client missed-target warnings. |
| 100K | Shortfin | 62,863 | 7,093 ms | 65,613 | 5,921 ms | No client missed-target warnings. |
| 100K | Mahi-Mahi | 62,087 | 6,137 ms | 64,283 | 5,168 ms | 311 client missed-target warnings. |
| 110K | Shortfin | 71,508 | 4,995 ms | 75,473 | 3,914 ms | No client missed-target warnings. |
| 110K | Mahi-Mahi | 65,391 | 11,145 ms | 67,462 | 9,974 ms | 334 client missed-target warnings. |
| 120K | Shortfin | 79,317 | 8,887 ms | 82,779 | 6,666 ms | No client missed-target warnings. |
| 120K | Mahi-Mahi | 73,433 | 6,911 ms | 75,812 | 5,852 ms | 287 client missed-target warnings. |
| 130K | Shortfin | 86,474 | 5,550 ms | 89,200 | 4,540 ms | No client missed-target warnings. |
| 130K | Mahi-Mahi | 79,140 | 6,846 ms | 81,862 | 5,852 ms | 316 client missed-target warnings. |
| 140K | Shortfin | 93,152 | 8,390 ms | 97,665 | 5,142 ms | No client missed-target warnings. |
| 140K | Mahi-Mahi | 82,600 | 8,585 ms | 85,233 | 7,423 ms | 303 client missed-target warnings. |
| 150K | Shortfin | 71,302 | 22,182 ms | 73,681 | 9,612 ms | No client missed-target warnings, but latency shows overload. |
| 150K | Mahi-Mahi | 93,005 | 7,044 ms | 95,530 | 5,894 ms | 355 client missed-target warnings. |

The archived logs are under `benchmark/logs/gcp-n10-f3-equiv-shortfin-mahi-r{30k,100k,110k,120k,130k,140k,150k}-50s-20260916/` in the local workspace, but those directories are ignored by Git.

## Sanity Checks

- GCP VMs were stopped after each run; inventory confirmed 10 `TERMINATED` instances.
- One europe-west1 VM in `europe-west1-b` repeatedly failed to start and was replaced in-zone-region to `europe-west1-d`; the final layout has two europe-west1 VMs in `europe-west1-d`.
- Parsed result files now show `Committee size: 10` because active attacks keep Byzantine nodes running. Earlier parsing with `faults=3` incorrectly displayed `Committee size: 13`.
- No panic lines were found in the checked protocol logs.
- Attack instrumentation was observed:
  - 30K: Shortfin 668 equivocation-send / 3 refusal lines; Mahi-Mahi 1,184 equivocation-send / 3,742 refusal lines.
  - 100K: Shortfin 100 equivocation-send / 0 refusal lines; Mahi-Mahi 1,162 equivocation-send / 4,725 refusal lines.
  - 110K: Shortfin 348 equivocation-send / 26 refusal lines; Mahi-Mahi 1,564 equivocation-send / 5,822 refusal lines.
  - 120K: Shortfin 504 equivocation-send / 22 refusal lines; Mahi-Mahi 1,430 equivocation-send / 4,521 refusal lines.
  - 130K: Shortfin 316 equivocation-send / 24 refusal lines; Mahi-Mahi 1,105 equivocation-send / 4,255 refusal lines.
  - 140K: Shortfin 100 equivocation-send / 0 refusal lines; Mahi-Mahi 1,456 equivocation-send / 4,797 refusal lines.
  - 150K: Shortfin 404 equivocation-send / 39 refusal lines; Mahi-Mahi 1,400 equivocation-send / 4,699 refusal lines.

## Initial Interpretation

From 30K through 140K, Shortfin has higher effective E2E TPS than Mahi-Mahi under equivocation in these single-run points. Mahi-Mahi reports missed target-rate events at every tested load, which suggests the active equivocation payload path is stressing its data plane earlier. At 150K, Shortfin shows overload through very high E2E latency and lower effective TPS than at 140K; Mahi-Mahi reports higher TPS than at 140K but with more missed-target warnings. Treat 150K as an overloaded pressure point rather than a stable comparison point.
