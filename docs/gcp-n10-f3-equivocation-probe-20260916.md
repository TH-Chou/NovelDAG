# GCP 10-Node Equivocation Probe, 2026-09-16

This probe tests the active equivocation attack on the 10-node GCP WAN testbed.
It is intended as an initial load sweep before running a wider attack curve.

## Setup

- Nodes: 10, two VMs per region across five GCP regions.
- Byzantine replicas: 3.
- Fault mode: `equivocation`.
- Input rates: 30,000 / 120,000 / 150,000 tx/s.
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
- `benchmark/csv_plots/gcp-n10-f3-equiv-shortfin-mahi-r120k-50s-20260916.csv`
- `benchmark/csv_plots/gcp-n10-f3-equiv-shortfin-mahi-r150k-50s-20260916.csv`

| Rate | Protocol | E2E TPS | E2E latency | Consensus TPS | Consensus latency | Notes |
| ---: | --- | ---: | ---: | ---: | ---: | --- |
| 30K | Shortfin | 19,240 | 4,433 ms | 19,985 | 3,435 ms | No client missed-target warnings. |
| 30K | Mahi-Mahi | 18,449 | 6,873 ms | 18,835 | 5,921 ms | 239 client missed-target warnings. |
| 120K | Shortfin | 79,317 | 8,887 ms | 82,779 | 6,666 ms | No client missed-target warnings. |
| 120K | Mahi-Mahi | 73,433 | 6,911 ms | 75,812 | 5,852 ms | 287 client missed-target warnings. |
| 150K | Shortfin | 71,302 | 22,182 ms | 73,681 | 9,612 ms | No client missed-target warnings, but latency shows overload. |
| 150K | Mahi-Mahi | 93,005 | 7,044 ms | 95,530 | 5,894 ms | 355 client missed-target warnings. |

The archived logs are under `benchmark/logs/gcp-n10-f3-equiv-shortfin-mahi-r{30k,120k,150k}-50s-20260916/` in the local workspace, but those directories are ignored by Git.

## Sanity Checks

- GCP VMs were stopped after each run; inventory confirmed 10 `TERMINATED` instances.
- One europe-west1 VM in `europe-west1-b` repeatedly failed to start and was replaced in-zone-region to `europe-west1-d`; the final layout has two europe-west1 VMs in `europe-west1-d`.
- Parsed result files now show `Committee size: 10` because active attacks keep Byzantine nodes running. Earlier parsing with `faults=3` incorrectly displayed `Committee size: 13`.
- No panic lines were found in the checked protocol logs.
- Attack instrumentation was observed:
  - 30K: Shortfin 668 equivocation-send / 3 refusal lines; Mahi-Mahi 1,184 equivocation-send / 3,742 refusal lines.
  - 120K: Shortfin 504 equivocation-send / 22 refusal lines; Mahi-Mahi 1,430 equivocation-send / 4,521 refusal lines.
  - 150K: Shortfin 404 equivocation-send / 39 refusal lines; Mahi-Mahi 1,400 equivocation-send / 4,699 refusal lines.

## Initial Interpretation

At 30K and 120K, Shortfin has slightly higher effective E2E TPS than Mahi-Mahi under equivocation. Mahi-Mahi reports missed target-rate events even at 30K, which suggests the active equivocation payload path is already stressing its data plane. At 150K, Shortfin shows overload through very high E2E latency and lower effective TPS than at 120K; Mahi-Mahi reports higher TPS than at 120K but with more missed-target warnings. Treat 150K as an overloaded pressure point rather than a stable comparison point.
