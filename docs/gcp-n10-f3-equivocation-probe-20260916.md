# GCP 10-Node Equivocation Probe, 2026-09-16

This probe tests the active equivocation attack on the 10-node GCP WAN testbed.
It is intended as a first single-load check before running a wider attack curve.

## Setup

- Nodes: 10, two VMs per region across five GCP regions.
- Byzantine replicas: 3.
- Fault mode: `equivocation`.
- Input rate: 120,000 tx/s.
- Duration: 50 s.
- Protocols: Shortfin and Mahi-Mahi.
- Remote binary commit checked on VMs: `2814bbc2552a067aa16851e8920491e35ce3ab75`.
- Local benchmark-script fixes used for the run:
  - `d8404ca` supports active remote fault modes.
  - `cc6dee4` passes the GCP settings path as an absolute path.
  - `8a2db08` reparses active-fault logs with live committee size.

## Result

CSV: `benchmark/csv_plots/gcp-n10-f3-equiv-shortfin-mahi-r120k-50s-20260916.csv`

| Protocol | E2E TPS | E2E latency | Consensus TPS | Consensus latency | Notes |
| --- | ---: | ---: | ---: | ---: | --- |
| Shortfin | 79,317 | 8,887 ms | 82,779 | 6,666 ms | No client missed-target warnings. |
| Mahi-Mahi | 73,433 | 6,911 ms | 75,812 | 5,852 ms | 287 client missed-target warnings. |

The archived logs are under `benchmark/logs/gcp-n10-f3-equiv-shortfin-mahi-r120k-50s-20260916/` in the local workspace, but that directory is ignored by Git.

## Sanity Checks

- GCP VMs were stopped after the run; inventory confirmed 10 `TERMINATED` instances.
- Parsed result files now show `Committee size: 10` because active attacks keep Byzantine nodes running. Earlier parsing with `faults=3` incorrectly displayed `Committee size: 13`.
- No panic lines were found in either protocol logs.
- Attack instrumentation was observed:
  - Shortfin: 504 equivocation-send log lines and 22 Byzantine worker refusal log lines.
  - Mahi-Mahi: 1,430 equivocation-send log lines and 4,521 Byzantine worker refusal log lines.

## Initial Interpretation

At this load, Shortfin has higher effective E2E TPS than Mahi-Mahi under equivocation. Mahi-Mahi also starts missing client target-rate events, which suggests 120K is already near or beyond a stressed point for this attack mode. The lower Mahi-Mahi latency here should not be read as better overall behavior, because it is committing fewer effective transactions under a load generator that is already reporting missed target-rate events.
