# Wahoo 50-Node GCP 20k Probe, 2026-09-16

Goal:
- Add a 20k offered-load point for the 50-node, no-fault Wahoo GCP probe series.

Setup:
- Deployed node code: `6e88d9f`
- Local runner commit: `ece5562`
- Nodes: 50, 10 per region
- Regions: `asia-east1`, `asia-southeast1`, `europe-west1`, `us-east1`, `us-west1`
- Europe placement: 9 VMs in `europe-west1-b`, 1 VM in `europe-west1-d`
- Machine type: `n2-standard-2`
- Disk: 25GB `pd-ssd`
- Protocol: `wahoo`
- Faults: 0
- Input rate: 20,000 tx/s
- Duration: 40s
- Benchmark delay: 20s

Infrastructure notes:
- The run used `--expected-commit=6e88d9f7aa89f1f7513525f887d5e69b9dde5025` because deployed node binaries are still from `6e88d9f`, while the local runner has later documentation/script commits.
- The recurring bad GCP ephemeral IP `34.21.235.203` appeared again on `noveldag-node-asia-southeast1-a-1789483067-2`. A temporary static IP, `35.247.180.120`, was reserved and attached to avoid it.
- Temporary static IPs now attached to stopped VMs in `asia-southeast1`: `34.87.109.198`, `34.87.80.138`, `35.247.180.120`, and `35.198.244.173`.

Result:
- Consensus TPS: 5,362 tx/s
- Consensus latency: 24,894 ms
- End-to-end TPS: 5,105 tx/s
- End-to-end latency: 30,061 ms
- Config hash verification: yes

Sanity checks:
- All 150 remote log files were downloaded.
- No `panic`, `fatal`, or Rust panic lines were found.
- No client `missed target` or `too late` lines were found.
- All 50 GCP VMs were confirmed `TERMINATED` after the run.

Comparison:

| Offered load | Consensus TPS | Consensus latency | E2E TPS | E2E latency |
| ---: | ---: | ---: | ---: | ---: |
| 10k | 3,053 | 20.282s | 2,951 | 25.276s |
| 20k | 5,362 | 24.894s | 5,105 | 30.061s |
| 30k | 8,432 | 24.884s | 8,232 | 30.873s |
| 50k | 4,486 | 22.895s | 4,285 | 25.950s |
| 120k | 4,902 | 33.080s | 4,330 | 35.930s |

Conclusion:
- This is a valid completed point, but it remains far below the offered load.
- The 20k point fits the broader diagnosis that Wahoo's current 50-node implementation is unstable and dominated by protocol/message progression overhead at this scale.
- The 30k point remains the best observed point in this probe series, while 50k and 120k fall back to around 4.3k E2E TPS.

Artifacts:
- Summary CSV: `benchmark/csv_plots/gcp-n50-f0-wahoo-r20k-40s-20260916.csv`
- Local logs, not committed due size: `benchmark/logs/gcp-n50-f0-wahoo-r20k-40s-20260916/`
