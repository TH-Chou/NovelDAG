# Wahoo 50-Node GCP 30k Probe, 2026-09-16

Goal:
- Retry a low-load 50-node, no-fault Wahoo GCP run after the 120k point showed very low throughput.

Setup:
- Commit: `6e88d9f`
- Nodes: 50, 10 per region
- Regions: `asia-east1`, `asia-southeast1`, `europe-west1`, `us-east1`, `us-west1`
- Europe placement: 9 VMs in `europe-west1-b`, 1 VM in `europe-west1-d`
- Machine type: `n2-standard-2`
- Disk: 25GB `pd-ssd`
- Protocol: `wahoo`
- Faults: 0
- Input rate: 30,000 tx/s
- Duration: 40s
- Benchmark delay: 20s

Infrastructure notes:
- Initial attempts failed before benchmark execution because a GCP ephemeral IP (`34.21.235.203`) was repeatedly unreachable over SSH.
- Replaced `noveldag-node-asia-southeast1-a-1789483067-7`.
- The same bad IP was then reassigned to `noveldag-node-asia-southeast1-a-1789483067-0`, so a temporary static IP (`34.87.109.198`) was reserved and attached to that VM.
- The GCP runner was adjusted to suppress per-node SSH command spam and wait longer for SSH readiness.

Result:
- Consensus TPS: 8,432 tx/s
- Consensus latency: 24,884 ms
- End-to-end TPS: 8,232 tx/s
- End-to-end latency: 30,873 ms
- Config hash verification: yes

Sanity checks:
- All 150 remote log files were downloaded.
- No `panic`, `fatal`, or Rust panic lines were found.
- No client `missed target` or `too late` lines were found.
- Startup logs contain many transient connection-refused warnings while the 50 primaries connect, but the primary ready barrier completed before clients started.
- All 50 GCP VMs were confirmed `TERMINATED` after the run.

Conclusion:
- This is a valid completed 50-node Wahoo point, but it is still a poor performance point: even at a 30k tx/s offered load, Wahoo only reaches about 8.2k E2E TPS with about 30.9s E2E latency.
- This supports the earlier diagnosis that the current 50-node Wahoo implementation is bottlenecked by protocol/message progression rather than only by overload at high input rates.
- Do not use this as a competitive paper result unless the paper explicitly discusses Wahoo as an unstable or expensive baseline at 50 nodes.

Artifacts:
- Summary CSV: `benchmark/csv_plots/gcp-n50-f0-wahoo-r30k-40s-retry3-20260916.csv`
- Local logs, not committed due size: `benchmark/logs/gcp-n50-f0-wahoo-r30k-40s-retry3-20260916/`
