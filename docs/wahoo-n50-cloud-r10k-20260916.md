# Wahoo 50-Node GCP 10k Probe, 2026-09-16

Goal:
- Check whether Wahoo's poor 50-node result is only a high-load artifact by running a much lower offered load.

Setup:
- Deployed node code: `6e88d9f`
- Local runner commit: `7fdb181`
- Nodes: 50, 10 per region
- Regions: `asia-east1`, `asia-southeast1`, `europe-west1`, `us-east1`, `us-west1`
- Europe placement: 9 VMs in `europe-west1-b`, 1 VM in `europe-west1-d`
- Machine type: `n2-standard-2`
- Disk: 25GB `pd-ssd`
- Protocol: `wahoo`
- Faults: 0
- Input rate: 10,000 tx/s
- Duration: 40s
- Benchmark delay: 20s

Infrastructure notes:
- The run used `--expected-commit=6e88d9f7aa89f1f7513525f887d5e69b9dde5025` because the deployed node binaries are still from `6e88d9f`, while the local runner has documentation/script commits after that.
- The bad GCP ephemeral IP `34.21.235.203` was assigned again during startup. It was avoided by assigning a second temporary static IP, `35.198.244.173`, to `noveldag-node-asia-southeast1-a-1789483067-6`.
- Static IP `34.87.109.198` remains attached to `noveldag-node-asia-southeast1-a-1789483067-0` from the prior 30k run.

Result:
- Consensus TPS: 3,053 tx/s
- Consensus latency: 20,282 ms
- End-to-end TPS: 2,951 tx/s
- End-to-end latency: 25,276 ms
- Config hash verification: yes

Sanity checks:
- All 150 remote log files were downloaded.
- No `panic`, `fatal`, or Rust panic lines were found.
- No client `missed target` or `too late` lines were found.
- All 50 GCP VMs were confirmed `TERMINATED` after the run.

Conclusion:
- The 10k point confirms that Wahoo's 50-node problem is not just high-load saturation.
- At 10k offered load, Wahoo still only reaches about 2.95k E2E TPS and about 25.3s E2E latency.
- Together with the 30k point (8.23k E2E TPS / 30.9s), this suggests the current 50-node Wahoo implementation is dominated by protocol/message progression overhead at this scale.

Artifacts:
- Summary CSV: `benchmark/csv_plots/gcp-n50-f0-wahoo-r10k-40s-20260916.csv`
- Local logs, not committed due size: `benchmark/logs/gcp-n50-f0-wahoo-r10k-40s-20260916/`
