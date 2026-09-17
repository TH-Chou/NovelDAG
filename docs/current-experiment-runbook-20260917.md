# Current Experiment Runbook, 2026-09-17

This is the current handoff entry for running NovelDAG/Shortfin experiments after the September 2026 protocol updates. Prefer WSL/Linux for all benchmark commands.

## Environment

Controller:
- Windows host with WSL enabled. Run benchmark commands inside WSL from `/mnt/d/Paper/DAG/NovelDAG`.
- GCP CLI is authenticated for project `shortfin`.
- SSH key used by GCP settings: `/root/.ssh/gcp_noveldag_rsa` inside WSL.
- Rust release builds and Python benchmark scripts are driven from WSL. Native PowerShell is fine for Git/status checks, but the experiment scripts use Unix tools.

GCP settings:
- Settings file: `benchmark/settings.gcp.json`
- Project: `shortfin`
- Branch deployed on VMs: `icde_shortfin_archive`
- VM type: `n2-standard-2`
- Image family: `shortfin-noveldag-25gb`
- Disk: 25GB `pd-ssd`
- SSH user: `ubuntu`
- Ports: base port `5000`; firewall rule `noveldag-benchmark`
- Current 10-node topology: 2 VMs in each of `asia-east1-a`, `asia-southeast1-a`, `europe-west1-b`, `us-east1-b`, and `us-west1-b`.
- Current retained VM state after the latest runs: 10 VMs stopped/`TERMINATED`.

The retained 10-node VMs were built from the repaired 50-node state. The main cloud code baseline used by recent runs is commit `2814bbc2552a067aa16851e8920491e35ce3ab75`; newer result commits only add local summaries, plots, docs, or logs.

## Supported Protocols And Fault Modes

Protocols:
- `shortfin`
- `mahi_mahi`
- `narwhal`
- `wahoo`

Fault modes:
- Normal/no-fault: `--faults 0 --fault-mode silence`
- Silence/crash: `--faults F --fault-mode silence`; the last `F` authorities are not started.
- Equivocation: `--faults F --fault-mode equivocation`; Byzantine authorities run, keep one progress path, and send conflicting blocks to honest authorities. Byzantine blocks are modeled as invalid or duplicate application payload and are excluded from useful TPS.

## Recommended Command Shape

Run from WSL:

```bash
cd /mnt/d/Paper/DAG/NovelDAG
python3 benchmark/scripts/run_gcp_single_point.py status \
  --settings benchmark/settings.gcp.json --nodes 10
```

Single point:

```bash
python3 benchmark/scripts/run_gcp_single_point.py run \
  --settings benchmark/settings.gcp.json \
  --nodes 10 \
  --faults 3 \
  --fault-mode silence \
  --rate 120000 \
  --duration 50 \
  --benchmark-delay 20 \
  --protocols shortfin,mahi_mahi \
  --run-id gcp-n10-f3-silence-shortfin-mahi-r120k-50s-YYYYMMDD \
  --expected-commit 2814bbc2552a067aa16851e8920491e35ce3ab75
```

Notes:
- Use `--resume` with the same `--run-id` after interruption. The wrapper skips protocols already present in that CSV.
- The wrapper starts the testbed, runs protocols sequentially, downloads logs, writes CSV summaries, and stops all VMs in a `finally` block.
- In silence mode, config hash verification is intentionally marked `skipped_silence` because silent nodes do not receive fresh configs.
- Use `status` after each run to confirm all VMs are `TERMINATED`.

Explicit stop:

```bash
python3 benchmark/scripts/run_gcp_single_point.py stop \
  --settings benchmark/settings.gcp.json --nodes 10
```

## Current Data Map

No-fault cloud results:
- Main 10/20/50-node four-protocol E2E summary: `benchmark/csv_plots/gcp-f0-n10-n20-n50-four-protocols-e2e-summary-20260916.csv`
- Figure script: `benchmark/scripts/plot_gcp_all_nodes_e2e.py`
- Figures: `benchmark/figures/gcp_f0_n10_n20_n50_four_protocols_e2e_20260916.png` and `.pdf`
- Explanation: `docs/gcp-all-nodes-four-protocol-e2e-20260916.md`

10-node, f=3 silence:
- Combined current silence table: `benchmark/csv_plots/gcp-n10-f3-silence-combined-20260917.csv`
- Mahi-Mahi line: `benchmark/csv_plots/gcp-n10-f3-silence-mahi-line-20260917.csv`
- Shortfin/Mahi-Mahi 200K overload point: `benchmark/csv_plots/gcp-n10-f3-silence-shortfin-mahi-r200k-50s-20260917.csv`
- Explanation: `docs/gcp-n10-f3-silence-combined-20260917.md` and `docs/gcp-n10-f3-silence-probe-20260917.md`

10-node, f=3 equivocation:
- Current combined attack table: `benchmark/csv_plots/gcp-n10-f3-attacks-combined-20260917.csv`
- Missing-rate table: `benchmark/csv_plots/gcp-n10-f3-attacks-missing-rates-20260917.csv`
- Figures: `benchmark/plots/gcp_20260917/gcp-n10-f3-attacks-e2e-tps-latency-20260917.png` and `.pdf`
- Explanation: `docs/gcp-n10-f3-attacks-combined-20260917.md`

Local pre-cloud validation:
- Summary doc: `docs/local-precloud-validation-20260914.md`
- CSV prefix: `benchmark/csv_plots/local_precloud_`
- Figures: `benchmark/plots/precloud_20260914/`

Raw or near-raw cloud logs:
- Parsed one-run summaries: `benchmark/logs/results/bench-*.txt`
- Some retained raw log archives are under `benchmark/gcp_bridge_logs_20260909/`, `benchmark/gcp_bridge_logs_20260910/`, and `benchmark/gcp_mahi_mahi_logs_20260910/`.
- Most large run directories under `benchmark/logs/gcp-*` are intentionally not committed unless specifically archived.

## Experiments Already Run

No-fault:
- 10-node WAN lines for Shortfin, Narwhal/Tusk, Wahoo from the legacy paper archive; Mahi-Mahi was added with current single-run GCP points.
- 20-node WAN lines for Shortfin, Narwhal/Tusk, Wahoo from the legacy paper archive; Mahi-Mahi was added with current single-run GCP points.
- 50-node WAN lines for Shortfin, Narwhal/Tusk, Mahi-Mahi, and Wahoo. Wahoo 50-node points are diagnostic and have much higher latency than the other protocols.

Fault/attack:
- 10-node f=3 silence lines: Shortfin, Narwhal/Tusk, and Wahoo from the legacy paper archive; Mahi-Mahi added at 30K, 60K, 100K, 120K, 140K, 150K, 180K, and 200K; Shortfin also has a new 200K overload point.
- 10-node f=3 equivocation: Shortfin and Mahi-Mahi at 30K, 100K, 110K, 120K, 130K, 140K, and 150K; Wahoo at 30K and 100K; Narwhal/Tusk at 30K, 100K, 120K, and 150K.

Interpretation notes:
- 200K f=3 silence points for Shortfin and Mahi-Mahi have many client missed-target warnings and should be treated as overload/reference points.
- Mahi-Mahi f=3 silence 180K is also close to overload.
- Shortfin f=3 equivocation 150K has very high E2E latency and is best read as an overloaded pressure point.

## Before Handing Off A New Run

Record:
- `git rev-parse HEAD`
- Exact command and `run-id`
- Node count, fault count, fault mode, offered rate, duration, benchmark delay, protocols
- CSV output path
- Plot output path if a plot is regenerated
- Whether `status` confirms all VMs are stopped

After meaningful changes, update `docs/agent-handoff.md` in the outer repository and commit the relevant repository.
