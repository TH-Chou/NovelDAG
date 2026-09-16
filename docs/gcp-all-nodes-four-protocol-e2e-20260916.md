# 10/20/50-node four-protocol E2E summary, 2026-09-16

Goal:
- Build one curated summary file and one total E2E TPS/latency figure for four protocols across 10, 20, and 50 nodes.

Data sources:
- 10-node Shortfin, Narwhal/Tusk, and Wahoo: `paperdata/csv/wan/wan_n10_plot_summary.csv`, f=0 rows.
- 10-node Mahi-Mahi: current GCP single-run files from `benchmark/csv_plots/gcp-n10-f0-mahi*20260916*.csv`.
- 20-node Shortfin, Narwhal/Tusk, and Wahoo: `paperdata/csv/wan/wan_n20_f0_plot_summary.csv`.
- 20-node Mahi-Mahi: current GCP single-run line in `benchmark/csv_plots/gcp-n20-f0-mahi-line-20260916-summary.csv`.
- 50-node four-protocol data: curated cleaned summary `benchmark/csv_plots/gcp-n50-f0-four-protocols-e2e-summary-20260916.csv`.

Artifacts:
- Summary CSV: `benchmark/csv_plots/gcp-f0-n10-n20-n50-four-protocols-e2e-summary-20260916.csv`
- Plot script: `benchmark/scripts/plot_gcp_all_nodes_e2e.py`
- Figure PDF: `benchmark/figures/gcp_f0_n10_n20_n50_four_protocols_e2e_20260916.pdf`
- Figure PNG: `benchmark/figures/gcp_f0_n10_n20_n50_four_protocols_e2e_20260916.png`

Coverage:
- Total rows: 82.
- 10 nodes: 8 points each for Shortfin, Narwhal/Tusk, Mahi-Mahi, and Wahoo.
- 20 nodes: 7 points each for Shortfin, Mahi-Mahi, and Wahoo; 5 points for Narwhal/Tusk because the legacy table has no 240K/260K Narwhal rows.
- 50 nodes: 6 points each for Shortfin, Narwhal/Tusk, Mahi-Mahi, and Wahoo.

Notes:
- The summary uses end-to-end throughput and latency for plotting.
- 50-node Wahoo has much higher latency than the other curves, so the 50-node panel's y-axis is much larger.
- The figure intentionally keeps Wahoo visible instead of clipping it, because those high-latency points are part of the current 50-node diagnostic data.
