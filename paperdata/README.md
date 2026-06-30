# Paper Data Archive

This directory stores the compact data used by the Shortfin manuscript.
It intentionally keeps summarized CSV files and benchmark summary text
files. Full raw logs are not required for reading the paper figures,
although a small amount of raw data is retained where it is needed to
derive latency-breakdown statistics.

## Layout

```text
paperdata/
  csv/
    latency_breakdown/   summarized block-created-to-commit data
    region_latency/      GCP region RTT table data
    rtt/                 local RTT-sweep CSV data
    wan/                 WAN throughput/latency CSV data
  summary/
    wan/                 compact benchmark summary txt files
  log/
    latency_breakdown_raw/  raw archives used for Shortfin/Wahoo block latency
    raw_wan_by_rate/        retained raw WAN logs, not needed for normal figure checks
  plot/                  archived rendered plots from the experiment repository
  icde_shortfin_archive/ legacy archive metadata and normalized cloud CSV
```

The historical implementation label `noveldag` denotes Shortfin in the
paper. The paper labels `narwhal`, `wahoo`, and `noveldag` as Tusk,
Wahoo, and Shortfin, respectively.

## Manuscript Figure Sources

| Paper item | Main CSV source | Summary txt source |
| --- | --- | --- |
| GCP region RTT table | `csv/region_latency/gcp_region_latency_from_logs.csv`; `csv/region_latency/gcp_region_latency_table.tex` | Derived from protocol logs; no `bench-*.txt` summary format. |
| Fig. 1 WAN throughput-latency frontier | `csv/wan/wan_n10_plot_summary.csv`; `csv/wan/wan_n20_f0_plot_summary.csv` | `summary/wan/bench-*.txt` for the corresponding WAN runs. |
| Fig. 2 throughput under 6s latency | `csv/wan/wan_max_tps_under_6s_interpolated.csv` | Uses the same WAN summary txt files as Fig. 1. |
| Fig. 4 crash-fault WAN sensitivity | `csv/wan/wan_n10_plot_summary.csv` with `faults in {1,3}` | `summary/wan/bench-*.txt` for available crash-fault WAN runs. |
| Fig. 5 RTT sensitivity | `csv/rtt/rtt_avg_latency_by_fault_summary.csv`; `csv/rtt/rtt_mean_rows_used.csv` | No summary txt files are archived for the local RTT sweep. |
| Fig. 6 block-created-to-commit breakdown | `csv/latency_breakdown/latency_breakdown_narwhal_vs_shortfin_120k.csv`; `csv/latency_breakdown/latency_breakdown_summary.csv` | No `bench-*.txt` summary counterpart; Shortfin/Wahoo statistics use `log/latency_breakdown_raw/`. |

## WAN Summary Files

All compact WAN benchmark summaries are consolidated under:

```text
paperdata/summary/wan/
```

Most files follow the legacy benchmark summary naming convention:

```text
bench-FAULTS-NODES-WORKERS-COLLOCATE-RATE-TX_SIZE-PROTOCOL-runRUN.txt
```

One conflicting summary was preserved with a suffix instead of being
overwritten:

```text
summary/wan/bench-1-10-1-True-240000-512-wahoo-run1.cloud-archive.txt
```

The unsuffixed file with the same base name and the `.cloud-archive`
file are distinct runs with different metrics. The normalized legacy
cloud CSV under `icde_shortfin_archive/cloud/cloud_wan_summary.csv`
points to the `.cloud-archive` version.

## Known Gaps

- `summary/wan/bench-1-10-1-True-30000-512-wahoo-run1.txt` is not
  archived, although the corresponding row exists in
  `csv/wan/wan_n10_plot_summary.csv`.
- The current RTT CSV archive contains RTT values 0, 100, and 200 ms.
  If the manuscript figure or text uses 300 or 400 ms points, those
  summarized rows still need to be archived or the manuscript should be
  aligned to the archived data.
- Latency-breakdown data is complete at the summarized CSV level for
  the plotted metrics, but it is split across the Tusk/Shortfin CSV and
  the Shortfin/Wahoo CSV.
