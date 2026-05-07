#!/usr/bin/env python3
import csv
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import matplotlib.pyplot as plt

csv_path = 'csv_plots/consensus_fault_comparison.csv'
out_tps_png = 'csv_plots/consensus_tps_by_fault_matplotlib.png'
out_tps_svg = 'csv_plots/consensus_tps_by_fault_matplotlib.svg'
out_lat_png = 'csv_plots/consensus_latency_by_fault_matplotlib.png'
out_lat_svg = 'csv_plots/consensus_latency_by_fault_matplotlib.svg'

rows = []
with open(csv_path, newline='') as f:
    for row in csv.DictReader(f):
        row['fault'] = int(row['fault'])
        row['consensus_tps'] = float(row['consensus_tps'])
        row['consensus_latency_ms'] = float(row['consensus_latency_ms'])
        rows.append(row)

by_protocol = defaultdict(dict)
for r in rows:
    by_protocol[r['protocol']][r['fault']] = r

faults = sorted({r['fault'] for r in rows})
plt.style.use('seaborn-v0_8-whitegrid')

fig, ax = plt.subplots(figsize=(9, 5.2))
for protocol, color in [('round_robin', '#1f77b4'), ('common_coin', '#ff7f0e')]:
    ys = [by_protocol[protocol][f]['consensus_tps'] for f in faults]
    ax.plot(faults, ys, marker='o', linewidth=2.5, label=protocol, color=color)
ax.set_title('Consensus TPS vs Fault Nodes (n=16)')
ax.set_xlabel('Fault Nodes')
ax.set_ylabel('Consensus TPS')
ax.set_xticks(faults)
ax.legend()
fig.tight_layout()
fig.savefig(out_tps_png, dpi=180)
fig.savefig(out_tps_svg)
plt.close(fig)

fig, ax = plt.subplots(figsize=(9, 5.2))
for protocol, color in [('round_robin', '#1f77b4'), ('common_coin', '#ff7f0e')]:
    ys = [by_protocol[protocol][f]['consensus_latency_ms'] for f in faults]
    ax.plot(faults, ys, marker='o', linewidth=2.5, label=protocol, color=color)
ax.set_title('Consensus Latency vs Fault Nodes (n=16)')
ax.set_xlabel('Fault Nodes')
ax.set_ylabel('Consensus Latency (ms)')
ax.set_xticks(faults)
ax.legend()
fig.tight_layout()
fig.savefig(out_lat_png, dpi=180)
fig.savefig(out_lat_svg)
plt.close(fig)

print('generated:', out_tps_png, out_tps_svg, out_lat_png, out_lat_svg)
