#!/usr/bin/env python3
import csv
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import numpy as np
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

csv_path = 'csv_plots/my_runs.csv'
out_png = 'csv_plots/my_runs_same_x_consensus_tps_box_rr012345_cc543210.png'
out_svg = 'csv_plots/my_runs_same_x_consensus_tps_box_rr012345_cc543210.svg'

rows = []
with open(csv_path, newline='') as f:
    for r in csv.DictReader(f):
        rows.append({
            'fault': int(r['fault']),
            'protocol': r['protocol'],
            'val': float(r['consensus_tps']),
        })

faults_rr = [0, 1, 2, 3, 4, 5]
faults_cc = [0, 1, 2, 3, 4, 5]
rr = [[x['val'] for x in rows if x['fault'] == f and x['protocol'] == 'round_robin'] for f in faults_rr]
cc = [[x['val'] for x in rows if x['fault'] == f and x['protocol'] == 'common_coin'] for f in faults_cc]

centers = np.arange(len(faults_rr))

fig, ax = plt.subplots(figsize=(10, 5.5))

bp_rr = ax.boxplot(
    rr,
    positions=centers,
    widths=0.55,
    patch_artist=True,
    showmeans=True,
    showfliers=True,
)
for b in bp_rr['boxes']:
    b.set_facecolor('none'); b.set_edgecolor('#1f77b4'); b.set_linewidth(2.0)
for w in bp_rr['whiskers'] + bp_rr['caps'] + bp_rr['medians']:
    w.set_color('#1f77b4')

bp_cc = ax.boxplot(
    cc,
    positions=centers,
    widths=0.30,
    patch_artist=True,
    showmeans=True,
    showfliers=True,
)
for b in bp_cc['boxes']:
    b.set_facecolor('#ff7f0e'); b.set_alpha(0.35); b.set_edgecolor('#ff7f0e'); b.set_linewidth(1.8)
for w in bp_cc['whiskers'] + bp_cc['caps'] + bp_cc['medians']:
    w.set_color('#ff7f0e')

ax.set_xticks(centers)
ax.set_xticklabels([str(f) for f in faults_rr])
ax.set_xlabel('Fault Nodes (x-axis reference for round_robin)')
ax.set_ylabel('Consensus TPS')
ax.set_title('TPS Boxplot: round_robin vs common_coin 16 replicas')
ax.grid(axis='y', alpha=0.25)
ax.legend(handles=[
    Line2D([0], [0], color='#1f77b4', lw=2, label='round_robin (outer)'),
    Line2D([0], [0], color='#ff7f0e', lw=2, label='common_coin (inner)'),
])

fig.tight_layout()
fig.savefig(out_png, dpi=180)
fig.savefig(out_svg)
print('generated:', out_png, out_svg)
