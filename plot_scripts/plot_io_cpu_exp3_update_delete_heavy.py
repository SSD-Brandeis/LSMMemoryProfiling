import os
import re
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[1]))
from plot_scripts.memtable_style import (
    MEMTABLE_DISPLAY_NAMES, MEMTABLE_COLORS, MEMTABLE_HATCHES, MEMTABLE_FACECOLORS,
    build_bar_figure, BAR_UNIT_WIDTH_IN,
)
from plot_scripts.style import configure_matplotlib_style

def latex_escape(s):
    return s.replace("_", "\\_").replace("%", "\\%")

configure_matplotlib_style(plt, fm)
plt.rcParams.update({
    "font.size": 15,
    "axes.labelsize": 15,
    "xtick.labelsize": 15,
    "ytick.labelsize": 15,
    "legend.fontsize": 11,
})

base_dir = Path(__file__).resolve().parents[1] / ".vstats" / "io-cpu-exp3-update-delete-heavy" / "runs"
out_dir = Path(__file__).resolve().parents[1] / "plot_scripts" / "plots"
os.makedirs(str(out_dir), exist_ok=True)

# Fixed canonical bar/legend order (AGENTS.md rule 16) -- never sort by value.
dirs = [
    ("vector-preallocated", "vector"),
    ("unsortedvector-preallocated", "unsorted_vector"),
    ("sortedvector-preallocated", "sorted_vector"),
    ("skiplist", "skiplist"),
    ("simpleskiplist", "simple_skiplist"),
    ("hashlinkedlist-H100000-X6", "hash_linklist"),
    ("hashskiplist-H100000-X6", "hash_skiplist"),
    ("hashvector-H100000-X6", "hash_vector"),
    ("art", "art"),
    ("btree", "tlx_btree"),
]

INSERTS_PRELOAD = 1000000
INSERTS_MAIN = 40000
DELETES_MAIN = 30000
UPDATES_MAIN = 30000

# workload.log's phase blocks: blocks[0]=header, blocks[1]=Phase: Preload,
# blocks[2]=end-of-run LogTreeState() dump (pre-existing, unlabeled), blocks[3]=Main (final, implicit).
def parse_phase_blocks(log_path):
    text = log_path.read_text()
    blocks = text.split("=====================")
    if len(blocks) < 4:
        return None
    return {"Preload": blocks[1], "Main": blocks[3]}

def exec_time_ns(block, field):
    m = re.search(rf"{field} Execution Time:\s*(\d+)", block)
    return int(m.group(1)) if m else None

memtable_data = []
for dir_name, canonical_name in dirs:
    log_path = base_dir / dir_name / "workload.log"
    if not log_path.exists():
        print(f"skip {dir_name}: no workload.log")
        continue
    phases = parse_phase_blocks(log_path)
    if not phases:
        print(f"skip {dir_name}: could not parse phase blocks")
        continue

    preload_ns = exec_time_ns(phases["Preload"], "Inserts")
    main_insert_ns = exec_time_ns(phases["Main"], "Inserts")
    main_update_ns = exec_time_ns(phases["Main"], "Updates")
    main_delete_ns = exec_time_ns(phases["Main"], "PointDelete")
    if None in (preload_ns, main_insert_ns, main_update_ns, main_delete_ns):
        print(f"skip {dir_name}: missing execution-time fields")
        continue

    # Overall = total ops / total wall time across ALL phases ("Workload
    # Execution Time" is each phase's own wall clock, reset at every
    # boundary -- see run_workload.cc -- so summing it across phases gives
    # the whole run's wall time, not a double count).
    preload_wall_ns = exec_time_ns(phases["Preload"], "Workload")
    main_wall_ns = exec_time_ns(phases["Main"], "Workload")
    if preload_wall_ns is None or main_wall_ns is None:
        print(f"skip {dir_name}: missing Workload Execution Time fields")
        continue
    total_ops = INSERTS_PRELOAD + INSERTS_MAIN + UPDATES_MAIN + DELETES_MAIN
    total_wall_ns = preload_wall_ns + main_wall_ns

    display_name = MEMTABLE_DISPLAY_NAMES.get(canonical_name, canonical_name)
    memtable_data.append({
        "display": latex_escape(display_name),
        "color": MEMTABLE_COLORS.get(canonical_name, "#000000"),
        "hatch": MEMTABLE_HATCHES.get(canonical_name, ""),
        "facecolor": MEMTABLE_FACECOLORS.get(canonical_name, "none"),
        "preload_kops": (INSERTS_PRELOAD / (preload_ns / 1e9)) / 1e3,
        "main_insert_kops": (INSERTS_MAIN / (main_insert_ns / 1e9)) / 1e3,
        "main_update_kops": (UPDATES_MAIN / (main_update_ns / 1e9)) / 1e3,
        "main_delete_kops": (DELETES_MAIN / (main_delete_ns / 1e9)) / 1e3,
        "overall_kops": (total_ops / (total_wall_ns / 1e9)) / 1e3,
    })

n = len(memtable_data)

# ---- Plot 1: Preload-phase bulk-insert throughput (1 category) ----
# memtable_data is already in the fixed canonical order (from `dirs`); never sort by value.
categories = ["insert"]
x = np.arange(len(categories))
width = 0.9 / n
max_val = max(d["preload_kops"] for d in memtable_data)
ymin = 1.0
ymax = 10 ** np.ceil(np.log10(max_val * 1.3))

def draw_preload(fig, ax):
    for i, d in enumerate(memtable_data):
        offsets = x + (i - (n - 1) / 2) * width
        ax.bar(offsets, [d["preload_kops"]], width, label=d["display"],
               facecolor=d["facecolor"], edgecolor=d["color"], hatch=d["hatch"], linewidth=1.5)
    ax.set_ylabel("throughput (Kops)")
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.set_xlim(-0.5, 0.5)
    ax.set_yscale("log")
    ax.set_ylim(ymin, ymax)

fig, ax = build_bar_figure(plt, draw_preload, xlim_range=len(categories), pad=0.6, height=3.6)
pdf_path = out_dir / "plot_io_cpu_exp3_preload_throughput.pdf"
fig.savefig(str(pdf_path), bbox_inches="tight", pad_inches=0.05)
print(f"Wrote {pdf_path}")

# ---- Plot 2: Main-phase throughput -- insert / update / delete (3 categories) ----
categories = [("main_insert_kops", "insert"), ("main_update_kops", "update"), ("main_delete_kops", "delete")]
x = np.arange(len(categories))
width = 0.9 / n
all_main_vals = [d[key] for d in memtable_data for key, _ in categories]
max_val = max(all_main_vals)
min_val = min(all_main_vals)
ymax = 10 ** np.ceil(np.log10(max_val * 1.3))
ymin = 1.0

def draw_main(fig, ax):
    for i, d in enumerate(memtable_data):
        offsets = x + (i - (n - 1) / 2) * width
        ax.bar(offsets, [d[key] for key, _ in categories], width, label=d["display"],
               facecolor=d["facecolor"], edgecolor=d["color"], hatch=d["hatch"], linewidth=1.5)
    ax.set_ylabel("throughput (Kops)")
    ax.set_xticks(x)
    ax.set_xticklabels([label for _, label in categories])
    ax.set_xlim(-0.5, 2.5)
    ax.set_yscale("log")
    ax.set_ylim(ymin, ymax)

fig, ax = build_bar_figure(plt, draw_main, xlim_range=len(categories), pad=0.6, height=3.6)
pdf_path = out_dir / "plot_io_cpu_exp3_main_throughput.pdf"
fig.savefig(str(pdf_path), bbox_inches="tight", pad_inches=0.05)
print(f"Wrote {pdf_path}")

# ---- Plot 3: Overall throughput -- total ops / total wall time across the
# whole run (Preload + Main), 1 category ----
categories = ["buffer"]
x = np.arange(len(categories))
width = 0.9 / n
max_val = max(d["overall_kops"] for d in memtable_data)
ymin = 1.0
ymax = 10 ** np.ceil(np.log10(max_val * 1.3))

def draw_overall(fig, ax):
    for i, d in enumerate(memtable_data):
        offsets = x + (i - (n - 1) / 2) * width
        ax.bar(offsets, [d["overall_kops"]], width, label=d["display"],
               facecolor=d["facecolor"], edgecolor=d["color"], hatch=d["hatch"], linewidth=1.5)
    ax.set_ylabel("throughput (Kops)")
    ax.set_xticks(x)
    ax.set_xticklabels(categories)
    ax.set_xlim(-0.5, 0.5)
    ax.set_yscale("log")
    ax.set_ylim(ymin, ymax)

fig, ax = build_bar_figure(plt, draw_overall, xlim_range=len(categories), pad=0.6, height=3.6,
                            unit_width_in=BAR_UNIT_WIDTH_IN * 1.4)
pdf_path = out_dir / "plot_io_cpu_exp3_overall_throughput.pdf"
fig.savefig(str(pdf_path), bbox_inches="tight", pad_inches=0.05)
print(f"Wrote {pdf_path}")

# ---- Shared standalone legend ----
handles = [
    mpatches.Patch(facecolor=d["facecolor"], edgecolor=d["color"], hatch=d["hatch"], label=d["display"])
    for d in memtable_data
]
legend_fig = plt.figure(figsize=(12, 1.2))
legend_fig.legend(handles=handles, loc="center", ncol=5, frameon=False,
                   edgecolor="none", columnspacing=1.0, handlelength=2.2, handletextpad=0.5)
legend_path = out_dir / "plot_io_cpu_exp3_legend.pdf"
legend_fig.savefig(str(legend_path), bbox_inches="tight", pad_inches=0.05)
plt.close(legend_fig)
print(f"Wrote {legend_path}")

print("\nmemtable                 preload(Kops)  insert(Kops)  update(Kops)  delete(Kops)  overall(Kops)")
for d in memtable_data:
    print(f"{d['display']:24s} {d['preload_kops']:14.2f} {d['main_insert_kops']:13.2f} {d['main_update_kops']:13.2f} {d['main_delete_kops']:13.2f} {d['overall_kops']:14.2f}")
