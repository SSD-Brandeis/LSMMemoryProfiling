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

REPO_ROOT = Path(__file__).resolve().parent
sys.path.append(str(REPO_ROOT))
from plot_scripts.memtable_style import MEMTABLE_DISPLAY_NAMES, MEMTABLE_COLORS, MEMTABLE_HATCHES, MEMTABLE_FACECOLORS, build_bar_figure
from plot_scripts.style import configure_matplotlib_style

def latex_escape(s):
    return s.replace("_", "\\_").replace("%", "\\%")

configure_matplotlib_style(plt, fm)
plt.rcParams.update({
    "font.size": 15,
    "axes.labelsize": 15,
    "xtick.labelsize": 15,
    "ytick.labelsize": 15,
    "legend.fontsize": 13,
})

out_dir = REPO_ROOT / "compaction_style_compare"
os.makedirs(str(out_dir), exist_ok=True)


CANDIDATE_POLICIES = [
    ("MinOR", REPO_ROOT / ".vstats" / "filepickingpolicy-exp" / "kMinOverlappingRatio"),
    ("RR", REPO_ROOT / ".vstats" / "filepickingpolicy-exp" / "kRoundRobin"),
    ("OLSF", REPO_ROOT / ".vstats" / "filepickingpolicy-exp" / "kOldestLargestSeqFirst"),
    ("OSSF", REPO_ROOT / ".vstats" / "filepickingpolicy-exp" / "kOldestSmallestSeqFirst"),
]

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

# Workload composition (see .vstats/diskbased-exp/workload.txt, reused as-is
# for every policy sweep -- diskbased-exp-old's workload.specs.json confirms
# the same totals): 100M inserts, 10K point queries, 1K range queries.
NUM_INSERTS = 100_000_000
NUM_POINT_QUERIES = 10_000
NUM_RANGE_QUERIES = 1_000
NUM_TOTAL_OPS = NUM_INSERTS + NUM_POINT_QUERIES + NUM_RANGE_QUERIES

# Same formula as process_data_movement() in src/.notebooks/plot_disk-based_exp.py.
GB_1 = 1024 ** 3
DATAMOVEMENT_TICKERS = [
    "rocksdb.bytes.read",
    "rocksdb.bytes.written",
    "rocksdb.compact.write.bytes",
    "rocksdb.compact.read.bytes",
]

def get_int(content, pattern):
    m = re.search(pattern, content)
    return int(m.group(1)) if m else None

def get_ticker(content, name):
    return get_int(content, re.escape(name) + r"\s+COUNT:\s*(\d+)") or 0

def parse_memtable_log(log_path):
    if not log_path.exists():
        return None
    content = log_path.read_text()
    tot_ns = get_int(content, r"Workload Execution Time:\s*(\d+)")
    if tot_ns is None:
        return None  # run still in progress / never finished
    ins_ns = get_int(content, r"Inserts Execution Time:\s*(\d+)")
    pq_ns = get_int(content, r"PointQuery Execution Time:\s*(\d+)")
    rq_ns = get_int(content, r"RangeQuery Execution Time:\s*(\d+)")
    return {
        "ins_tp": (NUM_INSERTS / (ins_ns / 1e9)) if ins_ns else None,
        "pq_tp": (NUM_POINT_QUERIES / (pq_ns / 1e9)) if pq_ns else None,
        "rq_tp": (NUM_RANGE_QUERIES / (rq_ns / 1e9)) if rq_ns else None,
        "flush": get_int(content, r"rocksdb\.db\.flush\.micros.*COUNT\s*:\s*(\d+)"),
        "compaction": get_int(content, r"rocksdb\.compaction\.times\.micros.*COUNT\s*:\s*(\d+)"),
        "data_movement": sum(get_ticker(content, t) for t in DATAMOVEMENT_TICKERS) / GB_1,
    }

METRICS = ["ins_tp", "pq_tp", "rq_tp", "flush", "compaction", "data_movement"]

# A policy only counts if every one of our 9 standard memtables finished.
policies = []
for label, policy_dir in CANDIDATE_POLICIES:
    per_memtable = {}
    complete = True
    for dir_name, canonical_name in dirs:
        row = parse_memtable_log(policy_dir / dir_name / "workload.log")
        if row is None:
            if canonical_name == "sorted_vector":
                continue  # not part of our standard 9; only diskbased-exp-old has it
            complete = False
            break
        per_memtable[canonical_name] = row
    if complete and per_memtable:
        policies.append((label, per_memtable))
    else:
        print(f"[skip] {label}: incomplete or missing data at {policy_dir}")

if len(policies) < 2:
    print("Need at least 2 complete policies to compare -- nothing to plot yet.")
    sys.exit(0)

print(f"Comparing {len(policies)} policies: {[label for label, _ in policies]}")

# Standard 9 memtables, in a fixed order, restricted to ones present under
# every policy being compared (so bar positions line up 1:1 across x-groups).
canonical_order = [c for _, c in dirs if c != "sorted_vector"]
memtable_rows = []
for canonical_name in canonical_order:
    if not all(canonical_name in per_memtable for _, per_memtable in policies):
        continue
    display_name = MEMTABLE_DISPLAY_NAMES.get(canonical_name, canonical_name)
    row = {
        "name": latex_escape(display_name),
        "color": MEMTABLE_COLORS.get(canonical_name, "#000000"),
        "hatch": MEMTABLE_HATCHES.get(canonical_name, ""),
        "facecolor": MEMTABLE_FACECOLORS.get(canonical_name, "none"),
    }
    for label, per_memtable in policies:
        for metric in METRICS:
            row[f"{label}::{metric}"] = per_memtable[canonical_name][metric]
    memtable_rows.append(row)


def nice_step(max_val, target_ticks=5):
    raw_step = max_val / target_ticks
    magnitude = 10 ** np.floor(np.log10(raw_step))
    for m in (1, 2, 2.5, 5, 10):
        step = m * magnitude
        if step >= raw_step:
            return step
    return 10 * magnitude

def nice_ticks_and_ylim(max_val, target_ticks=5):
    step = nice_step(max_val, target_ticks)
    tick_ymax = step * np.ceil(max_val / step)
    yticks = np.arange(0, tick_ymax + step / 2, step)
    return yticks, tick_ymax + step * 0.04

def plot_by_policy(rows, metric, ylabel, filename, log_scale=False, figsize=None, unit_divisor=1.0):
    """One plot per metric: x-axis = policy, grouped bars = memtable.
    unit_divisor rescales displayed values (e.g. 1_000 for K, 1_000_000 for M)
    so the y-axis doesn't need a string of trailing zeros -- pair with a
    ylabel that names the resulting unit (e.g. "(KOps)").
    """
    policy_labels = [label for label, _ in policies]
    keys = [f"{label}::{metric}" for label in policy_labels]
    n = len(rows)
    width = 0.85 / n
    x = np.arange(len(keys))

    def val(r, k):
        return r[k] / unit_divisor

    if log_scale:
        ymax = 10 ** np.ceil(np.log10(max(max(val(r, k) for k in keys) for r in rows) * 1.3))
    else:
        yticks, ymax = nice_ticks_and_ylim(max(max(val(r, k) for k in keys) for r in rows))

    def draw(fig, ax):
        for i, r in enumerate(rows):
            offsets = x + (i - (n - 1) / 2) * width
            ax.bar(offsets, [val(r, k) for k in keys], width, label=r["name"],
                   facecolor=r["facecolor"], edgecolor=r["color"], hatch=r["hatch"], linewidth=1.5)
        ax.set_ylabel(ylabel)
        ax.set_xticks(x)
        ax.set_xticklabels(policy_labels)
        ax.set_xlim(-0.5, len(keys) - 0.5)
        if log_scale:
            ax.set_yscale("log")
            ax.set_ylim(1, ymax)
        else:
            ax.set_yticks(yticks)
            ax.set_ylim(0, ymax)

    fig, ax = build_bar_figure(plt, draw, xlim_range=len(keys), figsize=figsize)
    fig.savefig(str(out_dir / filename), bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)

def plot_legend(rows, filename):
    # Legend swatches are much smaller than an actual bar. Every hatch reads
    # fine at that size except "-" (skiplist/InSkip-L), which ends up with
    # zero lines landing inside the tiny swatch -- "---" is the minimum
    # repeat count that reliably renders as exactly one horizontal line at
    # this swatch size (tested directly against this legend's figsize).
    # Legend only, canonical per-bar hatch is untouched.
    def legend_hatch(h):
        return "---" if h == "-" else h

    handles = [
        mpatches.Patch(facecolor=r["facecolor"], edgecolor=r["color"],
                       hatch=legend_hatch(r["hatch"]), linewidth=1.5, label=r["name"])
        for r in rows
    ]
    fig = plt.figure(figsize=(0.5 + 1.7 * len(handles), 0.6))
    fig.legend(handles=handles, loc="center", ncol=len(handles), frameon=False,
               edgecolor="none", columnspacing=1.0, handlelength=1.6,
               handletextpad=0.5)
    fig.canvas.draw()
    fig.savefig(str(out_dir / filename), bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)


# Policy tick labels are abbreviated (MinOR/RR/OLSF/OSSF), so width just
# needs to scale with the number of policy groups now.
compare_figsize = (4.5, 3)

plot_by_policy(memtable_rows, "ins_tp", "insert throughput (MOpS)", "plot_compare_insert_tp.pdf", figsize=compare_figsize, unit_divisor=1_000_000)
plot_by_policy(memtable_rows, "pq_tp", "PQ throughput (KOpS)", "plot_compare_pq_tp.pdf", figsize=compare_figsize, unit_divisor=1_000)
plot_by_policy(memtable_rows, "rq_tp", "RQ throughput (ops)", "plot_compare_rq_tp.pdf", figsize=compare_figsize)
plot_by_policy(memtable_rows, "flush", "flush count", "plot_compare_flush.pdf", log_scale=True, figsize=compare_figsize)
plot_by_policy(memtable_rows, "compaction", "compaction count", "plot_compare_compaction.pdf", log_scale=True, figsize=compare_figsize)
plot_by_policy(memtable_rows, "data_movement", "data movement (GB)", "plot_compare_datamovement.pdf", figsize=compare_figsize)
plot_legend(memtable_rows, "plot_compare_legend.pdf")
