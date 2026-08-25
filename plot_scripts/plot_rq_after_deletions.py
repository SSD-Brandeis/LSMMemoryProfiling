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

base_dir = Path(__file__).resolve().parents[1] / "data" / "rq_after_deletions"
out_dir = base_dir / "plot"
os.makedirs(str(out_dir), exist_ok=True)

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

# Same formula as process_data_movement() in src/.notebooks/plot_disk-based_exp.py:
#   data_movement_bytes = tickers["rocksdb.bytes.read"] + tickers["rocksdb.bytes.written"]
#                        + tickers["rocksdb.compact.write.bytes"] + tickers["rocksdb.compact.read.bytes"]
# (each defaulting to 0 if absent from the log, matching that script's dict.get(..., 0))
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

# Parse every memtable's workload.log once, computing all metrics used by any
# of the plots below. Each metric is None when its source line is absent from
# the log, so downstream plots can each filter to the rows they need.
memtables = []
for dir_name, canonical_name in dirs:
    log_path = base_dir / dir_name / "workload.log"
    if not log_path.exists():
        continue
    content = log_path.read_text()

    tot_ns = get_int(content, r"Workload Execution Time:\s*(\d+)")
    ins_ns = get_int(content, r"Inserts Execution Time:\s*(\d+)")
    del_ns = get_int(content, r"PointDelete Execution Time:\s*(\d+)")
    rq_ns = get_int(content, r"RangeQuery Execution Time:\s*(\d+)")

    display_name = MEMTABLE_DISPLAY_NAMES.get(canonical_name, canonical_name)
    memtables.append({
        "name": latex_escape(display_name),
        "color": MEMTABLE_COLORS.get(canonical_name, "#000000"),
        "hatch": MEMTABLE_HATCHES.get(canonical_name, ""),
        "facecolor": MEMTABLE_FACECOLORS.get(canonical_name, "none"),
        "tot_tp": (3001000 / (tot_ns / 1e9)) / 1000.0 if tot_ns else None,
        "ins_tp": (2000000 / (ins_ns / 1e9)) / 1000.0 if ins_ns else None,
        "del_tp": (1000000 / (del_ns / 1e9)) / 1000.0 if del_ns else None,
        "rq_tp": (1000 / (rq_ns / 1e9)) if rq_ns else None,
        "flush": get_int(content, r"rocksdb\.db\.flush\.micros.*COUNT\s*:\s*(\d+)"),
        "compaction": get_int(content, r"rocksdb\.compaction\.times\.micros.*COUNT\s*:\s*(\d+)"),
        "data_movement": sum(get_ticker(content, t) for t in DATAMOVEMENT_TICKERS) / GB_1,
    })


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

FIG_HEIGHT_IN = 3
BAR_WIDTH = 0.85 / 9

def plot_single_category(rows, key, tick_label, ylabel, filename):
    n = len(rows)
    width = BAR_WIDTH
    yticks, ymax = nice_ticks_and_ylim(max(r[key] for r in rows))

    def draw(fig, ax):
        for i, r in enumerate(rows):
            pos = (i - (n - 1) / 2) * width
            ax.bar(pos, r[key], width, label=r["name"],
                   facecolor=r["facecolor"], edgecolor=r["color"],
                   hatch=r["hatch"], linewidth=1.5)
        ax.set_ylabel(ylabel)
        ax.set_xticks([0])
        ax.set_xticklabels([tick_label])
        ax.set_xlim(-0.5, 0.5)
        ax.set_yticks(yticks)
        ax.set_ylim(0, ymax)

    fig, ax = build_bar_figure(plt, draw, xlim_range=1, height=FIG_HEIGHT_IN)
    fig.savefig(str(out_dir / filename), bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)

def plot_multi_category(rows, keys, tick_labels, ylabel, filename, log_scale=False):
    n = len(rows)
    width = BAR_WIDTH
    x = np.arange(len(keys))

    if log_scale:
        ymax = 10 ** np.ceil(np.log10(max(max(r[k] for k in keys) for r in rows) * 1.3))
    else:
        yticks, ymax = nice_ticks_and_ylim(max(max(r[k] for k in keys) for r in rows))

    def draw(fig, ax):
        for i, r in enumerate(rows):
            offsets = x + (i - (n - 1) / 2) * width
            ax.bar(offsets, [r[k] for k in keys], width, label=r["name"],
                   facecolor=r["facecolor"], edgecolor=r["color"], hatch=r["hatch"], linewidth=1.5)
        ax.set_ylabel(ylabel)
        ax.set_xticks(x)
        ax.set_xticklabels(tick_labels)
        ax.set_xlim(-0.5, len(keys) - 0.5)
        if log_scale:
            ax.set_yscale("log")
            ax.set_ylim(1, ymax)
        else:
            ax.set_yticks(yticks)
            ax.set_ylim(0, ymax)

    fig, ax = build_bar_figure(plt, draw, xlim_range=len(keys), height=FIG_HEIGHT_IN)
    fig.savefig(str(out_dir / filename), bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)

def plot_legend(rows, filename):
    handles = [
        mpatches.Patch(facecolor=r["facecolor"], edgecolor=r["color"],
                       hatch=r["hatch"], linewidth=1.5, label=r["name"])
        for r in rows
    ]
    fig = plt.figure(figsize=(8.5, 2.4))
    leg = fig.legend(handles=handles, loc="center", ncol=4, frameon=False,
                      edgecolor="none", columnspacing=1.0, handlelength=1.6,
                      handletextpad=0.5)
    fig.canvas.draw()

    # matplotlib tiles hatch lines at a fixed physical size regardless of the
    # patch they're drawn on, so a single "-" can miss this small legend
    # swatch depending on where it lands on the page (it still shows fine on
    # the much larger bars). Draw the missing line by hand instead of
    # touching swatch size or hatch/line weight.
    for r, handle in zip(rows, leg.legendHandles):
        if r["hatch"] != "-":
            continue
        bbox = handle.get_window_extent()
        (x0, y), (x1, _) = fig.transFigure.inverted().transform(
            [(bbox.x0, bbox.y0 + bbox.height / 2), (bbox.x1, bbox.y0)])
        fig.add_artist(plt.Line2D([x0, x1], [y, y], color=r["color"],
                                   linewidth=2.0, transform=fig.transFigure))

    fig.savefig(str(out_dir / filename), bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)


# Overall throughput (insert + delete + range query) + its standalone legend.
main_rows = [d for d in memtables if d["tot_tp"] is not None]
plot_single_category(main_rows, "tot_tp", "insert + delete +\nrange query",
                      "throughput (Kops)", "plot_rq_after_deletions.pdf")
plot_legend(main_rows, "plot_rq_after_deletions_legend.pdf")

# Flush / compaction event counts (log scale).
events_rows = [d for d in memtables if d["flush"] is not None and d["compaction"] is not None]
plot_multi_category(events_rows, ["flush", "compaction"], ["flush", "compaction"], "count",
                     "plot_rq_after_deletions_events.pdf", log_scale=True)

# Total bytes moved (reads + writes + compaction I/O).
plot_single_category(memtables, "data_movement", "buffer", "data movement (GB)",
                      "plot_rq_after_deletions_datamovement.pdf")

# Insert vs. delete throughput, grouped.
insdel_rows = [d for d in memtables if d["ins_tp"] is not None and d["del_tp"] is not None]
plot_multi_category(insdel_rows, ["ins_tp", "del_tp"], ["insert", "delete"], "throughput (Kops)",
                     "plot_rq_after_deletions_insert_delete.pdf")

# Per-operation breakdown: standalone insert / delete / range-query plots.
byop_rows = [d for d in memtables
             if d["tot_tp"] is not None and d["ins_tp"] is not None
             and d["del_tp"] is not None and d["rq_tp"] is not None]
plot_single_category(byop_rows, "ins_tp", "insert", "throughput (Kops)", "plot_rq_after_deletions_insert.pdf")
plot_single_category(byop_rows, "del_tp", "delete", "throughput (Kops)", "plot_rq_after_deletions_delete.pdf")
plot_single_category(byop_rows, "rq_tp", "RQ", "throughput (ops)", "plot_rq_after_deletions_rq.pdf")
