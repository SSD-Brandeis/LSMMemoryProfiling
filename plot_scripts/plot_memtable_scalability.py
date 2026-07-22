#!/usr/bin/env python3

import csv
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

REPO_ROOT = Path(__file__).resolve().parents[1]
R = REPO_ROOT / "data" / "memtable_scalability_vs_threads"
OUT = R / "plots"
FONT_PATH = REPO_ROOT / "LinLibertine_Mah.ttf"

THREAD_COUNTS = [1, 2, 4, 8, 16]

MEMTABLES = ["skiplist", "simple_skiplist", "vector", "unsorted_vector",
             "sorted_vector", "art", "tlx_btree"]

# The write sweep's 6 (max_background_jobs, unordered_write) combinations.
# allow_concurrent_memtable_write is always 1 (see module docstring) but is
# still spelled out on every legend entry per the project's request that
# all three DBOptions be visible, not just the two that vary.
BG_JOBS_VALUES = [1, 8, 16]
UNORDERED_WRITE_VALUES = [False, True]

# Color encodes unordered_write (the dimension with the larger effect on
# throughput -- see ANALYSIS.md); linestyle encodes max_background_jobs.
# Both fixed, never reassigned, so the same combo is the same look in every
# one of the 7 per-memtable figures.
UW_COLOR = {False: "#2a78d6", True: "#eb6834"}  # blue / orange
BG_LINESTYLE = {1: (0, (1, 1)), 8: (0, (5, 2)), 16: "solid"}  # dotted/dashed/solid

fm.fontManager.addfont(str(FONT_PATH))
FONT_NAME = fm.FontProperties(fname=str(FONT_PATH)).get_name()

plt.rcParams.update({
    "font.family": FONT_NAME,
    "font.size": 20,
    "axes.titlesize": 20,
    "axes.labelsize": 20,
    "xtick.labelsize": 20,
    "ytick.labelsize": 20,
    "legend.fontsize": 20,
    "axes.spines.top": True,
    "axes.spines.right": True,
    "axes.spines.bottom": True,
    "axes.spines.left": True,
    "axes.grid": False,
    "xtick.direction": "out",
    "ytick.direction": "out",
    "axes.edgecolor": "black",
    "figure.facecolor": "white",
    "axes.facecolor": "white",
})


def load_read(path):
    d = defaultdict(dict)
    for row in csv.DictReader(open(path)):
        d[row["memtable"]][int(row["threads"])] = float(row["ops_per_sec"])
    return d


def load_write(path):
    """{memtable: {(bg_jobs, unordered_write): {threads: ops/s}}}"""
    d = defaultdict(lambda: defaultdict(dict))
    for row in csv.DictReader(open(path)):
        key = (int(row["max_background_jobs"]), row["unordered_write"] == "1")
        d[row["memtable"]][key][int(row["threads"])] = float(row["ops_per_sec"])
    return d


def style_x_axis(ax):
    ax.set_xscale("log", base=2)
    ax.set_xticks(THREAD_COUNTS)
    ax.get_xaxis().set_major_formatter(mticker.ScalarFormatter())
    ax.set_xlabel("client threads")
    ax.margins(x=0.08)


def plot_write_memtable(data, memtable, out_path):
    fig, ax = plt.subplots(figsize=(11, 8))
    for uw in UNORDERED_WRITE_VALUES:
        for bg in BG_JOBS_VALUES:
            v = data[(bg, uw)]
            ys = [v[t] for t in THREAD_COUNTS]
            ax.plot(THREAD_COUNTS, ys, marker="o", markersize=6,
                    linewidth=2.2, color=UW_COLOR[uw],
                    linestyle=BG_LINESTYLE[bg],
                    label=f"cmw=1, uw={int(uw)}, bg={bg}")
    style_x_axis(ax)
    ax.set_ylabel("throughput (ops/s)")
    ax.set_ylim(bottom=0)
    ax.set_title(f"100% insert, {memtable}: throughput")
    ax.legend(loc="best", frameon=True, framealpha=0.9, fontsize=15,
               ncol=2, columnspacing=1.2, handletextpad=0.6, handlelength=2.6)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


MEMTABLE_PALETTE = ["#2a78d6", "#008300", "#e87ba4", "#eda100", "#1baf7a",
                    "#eb6834", "#4a3aa7"]


def plot_read(data, out_path):
    fig, ax = plt.subplots(figsize=(11, 8))
    # Read-scenario colors: reuse the same fixed categorical order as
    # before (memtable is the varying identity here, not uw/bg -- the read
    # scenario runs a single fixed setting, named in the title below).
    for name, color in zip(MEMTABLES, MEMTABLE_PALETTE):
        v = data[name]
        ys = [v[t] for t in THREAD_COUNTS]
        ax.plot(THREAD_COUNTS, ys, marker="o", markersize=7, linewidth=2.2,
                color=color, label=name)
    style_x_axis(ax)
    ax.set_ylabel("throughput (ops/s)")
    ax.set_ylim(bottom=0)
    ax.set_title("100% read, cmw=1, uw=0, bg=8: throughput")
    ax.legend(loc="lower right", frameon=True, framealpha=0.9, fontsize=17)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_write_all(data, uw, out_path):
    """All 7 memtables on one axis, one line each, at bg_jobs=8 with
    unordered_write fixed at the given value -- bg=8 matches the read
    scenario's fixed setting, so this is directly comparable to
    read_throughput.pdf. The per-memtable files remain the source for the
    full 6-combination sweep; this view trades that detail for
    cross-memtable comparison. uw's value is stated in the title -- not
    repeated on every legend entry, which at this label length would blow
    the legend wider than the figure -- so it's never ambiguous which of
    the two files (uw=0 or uw=1) a given plot is."""
    fig, ax = plt.subplots(figsize=(11, 8))
    for name, color in zip(MEMTABLES, MEMTABLE_PALETTE):
        v = data[name][(8, uw)]
        ys = [v[t] for t in THREAD_COUNTS]
        ax.plot(THREAD_COUNTS, ys, marker="o", markersize=7, linewidth=2.2,
                color=color, label=name)
    style_x_axis(ax)
    ax.set_ylabel("throughput (ops/s)")
    # Headroom reserved at the top for the legend -- every data line stays
    # well clear of it (unlike a corner placement, which this data's
    # crossing lines and near-zero sorted_vector line make hard to do
    # without overlap).
    ax.set_ylim(0, ax.get_ylim()[1] * 1.32)
    ax.set_title(f"100% insert, cmw=1, uw={int(uw)}, bg=8: throughput")
    ax.legend(loc="upper center", frameon=True, framealpha=0.9, fontsize=16,
               ncol=4, columnspacing=1.2, handletextpad=0.6, handlelength=2.2)
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def main():
    OUT.mkdir(parents=True, exist_ok=True)

    # Superseded: relative-speedup plots and the old single combined
    # write_throughput.pdf (replaced by the uw0/uw1-split pair below).
    for stale in ["write_speedup.pdf", "write_throughput.pdf",
                  "write_speedup_uw0.pdf", "write_speedup_uw1.pdf",
                  "read_speedup.pdf"]:
        (OUT / stale).unlink(missing_ok=True)

    write = load_write(R / "write_100pct" / "results.csv")
    for memtable in MEMTABLES:
        plot_write_memtable(write[memtable], memtable,
                            OUT / f"write_throughput_{memtable}.pdf")
    plot_write_all(write, False, OUT / "write_throughput_uw0.pdf")
    plot_write_all(write, True, OUT / "write_throughput_uw1.pdf")

    read = load_read(R / "read_100pct" / "results.csv")
    plot_read(read, OUT / "read_throughput.pdf")


if __name__ == "__main__":
    main()
