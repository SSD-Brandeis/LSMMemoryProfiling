#!/usr/bin/env python3

import argparse
import csv
import math
import re
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.lines import Line2D

from style import line_styles

REPO_ROOT = Path(__file__).resolve().parents[1]
R = REPO_ROOT / "data" / "memtable_scalability_vs_threads_inmemory_lowpri0_uw1"
OUT = R / "plots"
FONT_PATH = REPO_ROOT / "LinLibertine_Mah.ttf"

THREAD_COUNTS = [1, 2, 4, 8, 16]

FIGSIZE = (3, 2.8)
FONT_SIZE = 13


ALL_MEMTABLES = ["skiplist", "simple_skiplist", "vector", "unsorted_vector",
                 "sorted_vector", "art", "tlx_btree"]
WRITE_MEMTABLES = ALL_MEMTABLES
READ_MEMTABLES = ALL_MEMTABLES
MIXED_MEMTABLES = ALL_MEMTABLES


#   V-Qsort    InSkip-L
#   V-Qscan    ART
#   V-Sorted   B+-tree
#   Skip-L
LEGEND_ORDER = ["vector", "unsorted_vector", "sorted_vector", "simple_skiplist",
                "skiplist", "art", "tlx_btree"]


STYLE_KEY = {
    "vector": "vector",
    "unsorted_vector": "unsortedvector",
    "sorted_vector": "alwayssortedvector",
    "skiplist": "skiplist",
    "simple_skiplist": "simpleskiplist",
    "art": "art",
    "tlx_btree": "btree",
}


def plain_label(label):
    """style.py wraps labels in \\texttt{...}, which renders legends in
    LaTeX's default Computer Modern Typewriter font -- clashing with the
    LinLibertine font enforced everywhere else in this repo. Strip the
    wrapper so the legend stays plain text, without editing style.py."""
    m = re.fullmatch(r"\\texttt\{(.*)\}", label)
    return m.group(1) if m else label


fm.fontManager.addfont(str(FONT_PATH))
FONT_NAME = fm.FontProperties(fname=str(FONT_PATH)).get_name()

plt.rcParams.update({
    "text.usetex": True,
    "font.family": FONT_NAME,
    "font.size": FONT_SIZE,
    "axes.titlesize": FONT_SIZE,
    "axes.labelsize": FONT_SIZE,
    "xtick.labelsize": FONT_SIZE,
    "ytick.labelsize": FONT_SIZE,
    "legend.fontsize": FONT_SIZE,
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


def _mean(values):
    return sum(values) / len(values)


def load_sweep_by_uw(path):
    """{unordered_write (0/1): ({memtable: {threads: ops/s}}, bg_jobs)} --
    used for write and mixed. ops/s is the mean across every rep row sharing
    the same (unordered_write, memtable, threads) key -- each rep is an
    independent run against IDENTICAL workload data (see
    run_memtable_scalability_inmemory.py), so this averages out run-to-run
    system/timing noise, not data variance."""
    raw = {}
    bg = {}
    for row in csv.DictReader(open(path)):
        uw = int(row["unordered_write"])
        bg[uw] = row["max_background_jobs"]
        raw.setdefault(uw, defaultdict(lambda: defaultdict(list)))
        raw[uw][row["memtable"]][int(row["threads"])].append(
            float(row["ops_per_sec"]))
    out = {}
    for uw, per_memtable in raw.items():
        d = defaultdict(dict)
        for memtable, per_threads in per_memtable.items():
            for threads, values in per_threads.items():
                d[memtable][threads] = _mean(values)
        out[uw] = (d, bg[uw])
    return out


def load_read(path):
    """{memtable: {threads: ops/s}} -- point-query-only throughput, meaned
    across reps (see load_sweep_by_uw docstring). The load phase's time is
    excluded by the harness itself (run_workload_multithread.cc times only
    the T-threaded query phase), so there's no "insert time" to strip out
    here."""
    raw = defaultdict(lambda: defaultdict(list))
    for row in csv.DictReader(open(path)):
        raw[row["memtable"]][int(row["threads"])].append(
            float(row["ops_per_sec"]))
    d = defaultdict(dict)
    for memtable, per_threads in raw.items():
        for threads, values in per_threads.items():
            d[memtable][threads] = _mean(values)
    return d


def style_x_axis(ax):
    ax.set_xscale("log", base=2)
    ax.set_xticks(THREAD_COUNTS)
    ax.get_xaxis().set_major_formatter(mticker.ScalarFormatter())
    ax.set_xlabel(r"\# threads")
    ax.margins(x=0.08)


def throughput_scale(max_value):
    if max_value >= 1e6:
        return 1e6, "Mops"
    if max_value >= 1e3:
        return 1e3, "Kops"
    return 1.0, "ops"


def trim_trailing_zero_decimal(s):
    """'6.0' -> '6', but '6.5' stays '6.5'."""
    return s[:-2] if s.endswith(".0") else s


def ordered_legend(ax, **kwargs):
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    order = [plain_label(line_styles[STYLE_KEY[name]]["label"]) for name in LEGEND_ORDER]
    order = [label for label in order if label in by_label]
    ax.legend([by_label[label] for label in order], order, **kwargs)


def plot_legend_only(out_path):
    """The "_all" plots (write/read/mixed) all overlay the same seven
    memtables, so one shared legend covers them all -- generated standalone
    since it's cropped from the data lines' overlap area in each plot."""
    handles, labels = [], []
    for name in LEGEND_ORDER:
        s = line_styles[STYLE_KEY[name]]
        handles.append(Line2D([], [], marker=s["marker"], markersize=s["markersize"],
                              markerfacecolor=s["markerfacecolor"], linewidth=s["linewidth"],
                              linestyle=s["linestyle"], color=s["color"]))
        labels.append(plain_label(s["label"]))
    fig = plt.figure(figsize=FIGSIZE)
    fig.legend(handles, labels, loc="center", frameon=False, fontsize=FONT_SIZE,
              ncol=2, columnspacing=1.0, handletextpad=0.5, handlelength=2.2,
              labelspacing=0.4)
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def log_power_of_ten(x, _pos):
    """Plain-LaTeX-safe replacement for LogFormatterSciNotation -- that
    formatter emits \\mathdefault{}, a mathtext-only macro that real LaTeX
    (text.usetex=True) doesn't define."""
    if x <= 0:
        return ""
    exponent = round(math.log10(x))
    return f"$10^{{{exponent}}}$"


def style_y_axis_throughput(ax, values, log_scale=False):
    if log_scale:
        ax.yaxis.set_major_locator(mticker.LogLocator(base=10))
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(log_power_of_ten))
        ax.set_ylabel("throughput (ops)")
        return
    scale, unit = throughput_scale(max(values))
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{x / scale:.0f}" if scale == 1.0
                              else trim_trailing_zero_decimal(f"{x / scale:.1f}")))
    ax.set_ylabel(f"throughput ({unit})")


# write/read/mixed "_all" plots sit side by side in the paper, so they share
# one y-scale (write's range covers all three) and only write keeps the
# y-axis label -- read/mixed drop theirs to save horizontal space.
SHARED_ALL_LOG_YTICKS = [1e0, 1e4, 1e8]


def style_shared_all_log_y(ax, show_label):
    ax.set_ylim(SHARED_ALL_LOG_YTICKS[0], SHARED_ALL_LOG_YTICKS[-1])
    ax.yaxis.set_major_locator(mticker.FixedLocator(SHARED_ALL_LOG_YTICKS))
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(log_power_of_ten))
    ax.set_ylabel("throughput (ops)" if show_label else "")


def style_y_scale(ax, log_scale):
    if log_scale:
        ax.set_yscale("log")
        ax.set_ylim(bottom=1)
    else:
        ax.set_ylim(bottom=0)


def plot_write_memtable(data, memtable, out_path, log_scale=False):
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ys = [data[t] for t in THREAD_COUNTS]
    s = line_styles[STYLE_KEY[memtable]]
    ax.plot(THREAD_COUNTS, ys, marker=s["marker"], markersize=s["markersize"],
            markerfacecolor=s["markerfacecolor"], linewidth=s["linewidth"],
            linestyle=s["linestyle"], color=s["color"])
    style_x_axis(ax)
    style_y_scale(ax, log_scale)
    style_y_axis_throughput(ax, ys, log_scale)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_write_all(data, out_path, log_scale=False):
    fig, ax = plt.subplots(figsize=FIGSIZE)
    all_ys = []
    for name in WRITE_MEMTABLES:
        ys = [data[name][t] for t in THREAD_COUNTS]
        all_ys.extend(ys)
        s = line_styles[STYLE_KEY[name]]
        ax.plot(THREAD_COUNTS, ys, marker=s["marker"], markersize=s["markersize"],
                markerfacecolor=s["markerfacecolor"], linewidth=s["linewidth"],
                linestyle=s["linestyle"], color=s["color"], label=plain_label(s["label"]))
    style_x_axis(ax)
    style_y_scale(ax, log_scale)
    style_y_axis_throughput(ax, all_ys, log_scale)
    if log_scale:
        style_shared_all_log_y(ax, show_label=True)
    # ordered_legend(ax, loc="upper left", frameon=False, fontsize=32,
    #                ncol=2, columnspacing=0.25, handletextpad=0.4, handlelength=1.7,
    #                labelspacing=0.25, borderaxespad=0.15, bbox_to_anchor=(0.0, 1.0))
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_read_memtable(data, memtable, out_path, log_scale=False):
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ys = [data[t] for t in THREAD_COUNTS]
    s = line_styles[STYLE_KEY[memtable]]
    ax.plot(THREAD_COUNTS, ys, marker=s["marker"], markersize=s["markersize"],
            markerfacecolor=s["markerfacecolor"], linewidth=s["linewidth"],
            linestyle=s["linestyle"], color=s["color"])
    style_x_axis(ax)
    style_y_scale(ax, log_scale)
    style_y_axis_throughput(ax, ys, log_scale)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_read_all(data, out_path, log_scale=False):
    fig, ax = plt.subplots(figsize=FIGSIZE)
    all_ys = []
    for name in READ_MEMTABLES:
        ys = [data[name][t] for t in THREAD_COUNTS]
        all_ys.extend(ys)
        s = line_styles[STYLE_KEY[name]]
        ax.plot(THREAD_COUNTS, ys, marker=s["marker"], markersize=s["markersize"],
                markerfacecolor=s["markerfacecolor"], linewidth=s["linewidth"],
                linestyle=s["linestyle"], color=s["color"], label=plain_label(s["label"]))
    style_x_axis(ax)
    style_y_scale(ax, log_scale)
    style_y_axis_throughput(ax, all_ys, log_scale)
    if log_scale:
        style_shared_all_log_y(ax, show_label=False)
    # ordered_legend(ax, loc="upper left", frameon=False, fontsize=32,
    #                ncol=2, columnspacing=0.7, handletextpad=0.4, handlelength=1.7,
    #                labelspacing=0.25, borderaxespad=0.15, bbox_to_anchor=(0.0, 1.0))
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_mixed_memtable(data, memtable, out_path, log_scale=False):
    fig, ax = plt.subplots(figsize=FIGSIZE)
    ys = [data[t] for t in THREAD_COUNTS]
    s = line_styles[STYLE_KEY[memtable]]
    ax.plot(THREAD_COUNTS, ys, marker=s["marker"], markersize=s["markersize"],
            markerfacecolor=s["markerfacecolor"], linewidth=s["linewidth"],
            linestyle=s["linestyle"], color=s["color"])
    style_x_axis(ax)
    style_y_scale(ax, log_scale)
    style_y_axis_throughput(ax, ys, log_scale)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_mixed_all(data, out_path, log_scale=False):
    fig, ax = plt.subplots(figsize=FIGSIZE)
    all_ys = []
    for name in MIXED_MEMTABLES:
        ys = [data[name][t] for t in THREAD_COUNTS]
        all_ys.extend(ys)
        s = line_styles[STYLE_KEY[name]]
        ax.plot(THREAD_COUNTS, ys, marker=s["marker"], markersize=s["markersize"],
                markerfacecolor=s["markerfacecolor"], linewidth=s["linewidth"],
                linestyle=s["linestyle"], color=s["color"], label=plain_label(s["label"]))
    style_x_axis(ax)
    style_y_scale(ax, log_scale)
    style_y_axis_throughput(ax, all_ys, log_scale)
    if log_scale:
        style_shared_all_log_y(ax, show_label=False)
    # ordered_legend(ax, loc="upper left", frameon=False, fontsize=32,
    #                ncol=2, columnspacing=0.7, handletextpad=0.4, handlelength=1.7,
    #                labelspacing=0.25, borderaxespad=0.15, bbox_to_anchor=(0.0, 1.0))
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def generate(out_root, log_scale):
    out_root.mkdir(parents=True, exist_ok=True)

    plot_legend_only(out_root / "legend_all.pdf")

    write_csv = R / "write_100pct" / "results.csv"
    if write_csv.exists():
        for uw, (write, bg) in sorted(load_sweep_by_uw(write_csv).items()):
            out_dir = out_root / "write" / f"uw{uw}"
            out_dir.mkdir(parents=True, exist_ok=True)
            for memtable in WRITE_MEMTABLES:
                plot_write_memtable(write[memtable], memtable,
                                    out_dir / f"write_throughput_{memtable}.pdf",
                                    log_scale)
            plot_write_all(write, out_dir / "write_throughput_all.pdf", log_scale)
    else:
        print(f"skipping write plots: {write_csv} not found")

    read_csv = R / "read_100pct" / "results.csv"
    if read_csv.exists():
        read = load_read(read_csv)
        out_dir = out_root / "read"
        out_dir.mkdir(parents=True, exist_ok=True)
        for memtable in READ_MEMTABLES:
            plot_read_memtable(read[memtable], memtable,
                               out_dir / f"read_throughput_{memtable}.pdf",
                               log_scale)
        plot_read_all(read, out_dir / "read_throughput_all.pdf", log_scale)
    else:
        print(f"skipping read plots: {read_csv} not found")

    mixed_csv = R / "mixed_50_50" / "results.csv"
    if mixed_csv.exists():
        for uw, (mixed, bg) in sorted(load_sweep_by_uw(mixed_csv).items()):
            out_dir = out_root / "mixed" / f"uw{uw}"
            out_dir.mkdir(parents=True, exist_ok=True)
            for memtable in MIXED_MEMTABLES:
                plot_mixed_memtable(mixed[memtable], memtable,
                                    out_dir / f"mixed_throughput_{memtable}.pdf",
                                    log_scale)
            plot_mixed_all(mixed, out_dir / "mixed_throughput_all.pdf", log_scale)
    else:
        print(f"skipping mixed plots: {mixed_csv} not found")


def main():
    global R, OUT
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default=None,
                        help="Override the data root (default: "
                             "data/memtable_scalability_vs_threads_inmemory_"
                             "lowpri0_uw1).")
    args = parser.parse_args()
    if args.data_root:
        R = Path(args.data_root).resolve()
        OUT = R / "plots"

    generate(OUT, log_scale=False)
    generate(R / "plot_log", log_scale=True)


if __name__ == "__main__":
    main()
