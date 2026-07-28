#!/usr/bin/env python3

import argparse
import csv
from collections import defaultdict
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

REPO_ROOT = Path(__file__).resolve().parents[1]
R = REPO_ROOT / "data" / "memtable_scalability_vs_threads_ondisk_small_l2"
OUT = R / "plots"
FONT_PATH = REPO_ROOT / "LinLibertine_Mah.ttf"

THREAD_COUNTS = [1, 2, 4, 8, 16]
MAX_WRITE_BUFFER_NUMBER = 8

ALL_MEMTABLES = ["skiplist", "simple_skiplist", "vector", "unsorted_vector",
                 "sorted_vector", "art", "tlx_btree"]
WRITE_MEMTABLES = ALL_MEMTABLES
READ_MEMTABLES = ALL_MEMTABLES
MIXED_MEMTABLES = ALL_MEMTABLES

MEMTABLE_PALETTE = {
    "skiplist": "#2a78d6",
    "simple_skiplist": "#008300",
    "vector": "#e87ba4",
    "unsorted_vector": "#eda100",
    "sorted_vector": "#8a5a2b",
    "art": "#1baf7a",
    "tlx_btree": "#4a3aa7",
}

fm.fontManager.addfont(str(FONT_PATH))
FONT_NAME = fm.FontProperties(fname=str(FONT_PATH)).get_name()

plt.rcParams.update({
    "text.usetex": True,
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


def load_sweep_by_uw(path):
    """{unordered_write (0/1): ({memtable: {threads: ops/s}}, bg_jobs)} --
    used for write and mixed, both of which sweep unordered_write."""
    out = {}
    for row in csv.DictReader(open(path)):
        uw = int(row["unordered_write"])
        d, _ = out.setdefault(uw, (defaultdict(dict), row["max_background_jobs"]))
        d[row["memtable"]][int(row["threads"])] = float(row["ops_per_sec"])
    return out


def load_read(path):
    """{memtable: {threads: ops/s}} -- read has no bg_jobs/unordered_write
    sweep, so results.csv has no those columns."""
    d = defaultdict(dict)
    for row in csv.DictReader(open(path)):
        d[row["memtable"]][int(row["threads"])] = float(row["ops_per_sec"])
    return d


def style_x_axis(ax):
    ax.set_xscale("log", base=2)
    ax.set_xticks(THREAD_COUNTS)
    ax.get_xaxis().set_major_formatter(mticker.ScalarFormatter())
    ax.set_xlabel("client threads")
    ax.margins(x=0.08)


def latex_escape(s):
    return s.replace("_", r"\_").replace("%", r"\%")


def throughput_scale(max_value):
    if max_value >= 1e6:
        return 1e6, "Mops"
    if max_value >= 1e3:
        return 1e3, "Kops"
    return 1.0, "ops"


def style_y_axis_throughput(ax, values):
    scale, unit = throughput_scale(max(values))
    ax.yaxis.set_major_formatter(
        mticker.FuncFormatter(lambda x, _: f"{x / scale:.0f}" if scale == 1.0
                              else f"{x / scale:.1f}"))
    ax.set_ylabel(f"throughput ({unit})")


def plot_write_memtable(data, memtable, config_str, out_path):
    fig, ax = plt.subplots(figsize=(11, 8))
    ys = [data[t] for t in THREAD_COUNTS]
    ax.plot(THREAD_COUNTS, ys, marker="o", markersize=7, linewidth=2.2,
            color="#2a78d6")
    style_x_axis(ax)
    style_y_axis_throughput(ax, ys)
    ax.set_ylim(bottom=0)
    ax.set_title(f"on-disk 100\\% insert, {latex_escape(memtable)}: "
                 f"throughput ({config_str})")
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_write_all(data, config_str, out_path):
    fig, ax = plt.subplots(figsize=(11, 8))
    all_ys = []
    for name in WRITE_MEMTABLES:
        ys = [data[name][t] for t in THREAD_COUNTS]
        all_ys.extend(ys)
        ax.plot(THREAD_COUNTS, ys, marker="o", markersize=7, linewidth=2.2,
                color=MEMTABLE_PALETTE[name], label=latex_escape(name))
    style_x_axis(ax)
    style_y_axis_throughput(ax, all_ys)
    ax.set_ylim(0, ax.get_ylim()[1] * 1.32)
    ax.set_title(f"on-disk 100\\% insert, all memtables: "
                 f"throughput ({config_str})")
    ax.legend(loc="upper center", frameon=True, framealpha=0.9, fontsize=16,
              ncol=4, columnspacing=1.2, handletextpad=0.6, handlelength=2.2)
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_read_memtable(data, memtable, config_str, out_path):
    fig, ax = plt.subplots(figsize=(11, 8))
    ys = [data[t] for t in THREAD_COUNTS]
    ax.plot(THREAD_COUNTS, ys, marker="o", markersize=7, linewidth=2.2,
            color="#2a78d6")
    style_x_axis(ax)
    style_y_axis_throughput(ax, ys)
    ax.set_ylim(bottom=0)
    ax.set_title(f"on-disk 100\\% point query, {latex_escape(memtable)}: "
                 f"throughput ({config_str})")
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_read_all(data, config_str, out_path):
    fig, ax = plt.subplots(figsize=(11, 8))
    all_ys = []
    for name in READ_MEMTABLES:
        ys = [data[name][t] for t in THREAD_COUNTS]
        all_ys.extend(ys)
        ax.plot(THREAD_COUNTS, ys, marker="o", markersize=7, linewidth=2.2,
                color=MEMTABLE_PALETTE[name], label=latex_escape(name))
    style_x_axis(ax)
    style_y_axis_throughput(ax, all_ys)
    ax.set_ylim(0, ax.get_ylim()[1] * 1.32)
    ax.set_title(f"on-disk 100\\% point query, all memtables: "
                 f"throughput ({config_str})")
    ax.legend(loc="upper center", frameon=True, framealpha=0.9, fontsize=16,
              ncol=3, columnspacing=1.2, handletextpad=0.6, handlelength=2.2)
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_mixed_memtable(data, memtable, config_str, out_path):
    fig, ax = plt.subplots(figsize=(11, 8))
    ys = [data[t] for t in THREAD_COUNTS]
    ax.plot(THREAD_COUNTS, ys, marker="o", markersize=7, linewidth=2.2,
            color="#2a78d6")
    style_x_axis(ax)
    style_y_axis_throughput(ax, ys)
    ax.set_ylim(bottom=0)
    ax.set_title(f"on-disk 50\\% insert / 50\\% point query, "
                 f"{latex_escape(memtable)}: throughput ({config_str})")
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_mixed_all(data, config_str, out_path):
    fig, ax = plt.subplots(figsize=(11, 8))
    all_ys = []
    for name in MIXED_MEMTABLES:
        ys = [data[name][t] for t in THREAD_COUNTS]
        all_ys.extend(ys)
        ax.plot(THREAD_COUNTS, ys, marker="o", markersize=7, linewidth=2.2,
                color=MEMTABLE_PALETTE[name], label=latex_escape(name))
    style_x_axis(ax)
    style_y_axis_throughput(ax, all_ys)
    ax.set_ylim(0, ax.get_ylim()[1] * 1.32)
    ax.set_title(f"on-disk 50\\% insert / 50\\% point query, all "
                 f"memtables: throughput ({config_str})")
    ax.legend(loc="upper center", frameon=True, framealpha=0.9, fontsize=16,
              ncol=3, columnspacing=1.2, handletextpad=0.6, handlelength=2.2)
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def main():
    global R, OUT
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-root", default=None,
                        help="Override the data root (default: "
                             "data/memtable_scalability_vs_threads_ondisk_"
                             "small_l2).")
    args = parser.parse_args()
    if args.data_root:
        R = Path(args.data_root).resolve()
        OUT = R / "plots"

    OUT.mkdir(parents=True, exist_ok=True)

    write_csv = R / "write_100pct" / "results.csv"
    if write_csv.exists():
        for uw, (write, bg) in sorted(load_sweep_by_uw(write_csv).items()):
            config_str = f"cmw=1, uw={uw}, bg={bg}, mwb={MAX_WRITE_BUFFER_NUMBER}"
            out_dir = OUT / "write" / f"uw{uw}"
            out_dir.mkdir(parents=True, exist_ok=True)
            for memtable in WRITE_MEMTABLES:
                plot_write_memtable(write[memtable], memtable, config_str,
                                    out_dir / f"write_throughput_{memtable}.pdf")
            plot_write_all(write, config_str, out_dir / "write_throughput_all.pdf")
    else:
        print(f"skipping write plots: {write_csv} not found")

    read_csv = R / "read_100pct" / "results.csv"
    if read_csv.exists():
        read = load_read(read_csv)
        read_config_str = f"cmw=1, uw=0, bg=8, mwb={MAX_WRITE_BUFFER_NUMBER}"
        out_dir = OUT / "read"
        out_dir.mkdir(parents=True, exist_ok=True)
        for memtable in READ_MEMTABLES:
            plot_read_memtable(read[memtable], memtable, read_config_str,
                               out_dir / f"read_throughput_{memtable}.pdf")
        plot_read_all(read, read_config_str, out_dir / "read_throughput_all.pdf")
    else:
        print(f"skipping read plots: {read_csv} not found")

    mixed_csv = R / "mixed_50_50" / "results.csv"
    if mixed_csv.exists():
        for uw, (mixed, bg) in sorted(load_sweep_by_uw(mixed_csv).items()):
            mixed_config_str = f"cmw=1, uw={uw}, bg={bg}, mwb={MAX_WRITE_BUFFER_NUMBER}"
            out_dir = OUT / "mixed" / f"uw{uw}"
            out_dir.mkdir(parents=True, exist_ok=True)
            for memtable in MIXED_MEMTABLES:
                plot_mixed_memtable(mixed[memtable], memtable, mixed_config_str,
                                    out_dir / f"mixed_throughput_{memtable}.pdf")
            plot_mixed_all(mixed, mixed_config_str, out_dir / "mixed_throughput_all.pdf")
    else:
        print(f"skipping mixed plots: {mixed_csv} not found")


if __name__ == "__main__":
    main()
