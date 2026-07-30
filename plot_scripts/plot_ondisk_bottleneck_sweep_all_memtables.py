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
R = REPO_ROOT / "data" / "ondisk_bottleneck_sweep_all_memtables"
OUT = R / "plots"
FONT_PATH = REPO_ROOT / "LinLibertine_Mah.ttf"

THREAD_COUNTS = [1, 2, 4, 8, 16]
MEMTABLES = ["skiplist", "simple_skiplist", "art", "tlx_btree", "vector",
            "unsorted_vector", "sorted_vector"]
MEMTABLE_DISPLAY_NAMES = {"tlx_btree": "b+tree"}
MEMTABLE_PALETTE = {
    "skiplist": "#2a78d6",
    "simple_skiplist": "#008300",
    "art": "#1baf7a",
    "tlx_btree": "#4a3aa7",
    "vector": "#c0392b",
    "unsorted_vector": "#e08a1e",
    "sorted_vector": "#8e44ad",
}

fm.fontManager.addfont(str(FONT_PATH))
FONT_NAME = fm.FontProperties(fname=str(FONT_PATH)).get_name()

plt.rcParams.update({
    "text.usetex": True,
    "font.family": FONT_NAME,
    "font.size": 20,
    "axes.titlesize": 18,
    "axes.labelsize": 20,
    "xtick.labelsize": 20,
    "ytick.labelsize": 20,
    "legend.fontsize": 15,
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


def style_x_axis(ax):
    ax.set_xscale("log", base=2)
    ax.set_xticks(THREAD_COUNTS)
    ax.get_xaxis().set_major_formatter(mticker.ScalarFormatter())
    ax.set_xlabel(r"\# of threads")
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


def load(path):
    """{memtable: {threads: ops/s}}"""
    d = defaultdict(dict)
    for row in csv.DictReader(open(path)):
        d[row["memtable"]][int(row["threads"])] = float(row["ops_per_sec"])
    return d


def plot_memtable(data, memtable, out_path):
    fig, ax = plt.subplots(figsize=(11, 8))
    xs = [t for t in THREAD_COUNTS if t in data[memtable]]
    ys = [data[memtable][t] for t in xs]
    ax.plot(xs, ys, marker="o", markersize=7, linewidth=2.2,
            color=MEMTABLE_PALETTE[memtable])
    style_x_axis(ax)
    style_y_axis_throughput(ax, ys)
    ax.set_ylim(bottom=0)
    ax.set_title(f"on-disk 100\\% insert, "
                 f"{latex_escape(MEMTABLE_DISPLAY_NAMES.get(memtable, memtable))}: "
                 "throughput (bg\\_jobs=8, mwb=16, buffer=128MB, uw=1, "
                 "T=10, key=128B, val=896B, ops=6M)")
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_all(data, out_path):
    fig, ax = plt.subplots(figsize=(11, 8))
    all_ys = []
    for name in MEMTABLES:
        if name not in data:
            continue
        xs = [t for t in THREAD_COUNTS if t in data[name]]
        ys = [data[name][t] for t in xs]
        all_ys.extend(ys)
        ax.plot(xs, ys, marker="o", markersize=7, linewidth=2.2,
                color=MEMTABLE_PALETTE[name],
                label=latex_escape(MEMTABLE_DISPLAY_NAMES.get(name, name)))
    style_x_axis(ax)
    style_y_axis_throughput(ax, all_ys)
    ax.set_ylim(0, ax.get_ylim()[1] * 1.32)
    ax.set_title("on-disk 100\\% insert, all memtables: throughput "
                 "(bg\\_jobs=8, mwb=16, buffer=128MB, uw=1, T=10, "
                 "key=128B, val=896B, ops=6M)")
    ax.legend(loc="upper center", frameon=True, framealpha=0.9, fontsize=14,
              ncol=4, columnspacing=1.2, handletextpad=0.6, handlelength=2.2)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    csv_path = R / "write_100pct" / "results.csv"
    data = load(csv_path)
    for name in MEMTABLES:
        if name in data:
            plot_memtable(data, name, OUT / f"write_throughput_{name}.pdf")
    plot_all(data, OUT / "write_throughput_all.pdf")


if __name__ == "__main__":
    main()
