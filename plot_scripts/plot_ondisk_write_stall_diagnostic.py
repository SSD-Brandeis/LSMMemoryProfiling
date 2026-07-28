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
R = REPO_ROOT / "data" / "ondisk_write_stall_diagnostic"
OUT = R / "plots"
FONT_PATH = REPO_ROOT / "LinLibertine_Mah.ttf"

THREAD_COUNTS = [1, 2, 4, 8, 16]
MEMTABLES = ["skiplist", "art"]
MWBN_VALUES = [2, 4, 8]
BG_VALUES = [1, 8, 16]
MWBN_COLOR = {2: "#c0392b", 4: "#e08a1e", 8: "#1a8a4a"}
BG_COLOR = {1: "#2a78d6", 8: "#7a3aa7", 16: "#1a8a4a"}

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
    "legend.fontsize": 16,
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


def load(path):
    """{(mwbn, bg): {memtable: {threads: (ops/s, stalls)}}}"""
    d = defaultdict(lambda: defaultdict(dict))
    for row in csv.DictReader(open(path)):
        key = (int(row["max_write_buffer_number"]), int(row["max_background_jobs"]))
        d[key][row["memtable"]][int(row["threads"])] = (
            float(row["ops_per_sec"]), int(row["stall_events"]))
    return d


def style_x_axis(ax):
    ax.set_xscale("log", base=2)
    ax.set_xticks(THREAD_COUNTS)
    ax.get_xaxis().set_major_formatter(mticker.ScalarFormatter())
    ax.set_xlabel("client threads")
    ax.margins(x=0.08)


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


def plot_mwbn_sweep(data, bg, out_path):
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    all_ys = []
    for ax, memtable in zip(axes, MEMTABLES):
        for mwbn in MWBN_VALUES:
            series = data[(mwbn, bg)][memtable]
            ys = [series[t][0] for t in THREAD_COUNTS]
            stalls = sum(series[t][1] for t in THREAD_COUNTS)
            all_ys.extend(ys)
            ax.plot(THREAD_COUNTS, ys, marker="o", markersize=6, linewidth=2.2,
                    color=MWBN_COLOR[mwbn],
                    label=f"mwbn={mwbn} ({stalls} stalls)")
        style_x_axis(ax)
        ax.set_ylim(bottom=0)
        ax.set_title(memtable.replace("_", r"\_"))
        ax.legend(loc="upper left", frameon=True, framealpha=0.9, fontsize=13)
    for ax in axes:
        style_y_axis_throughput(ax, all_ys)
    fig.suptitle(f"on-disk 100\\% insert: max\\_write\\_buffer\\_number sweep "
                 f"(bg\\_jobs={bg})")
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_bg_sweep(data, mwbn, out_path):
    fig, axes = plt.subplots(1, 2, figsize=(16, 7))
    all_ys = []
    for ax, memtable in zip(axes, MEMTABLES):
        for bg in BG_VALUES:
            series = data[(mwbn, bg)][memtable]
            ys = [series[t][0] for t in THREAD_COUNTS]
            all_ys.extend(ys)
            ax.plot(THREAD_COUNTS, ys, marker="o", markersize=6, linewidth=2.2,
                    color=BG_COLOR[bg], label=f"bg\\_jobs={bg}")
        style_x_axis(ax)
        ax.set_ylim(bottom=0)
        ax.set_title(memtable.replace("_", r"\_"))
        ax.legend(loc="upper left", frameon=True, framealpha=0.9, fontsize=14)
    for ax in axes:
        style_y_axis_throughput(ax, all_ys)
    fig.suptitle(f"on-disk 100\\% insert: bg\\_jobs sweep "
                 f"(max\\_write\\_buffer\\_number={mwbn})")
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    csv_path = R / "write_100pct" / "results.csv"
    data = load(csv_path)

    for bg in BG_VALUES:
        plot_mwbn_sweep(data, bg, OUT / f"mwbn_sweep_bg{bg}.pdf")
    for mwbn in MWBN_VALUES:
        plot_bg_sweep(data, mwbn, OUT / f"bg_sweep_mwbn{mwbn}.pdf")


if __name__ == "__main__":
    main()
