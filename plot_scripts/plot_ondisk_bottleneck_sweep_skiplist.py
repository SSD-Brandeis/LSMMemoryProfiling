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
R = REPO_ROOT / "data" / "ondisk_small_l2_bottleneck_sweep_skiplist"
OUT = R / "plots"
FONT_PATH = REPO_ROOT / "LinLibertine_Mah.ttf"

THREAD_COUNTS = [1, 2, 4, 8, 16]
BG_JOBS_VALUES = [1, 2, 4, 8, 16]
MWBN_VALUES = [2, 4, 8, 16, 32]
MWBN_SCALED_VALUES = [1, 2, 4, 8, 16]
BG_COLOR = {1: "#c0392b", 2: "#e08a1e", 4: "#f0d000", 8: "#1a8a4a", 16: "#2a78d6"}
MWBN_COLOR = {1: "#8e44ad", 2: "#c0392b", 4: "#e08a1e", 8: "#f0d000",
             16: "#1a8a4a", 32: "#2a78d6"}

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


def style_x_axis(ax):
    ax.set_xscale("log", base=2)
    ax.set_xticks(THREAD_COUNTS)
    ax.get_xaxis().set_major_formatter(mticker.ScalarFormatter())
    ax.set_xlabel(r"\# of threads")
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


def load(path, series_field):
    """{series_value: {threads: ops/s}}"""
    d = defaultdict(dict)
    for row in csv.DictReader(open(path)):
        d[int(row[series_field])][int(row["threads"])] = float(row["ops_per_sec"])
    return d


def plot_bg_sweep(subdir, buf_label, out_path):
    data = load(R / "write_100pct" / subdir / "results.csv", "max_background_jobs")
    fig, ax = plt.subplots(figsize=(11, 8))
    all_ys = []
    for bg in BG_JOBS_VALUES:
        xs = [t for t in THREAD_COUNTS if t in data[bg]]
        ys = [data[bg][t] for t in xs]
        all_ys.extend(ys)
        ax.plot(xs, ys, marker="o", markersize=6, linewidth=2.2,
                color=BG_COLOR[bg], label=f"bg\\_jobs={bg}")
    style_x_axis(ax)
    style_y_axis_throughput(ax, all_ys)
    ax.set_ylim(bottom=0)
    ax.set_title("on-disk 100\\% insert, skiplist: throughput vs bg\\_jobs "
                 f"(mwb=8, buffer={buf_label}, uw=1, T=10, key=128B, val=896B)")
    ax.legend(loc="best", frameon=True, framealpha=0.9, ncol=2)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_mwbn_sweep(subdir, buf_label, bg_label, out_path, mwbn_values=MWBN_VALUES,
                    op_count_label=None):
    data = load(R / "write_100pct" / subdir / "results.csv",
               "max_write_buffer_number")
    fig, ax = plt.subplots(figsize=(11, 8))
    all_ys = []
    for mwbn in mwbn_values:
        xs = [t for t in THREAD_COUNTS if t in data[mwbn]]
        ys = [data[mwbn][t] for t in xs]
        all_ys.extend(ys)
        ax.plot(xs, ys, marker="o", markersize=6, linewidth=2.2,
                color=MWBN_COLOR[mwbn], label=f"mwb={mwbn}")
    style_x_axis(ax)
    style_y_axis_throughput(ax, all_ys)
    ax.set_ylim(bottom=0)
    op_count_part = f", ops={op_count_label}" if op_count_label else ""
    ax.set_title("on-disk 100\\% insert, skiplist: throughput vs mwb "
                 f"(bg\\_jobs={bg_label}, buffer={buf_label}, uw=1, T=10, "
                 f"key=128B, val=896B{op_count_part})")
    ax.legend(loc="best", frameon=True, framealpha=0.9, ncol=2)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def plot_bufsize_sweep(out_path):
    rows = list(csv.DictReader(open(R / "write_100pct" / "bufsize_sweep" / "results.csv")))
    by_buf = defaultdict(dict)
    for row in rows:
        by_buf[int(row["buffer_bytes"])][int(row["threads"])] = float(row["ops_per_sec"])

    fig, ax = plt.subplots(figsize=(11, 8))
    all_ys = []
    buf_color = {8 * 1024 * 1024: "#c0392b", 32 * 1024 * 1024: "#e08a1e",
                128 * 1024 * 1024: "#2a78d6"}
    buf_label = {8 * 1024 * 1024: "buffer=8MB", 32 * 1024 * 1024: "buffer=32MB",
                128 * 1024 * 1024: "buffer=128MB"}
    for buf_bytes in sorted(by_buf):
        xs = [t for t in THREAD_COUNTS if t in by_buf[buf_bytes]]
        ys = [by_buf[buf_bytes][t] for t in xs]
        all_ys.extend(ys)
        ax.plot(xs, ys, marker="o", markersize=6, linewidth=2.2,
                color=buf_color.get(buf_bytes, "#000000"),
                label=buf_label.get(buf_bytes, f"buffer={buf_bytes}"))
    style_x_axis(ax)
    style_y_axis_throughput(ax, all_ys)
    ax.set_ylim(bottom=0)
    ax.set_title("on-disk 100\\% insert, skiplist: throughput vs write\\_buffer\\_size "
                 "(mwb=8, bg\\_jobs=threads, uw=1, T=10, key=128B, val=896B)")
    ax.legend(loc="best", frameon=True, framealpha=0.9)
    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    plot_bg_sweep("bg_sweep", "8MB", OUT / "write_throughput_bg_sweep_skiplist.pdf")
    plot_mwbn_sweep("mwbn_sweep", "8MB", "threads",
                   OUT / "write_throughput_mwbn_sweep_skiplist.pdf")
    plot_bufsize_sweep(OUT / "write_throughput_bufsize_sweep_skiplist.pdf")
    plot_bg_sweep("bg_sweep_buf128mib", "128MB",
                 OUT / "write_throughput_bg_sweep_buf128mib_skiplist.pdf")
    # mwbn_sweep_buf128mib (450,000 ops) never reached the L0 compaction
    # trigger at this buffer size (see manifest note), so this plot uses the
    # corrected mwbn_sweep_buf128mib_scaled data (7,000,000 ops, bg_jobs
    # fixed at 8) instead -- same output filename since it supersedes the
    # uncompacted result.
    plot_mwbn_sweep("mwbn_sweep_buf128mib_scaled", "128MB", "8",
                   OUT / "write_throughput_mwbn_sweep_buf128mib_skiplist.pdf",
                   mwbn_values=MWBN_SCALED_VALUES, op_count_label="7M")


if __name__ == "__main__":
    main()
