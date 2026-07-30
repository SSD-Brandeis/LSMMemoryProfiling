#!/usr/bin/env python3

import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker

REPO_ROOT = Path(__file__).resolve().parents[1]
R = REPO_ROOT / "data" / "ondisk_skiplist_diskio"
OUT = R / "plots"
FONT_PATH = REPO_ROOT / "LinLibertine_Mah.ttf"

THREAD_COUNTS = [1, 2, 4, 8, 16]

fm.fontManager.addfont(str(FONT_PATH))
FONT_NAME = fm.FontProperties(fname=str(FONT_PATH)).get_name()

plt.rcParams.update({
    "text.usetex": True,
    "font.family": FONT_NAME,
    "font.size": 20,
    "axes.titlesize": 17,
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


def load(path):
    rows = list(csv.DictReader(open(path)))
    threads = [int(r["threads"]) for r in rows]
    insert = [float(r["insert_mb_per_sec"]) for r in rows]
    disk = [float(r["disk_write_mb_per_sec"]) for r in rows]
    return threads, insert, disk


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    threads, insert, disk = load(R / "write_100pct" / "results.csv")

    fig, ax = plt.subplots(figsize=(11, 8))
    ax.plot(threads, insert, marker="o", markersize=7, linewidth=2.2,
            color="#2a78d6", label="insert throughput (client-observed)")
    ax.plot(threads, disk, marker="o", markersize=7, linewidth=2.2,
            color="#c0392b", label="measured disk write throughput (iostat)")
    style_x_axis(ax)
    ax.set_ylabel("throughput (MB/s)")
    ax.set_ylim(bottom=0)
    ax.set_title("on-disk 100\\% insert, skiplist: insert throughput vs "
                 "measured disk write (bg\\_jobs=8, mwb=16, buffer=128MB, "
                 "uw=1, T=10, key=128B, val=896B, ops=6M)")
    ax.legend(loc="best", frameon=True, framealpha=0.9)
    fig.tight_layout()
    out_path = OUT / "insert_vs_diskio_skiplist.pdf"
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)
    print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
