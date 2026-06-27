import math
import os
import re
from pathlib import Path

from plot import *

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

from plot.style import line_styles

TAG = "vary-entrysize-overhead-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / "vary-entrysize-overhead-exp"

N_FLUSHES = 10

# Entry sizes from vary-entrysize-exp.sh (bytes); extend as needed
ENTRY_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024, 2048]

# Fixed params matching the bash script
BUFFER_SIZE_MB = 128
PAGE_SIZE      = 4096
BUFFER_CAPACITY = BUFFER_SIZE_MB * 1024 * 1024  # 134,217,728 bytes (constant)

implementations = [
    "vector-preallocated",
    # "unsortedvector-preallocated",
    # "sortedvector-preallocated",
    "skiplist",
    "simpleskiplist",
    "hashskiplist-H100000-X6",
    "hashvector-H100000-X6",
    "hashlinkedlist-H100000-X6",
]


def normalize_name(name):
    name = name.lower()
    if "hashlinkedlist" in name:
        return "hashlinkedlist"
    if "hashskiplist" in name:
        return "hashskiplist"
    if "hashvector" in name:
        return "hashvector"
    if "simpleskiplist" in name:
        return "simpleskiplist"
    if name == "skiplist":
        return "skiplist"
    if "unsortedvector" in name:
        return "unsortedvector"
    if "sortedvector" in name:
        return "alwayssortedvector"
    if "vector" in name:
        return "vector"
    return None


_FLUSH_RE = re.compile(r"num_entries\]:\s*(\d+)")


def parse_flush_overhead(log_path, entry_size, n_flushes=N_FLUSHES):
    with open(log_path) as f:
        lines = f.readlines()

    num_entries_list = []
    for line in lines:
        match = _FLUSH_RE.search(line)
        if not match:
            continue
        num_entries_list.append(int(match.group(1)))
        if len(num_entries_list) >= n_flushes:
            break

    if not num_entries_list:
        return None

    avg_entries  = np.mean(num_entries_list)
    data_bytes   = avg_entries * entry_size
    overhead_pct = (BUFFER_CAPACITY - data_bytes) / BUFFER_CAPACITY * 100.0
    return float(overhead_pct)


def collect_data():
    data = {impl: [] for impl in implementations}

    for impl in implementations:
        for es in ENTRY_SIZES:
            log_path = EXP_DIR / f"{es}B" / impl / "workload.log"
            if not log_path.exists():
                continue
            overhead = parse_flush_overhead(log_path, es)
            if overhead is not None:
                data[impl].append((es, overhead))
        if data[impl]:
            print(f"  {impl}: {len(data[impl])} entry sizes loaded")

    return data


def plot_overhead():
    data = collect_data()

    fig, ax = plt.subplots(figsize=(4, 3))

    for impl in implementations:
        points = data[impl]
        if not points:
            continue
        key = normalize_name(impl)
        if key is None or key not in line_styles:
            continue
        style = line_styles[key]

        xs = [es  for es, _   in points]
        ys = [pct for _,  pct in points]

        ax.plot(
            xs, ys,
            label=style["label"],
            color=style["color"],
            linestyle=style["linestyle"],
            marker=style.get("marker"),
            markersize=style.get("markersize", 8),
            markerfacecolor=style.get("markerfacecolor", "none"),
            linewidth=style.get("linewidth", 2),
        )

    present = sorted({es for impl in implementations for es, _ in data[impl]})
    ax.set_xscale("log", base=2)
    ax.set_xticks(present[::2])
    ax.set_xticklabels([str(es) for es in present[::2]])
    ax.set_xlabel("entry size (B)", labelpad=-1)
    ax.set_ylabel("metadata overhead (\\%)", labelpad=-1, y=0.39)
    ax.set_ylim(0, 110)
    ax.text(0.99, 0.98, "(B)", transform=ax.transAxes,
            va="top", ha="right", fontsize=20)

    output_file = DROPBOX_PATH / "vary-entrysize-overhead.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_legend():
    legend_elements = []
    for impl in implementations:
        key = normalize_name(impl)
        if key is None or key not in line_styles:
            continue
        style = line_styles[key]
        legend_elements.append(
            Line2D(
                [0], [0],
                label=style["label"],
                color=style["color"],
                linestyle=style["linestyle"],
                marker=style.get("marker"),
                markersize=style.get("markersize", 8),
                markerfacecolor=style.get("markerfacecolor", "none"),
                linewidth=style.get("linewidth", 2),
            )
        )

    fig_legend = plt.figure(figsize=(10, 1.5))
    ax_legend = fig_legend.add_subplot(111)
    ax_legend.axis("off")
    ax_legend.legend(
        handles=legend_elements,
        loc="center",
        frameon=False,
        ncol=4,
        borderaxespad=0,
        labelspacing=0.2,
        borderpad=0,
        columnspacing=0.5,
        handletextpad=0.2,
    )

    legend_file = CURR_DIR / TAG / "vary-entrysize-overhead-legend.pdf"
    fig_legend.savefig(legend_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"Saved: {legend_file}")


def dump_csv():
    import csv

    data = collect_data()

    # Also compute wasted_mb per (impl, entry_size)
    wasted: dict[str, dict[int, float]] = {impl: {} for impl in implementations}
    for impl in implementations:
        for es in ENTRY_SIZES:
            log_path = EXP_DIR / f"{es}B" / impl / "workload.log"
            if not log_path.exists():
                continue
            with open(log_path) as f:
                entries = []
                for line in f:
                    m = _FLUSH_RE.search(line)
                    if m:
                        entries.append(int(m.group(1)))
                    if len(entries) >= N_FLUSHES:
                        break
            if entries:
                avg_data_bytes = np.mean(entries) * es
                wasted[impl][es] = (BUFFER_CAPACITY - avg_data_bytes) / (1024 * 1024)

    fields = ["entry_size_b"] + \
             [f"{impl}_overhead_pct" for impl in implementations] + \
             [f"{impl}_wasted_mb"    for impl in implementations]

    output_file = CURR_DIR / TAG / "vary-entrysize-overhead.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for es in ENTRY_SIZES:
            row: dict = {"entry_size_b": es}
            for impl in implementations:
                pct_val = next((v for bk, v in data[impl] if bk == es), None)
                row[f"{impl}_overhead_pct"] = f"{pct_val:.4f}" if pct_val is not None else ""
                wb = wasted[impl].get(es)
                row[f"{impl}_wasted_mb"]    = f"{wb:.4f}"    if wb    is not None else ""
            writer.writerow(row)

    print(f"Saved: {output_file}")


if __name__ == "__main__":
    plot_overhead()
    plot_legend()
    dump_csv()
