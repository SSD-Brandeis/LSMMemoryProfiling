import math
import os
import re
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

from plot import *
from plot.style import line_styles

TAG = "vary-buffersize-overhead-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / TAG

N_FLUSHES = 10  # number of flush lines to average per run

# Buffer sizes from vary-buffersize-exp.sh (in KB): 2^6 to 2^20
BUFFER_SIZES_KB = [64, 256, 1024, 4096, 16384, 65536, 262144, 1048576]

# Experiment parameters (must match vary-buffersize-exp.sh)
ENTRY_SIZE = 128
PAGE_SIZE  = 4096

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
    if "linkedlist" in name:
        return "linkedlist"
    if "unsortedvector" in name:
        return "unsortedvector"
    if "sortedvector" in name:
        return "alwayssortedvector"
    if "vector" in name:
        return "vector"
    return None


def fmt_kb_power2(kb):
    """Return a $2^n$ KB label for a buffer size given in KB."""
    exp = round(math.log2(kb))
    return f"$2^{{{exp}}}$"


# Matches: "... [num_entries]: N[Flush Stats] ..."
_FLUSH_RE = re.compile(r"num_entries\]:\s*(\d+)")


def parse_flush_overhead(log_path, buffer_capacity, entry_size, n_flushes=N_FLUSHES):
    """
    Parse workload.log and return the mean metadata overhead (%) over the
    first n_flushes flush events.

    Overhead = (buffer_capacity - avg_num_entries * entry_size) / buffer_capacity * 100
    """
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
    overhead_pct = (buffer_capacity - data_bytes) / buffer_capacity * 100.0
    return float(overhead_pct)


def collect_data():
    """
    Returns:
        buffer_sizes_kb — sorted list of buffer sizes (KB)
        data            — dict: impl -> list of (kb, overhead_pct) pairs
    """
    buffer_sizes_kb = BUFFER_SIZES_KB
    data = {impl: [] for impl in implementations}

    for impl in implementations:
        for kb in buffer_sizes_kb:
            log_path = EXP_DIR / f"{kb}KB" / impl / "workload.log"
            if not log_path.exists():
                continue
            pages_per_file   = (kb * 1024) // PAGE_SIZE
            entries_per_page = PAGE_SIZE // ENTRY_SIZE
            buffer_capacity  = pages_per_file * entries_per_page * ENTRY_SIZE
            overhead = parse_flush_overhead(log_path, buffer_capacity, ENTRY_SIZE)
            if overhead is not None:
                data[impl].append((kb, overhead))
        if data[impl]:
            print(f"  {impl}: {len(data[impl])} buffer sizes loaded")

    return buffer_sizes_kb, data


def plot_overhead():
    buffer_sizes_kb, data = collect_data()

    fig, ax = plt.subplots(figsize=(4, 3))

    for impl in implementations:
        points = data[impl]
        if not points:
            continue
        key = normalize_name(impl)
        if key is None or key not in line_styles:
            continue
        style = line_styles[key]

        xs = [kb for kb, _ in points]
        ys = [pct for _, pct in points]

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

    ax.set_xscale("log", base=2)
    ax.set_xticks([kb for i, kb in enumerate(buffer_sizes_kb) if i % 2 == 1])
    labels = [fmt_kb_power2(kb) for i, kb in enumerate(buffer_sizes_kb) if i % 2 == 1]
    ax.set_xticklabels(labels)
    ax.set_xlabel("buffer size (KB)", labelpad=-1)
    ax.set_ylabel("metadata overhead (\\%)", labelpad=-1, y=0.39)
    ax.set_ylim(0, 100 + 100 * 0.1)
    ax.text(0.99, 0.98, "(A)", transform=ax.transAxes,
            va="top", ha="right", fontsize=20)

    output_file = DROPBOX_PATH / "vary-buffersize-overhead.pdf"
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

    fig_legend = plt.figure(figsize=(10, 0.5))
    ax_legend = fig_legend.add_subplot(111)
    ax_legend.axis("off")
    ax_legend.legend(
        handles=legend_elements,
        loc="center",
        frameon=False,
        ncol=3,
        borderaxespad=0,
        labelspacing=0.2,
        borderpad=0,
        columnspacing=2,
        handletextpad=0.2,
    )

    legend_file = DROPBOX_PATH / "vary-buffersize-overhead-legend.pdf"
    fig_legend.savefig(legend_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"Saved: {legend_file}")


def dump_csv():
    import csv

    _, data = collect_data()

    # Pre-compute wasted_mb for each (impl, kb) from the same log files
    wasted: dict[str, dict[int, float]] = {impl: {} for impl in implementations}
    for impl in implementations:
        for kb in BUFFER_SIZES_KB:
            log_path = EXP_DIR / f"{kb}KB" / impl / "workload.log"
            if not log_path.exists():
                continue
            pages_per_file   = (kb * 1024) // PAGE_SIZE
            entries_per_page = PAGE_SIZE // ENTRY_SIZE
            buffer_capacity  = pages_per_file * entries_per_page * ENTRY_SIZE

            with open(log_path) as f:
                entries = []
                for line in f:
                    m = _FLUSH_RE.search(line)
                    if m:
                        entries.append(int(m.group(1)))
                    if len(entries) >= N_FLUSHES:
                        break
            if entries:
                avg_data_bytes = np.mean(entries) * ENTRY_SIZE
                wasted[impl][kb] = (buffer_capacity - avg_data_bytes) / (1024 * 1024)

    # Build wide CSV: one row per buffer size
    impl_keys = [k for k in implementations]
    fields = ["buffer_size_kb", "buffer_size_mb"] + \
             [f"{impl}_overhead_pct" for impl in impl_keys] + \
             [f"{impl}_wasted_mb"    for impl in impl_keys]

    output_file = CURR_DIR / TAG / "vary-buffersize-overhead.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for kb in BUFFER_SIZES_KB:
            row: dict = {"buffer_size_kb": kb, "buffer_size_mb": kb / 1024}
            for impl in impl_keys:
                pct_val = next((v for bk, v in data[impl] if bk == kb), None)
                row[f"{impl}_overhead_pct"] = f"{pct_val:.4f}" if pct_val is not None else ""
                wb = wasted[impl].get(kb)
                row[f"{impl}_wasted_mb"]    = f"{wb:.4f}"    if wb    is not None else ""
            writer.writerow(row)

    print(f"Saved: {output_file}")


if __name__ == "__main__":
    # plot_overhead()
    plot_legend()
    # dump_csv()
