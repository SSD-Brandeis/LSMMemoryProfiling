#!/usr/bin/env python3
import json
import re
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.font_manager as fm
import matplotlib.pyplot as plt
import matplotlib.lines as mlines
from matplotlib.ticker import FuncFormatter, LogLocator
import numpy as np

sys.path.append(str(Path(__file__).resolve().parents[1]))
from plot_scripts.memtable_style import (
    MEMTABLE_DISPLAY_NAMES,
    MEMTABLE_COLORS,
    MEMTABLE_LINESTYLES,
    MEMTABLE_MARKERS,
)
from plot_scripts.style import configure_matplotlib_style, MARKER_SIZE, LINE_WIDTH

def latex_escape(s):
    return s.replace("_", "\\_").replace("%", "\\%")

configure_matplotlib_style(plt, fm)

def trim_trailing_zero(x, _pos):
    s = f"{x:g}"
    return s

def parse_workload_log(log_path):
    metrics = {}
    if not log_path.exists():
        return metrics
    content = log_path.read_text()
    m_tot = re.search(r"Workload Execution Time:\s*(\d+)", content)
    m_ins = re.search(r"Inserts Execution Time:\s*(\d+)", content)
    if m_tot:
        metrics["workload_exec_time_ns"] = int(m_tot.group(1))
    if m_ins:
        metrics["inserts_exec_time_ns"] = int(m_ins.group(1))

    stats_start = content.find("[Rocksdb Stats]")
    stats_end = len(content)
    for marker in ("[Perf Context]", "[IO Stats Context]"):
        idx = content.find(marker, stats_start)
        if idx != -1:
            stats_end = min(stats_end, idx)
    rocksdb_stats = content[stats_start:stats_end] if stats_start != -1 else ""

    m_write_p99 = re.search(
        r"rocksdb\.db\.write\.micros P50 : [\d.eE+-]+ P95 : [\d.eE+-]+ P99 : ([\d.eE+-]+)",
        rocksdb_stats,
    )
    if m_write_p99:
        metrics["write_micros_p99"] = float(m_write_p99.group(1))

    m_get_p99 = re.search(
        r"rocksdb\.db\.get\.micros P50 : [\d.eE+-]+ P95 : [\d.eE+-]+ P99 : ([\d.eE+-]+)",
        rocksdb_stats,
    )
    if m_get_p99:
        metrics["get_micros_p99"] = float(m_get_p99.group(1))
    return metrics

def parse_specs_json(specs_path):
    if not specs_path.exists():
        return {}
    with open(specs_path, "r") as f:
        data = json.load(f)
    total_ops = 0
    total_inserts = 0
    for sec in data.get("sections", []):
        for grp in sec.get("groups", []):
            for op_type, op_info in grp.items():
                if isinstance(op_info, dict) and "op_count" in op_info:
                    count = op_info["op_count"]
                    total_ops += count
                    if op_type == "inserts":
                        total_inserts += count
    return {"total_ops": total_ops, "total_inserts": total_inserts}

def main():
    base_dir = Path(__file__).resolve().parents[1] / ".vstats" / "diskbased-kvsize-exp"
    if not base_dir.exists():
        print(f"Error: Directory {base_dir} does not exist.")
        sys.exit(1)

    kv_sizes = [32, 64, 128, 256, 512, 1024]
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

    results = {}
    for dir_name, canonical_name in dirs:
        results[canonical_name] = {
            "kops": [],
            "mbs": [],
            "write_micros_p99": [],
            "get_micros_p99": [],
        }

    for size in kv_sizes:
        size_dir = base_dir / f"{size}B"
        specs = parse_specs_json(size_dir / "workload.specs.json")
        total_ops = specs.get("total_ops", 0)
        total_inserts = specs.get("total_inserts", 0)

        for dir_name, canonical_name in dirs:
            log_file = size_dir / dir_name / "workload.log"
            metrics = parse_workload_log(log_file)
            exec_time_ns = metrics.get("workload_exec_time_ns", 0)

            if exec_time_ns > 0 and total_ops > 0:
                exec_time_sec = exec_time_ns / 1e9
                kops = (total_ops / exec_time_sec) / 1000.0
                mbs = (total_inserts * size) / (1024 * 1024 * exec_time_sec)
            else:
                kops = 0.0
                mbs = 0.0

            results[canonical_name]["kops"].append(kops)
            results[canonical_name]["mbs"].append(mbs)
            results[canonical_name]["write_micros_p99"].append(
                metrics.get("write_micros_p99", 0.0)
            )
            results[canonical_name]["get_micros_p99"].append(
                metrics.get("get_micros_p99", 0.0)
            )

    x_labels = [f"{s}" for s in kv_sizes]
    x_indices = np.arange(len(kv_sizes))

    out_dirs = [
        base_dir / "plots",
        Path(__file__).resolve().parents[1] / "plot_scripts" / "plots",
    ]
    for d in out_dirs:
        d.mkdir(parents=True, exist_ok=True)

    def plot_series(metric_key, ylabel, out_name):
        fig, ax = plt.subplots(figsize=(6, 5))
        for dir_name, canonical_name in dirs:
            y_vals = results[canonical_name][metric_key]
            display_name = MEMTABLE_DISPLAY_NAMES.get(canonical_name, canonical_name)
            label = latex_escape(display_name)
            color = MEMTABLE_COLORS.get(canonical_name, "#000000")
            linestyle = MEMTABLE_LINESTYLES.get(canonical_name, "solid")
            marker = MEMTABLE_MARKERS.get(canonical_name, "o")

            ax.plot(
                x_indices,
                y_vals,
                label=label,
                color=color,
                linestyle=linestyle,
                marker=marker,
                markersize=MARKER_SIZE,
                linewidth=LINE_WIDTH,
                markerfacecolor="none",
                markeredgewidth=1.5,
            )

        ax.set_xticks(x_indices)
        ax.set_xticklabels(x_labels)
        ax.set_xlabel("key-value size (B)")
        ax.set_ylabel(ylabel)
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_formatter(FuncFormatter(trim_trailing_zero))

        fig.tight_layout()
        for d in out_dirs:
            fig.savefig(str(d / f"{out_name}.pdf"), bbox_inches="tight", pad_inches=0.05)
        plt.close(fig)

    def plot_series_log(metric_key, ylabel, out_name):
        fig, ax = plt.subplots(figsize=(6, 5))
        for dir_name, canonical_name in dirs:
            y_vals = results[canonical_name][metric_key]
            display_name = MEMTABLE_DISPLAY_NAMES.get(canonical_name, canonical_name)
            label = latex_escape(display_name)
            color = MEMTABLE_COLORS.get(canonical_name, "#000000")
            linestyle = MEMTABLE_LINESTYLES.get(canonical_name, "solid")
            marker = MEMTABLE_MARKERS.get(canonical_name, "o")

            ax.plot(
                x_indices,
                y_vals,
                label=label,
                color=color,
                linestyle=linestyle,
                marker=marker,
                markersize=MARKER_SIZE,
                linewidth=LINE_WIDTH,
                markerfacecolor="none",
                markeredgewidth=1.5,
            )

        ax.set_xticks(x_indices)
        ax.set_xticklabels(x_labels)
        ax.set_xlabel("key-value size (B)")
        ax.set_ylabel(ylabel)
        ax.set_yscale("log")
        ax.set_ylim(bottom=1)
        ax.yaxis.set_major_locator(LogLocator(base=10))

        fig.tight_layout()
        for d in out_dirs:
            fig.savefig(str(d / f"{out_name}.pdf"), bbox_inches="tight", pad_inches=0.05)
        plt.close(fig)

    plot_series("kops", "throughput (kOPS)", "diskbased_kvsize_throughput_kops")
    plot_series("mbs", "write throughput (MB/s)", "diskbased_kvsize_throughput_mbs")
    plot_series_log(
        "write_micros_p99",
        "write tail latency ($\\mu$s)",
        "diskbased_kvsize_write_micros_p99",
    )
    plot_series_log(
        "get_micros_p99",
        "rocksdb.db.get.micros p99 (us)",
        "diskbased_kvsize_get_micros_p99",
    )

    handles = []
    for dir_name, canonical_name in dirs:
        display_name = MEMTABLE_DISPLAY_NAMES.get(canonical_name, canonical_name)
        label = latex_escape(display_name)
        color = MEMTABLE_COLORS.get(canonical_name, "#000000")
        linestyle = MEMTABLE_LINESTYLES.get(canonical_name, "solid")
        marker = MEMTABLE_MARKERS.get(canonical_name, "o")
        handles.append(mlines.Line2D(
            [], [], label=label, color=color, linestyle=linestyle,
            marker=marker, markersize=MARKER_SIZE, linewidth=LINE_WIDTH,
            markerfacecolor="none", markeredgewidth=1.5,
        ))

    legend_fig = plt.figure(figsize=(12, 1.2))
    legend_fig.legend(
        handles=handles, loc="center", ncol=5, frameon=False,
        edgecolor="none", columnspacing=1.0, handlelength=2.2,
        handletextpad=0.5,
    )
    for d in out_dirs:
        legend_fig.savefig(str(d / "diskbased_kvsize_legend.pdf"), bbox_inches="tight", pad_inches=0.05)
    plt.close(legend_fig)

    print("Saved throughput plots and separate legend.")

if __name__ == "__main__":
    main()
