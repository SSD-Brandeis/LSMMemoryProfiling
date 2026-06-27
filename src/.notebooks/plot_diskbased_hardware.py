import csv
import os
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

from plot import *
from plot.style import hatch_map, line_styles

# Override: latex not available on this machine
import re
plt.rcParams["text.usetex"] = False

TAG = "diskbased-hardware-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_BASE = PROJECT_ROOT / ".vstats"

# Workload counts matching run-diskbased-all-devices.sh
TOTAL_INSERTS = 100_000_000  # 80M + 10M + 10M
TOTAL_PQ = 10_000
TOTAL_RQ = 1_000

DEVICES = ["hdd", "ssd", "nvme"]
DEVICE_LABELS = {"hdd": "HDD", "ssd": "SSD", "nvme": "NVMe"}

implementations = [
    "vector-preallocated",
    "unsortedvector-preallocated",
    "sortedvector-preallocated",
    "skiplist",
    "simpleskiplist",
    "hashlinkedlist-H100000-X6",
    "hashskiplist-H100000-X6",
    "hashvector-H100000-X6",
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


def parse_workload_log(log_file: Path):
    """Parse workload.log and return (insert_ns, pq_ns, rq_ns)."""
    if not log_file.exists():
        return None
    insert_ns = pq_ns = rq_ns = 0
    with open(log_file) as f:
        for line in f:
            if line.startswith("Inserts Execution Time:"):
                insert_ns = int(line.split(":")[1].strip())
            elif line.startswith("PointQuery Execution Time:"):
                pq_ns = int(line.split(":")[1].strip())
            elif line.startswith("RangeQuery Execution Time:"):
                rq_ns = int(line.split(":")[1].strip())
    if insert_ns == 0:
        return None
    return insert_ns, pq_ns, rq_ns


def safe_tp(ops, time_ns):
    if time_ns == 0:
        return 0.0
    return ops / (time_ns / 1e9)


def collect_data():
    """Returns dict: results[device][impl] = [insert_tp, pq_tp, rq_tp]"""
    results = {dev: {} for dev in DEVICES}
    for dev in DEVICES:
        exp_dir = EXP_BASE / f"diskbased-exp-{dev}"
        for impl in implementations:
            log_file = exp_dir / impl / "workload.log"
            parsed = parse_workload_log(log_file)
            if parsed is None:
                print(f"  Missing: {log_file}")
                continue
            insert_ns, pq_ns, rq_ns = parsed
            insert_tp = safe_tp(TOTAL_INSERTS, insert_ns)
            pq_tp = safe_tp(TOTAL_PQ, pq_ns)
            rq_tp = safe_tp(TOTAL_RQ, rq_ns)
            results[dev][impl] = [insert_tp, pq_tp, rq_tp]
            print(
                f"  [{dev}] {impl}: "
                f"I={insert_tp/1e3:.1f}kOPS  PQ={pq_tp:.0f}  RQ={rq_tp:.0f} ops/s"
            )
    return results


def _plot_hardware_op(results, op_index, y_label, is_log_scale, output_name,
                      y_scale=1.0):
    """
    Grouped-bar plot: groups = devices (HDD/SSD/NVMe),
    bars within group = memtable implementations.
    y_scale: divide values by this factor before plotting (e.g. 1000 for kOPS).
    """
    impls = [i for i in implementations if any(i in results[d] for d in DEVICES)]
    n_impls = len(impls)

    x = np.arange(len(DEVICES))
    bar_width = 0.8 / n_impls

    fig, ax = plt.subplots(figsize=(5, 2.8))

    for j, impl in enumerate(impls):
        key = normalize_name(impl)
        if key is None or key not in line_styles:
            continue
        style = line_styles[key]
        color = style["color"]

        heights = [
            results[d].get(impl, [0, 0, 0])[op_index] / y_scale
            for d in DEVICES
        ]
        xpos = x + j * bar_width

        ax.bar(
            xpos,
            heights,
            bar_width - 0.01,
            facecolor=color if impl == "vector-preallocated" else "none",
            edgecolor=color,
            hatch=hatch_map.get(key, None),
            label=style["label"],
        )

    ax.set_xticks(x + bar_width * (n_impls - 1) / 2)
    ax.set_xticklabels([DEVICE_LABELS[d] for d in DEVICES])

    if is_log_scale:
        ax.set_yscale("log")
        ax.set_ylim(bottom=1e0)
    else:
        ax.set_ylim(bottom=0)

    ax.set_ylabel(y_label, labelpad=-1, loc="top")

    output_file = CURR_DIR / TAG / output_name
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.06)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_insert_throughput(results):
    _plot_hardware_op(
        results,
        op_index=0,
        y_label="throughput (kOPS)",
        is_log_scale=False,
        output_name="diskbased-insert-throughput.pdf",
        y_scale=1_000,
    )


def plot_pq_throughput(results):
    _plot_hardware_op(
        results,
        op_index=1,
        y_label="throughput (OPS)",
        is_log_scale=True,
        output_name="diskbased-pq-throughput.pdf",
    )


def plot_rq_throughput(results):
    _plot_hardware_op(
        results,
        op_index=2,
        y_label="throughput (OPS)",
        is_log_scale=True,
        output_name="diskbased-rq-throughput.pdf",
    )


def plot_combined(results):
    """Single figure with 3 subplots side-by-side, matching Fig. 20."""
    impls = [i for i in implementations if any(i in results[d] for d in DEVICES)]
    n_impls = len(impls)
    x = np.arange(len(DEVICES))
    bar_width = 0.8 / n_impls

    fig, axes = plt.subplots(1, 3, figsize=(14, 2.8))
    panel_labels = ["(A)", "(B)", "(C)"]
    configs = [
        (0, "throughput (kOPS)", False, 1_000),
        (1, "throughput (OPS)",  True,  1.0),
        (2, "throughput (OPS)",  True,  1.0),
    ]

    for ax, (op_index, y_label, is_log, y_scale), panel in zip(axes, configs, panel_labels):
        for j, impl in enumerate(impls):
            key = normalize_name(impl)
            if key is None or key not in line_styles:
                continue
            style = line_styles[key]
            color = style["color"]
            heights = [
                results[d].get(impl, [0, 0, 0])[op_index] / y_scale
                for d in DEVICES
            ]
            xpos = x + j * bar_width
            ax.bar(
                xpos,
                heights,
                bar_width - 0.01,
                facecolor=color if impl == "vector-preallocated" else "none",
                edgecolor=color,
                hatch=hatch_map.get(key, None),
                label=style["label"],
            )

        ax.set_xticks(x + bar_width * (n_impls - 1) / 2)
        ax.set_xticklabels([DEVICE_LABELS[d] for d in DEVICES])
        if is_log:
            ax.set_yscale("log")
            ax.set_ylim(bottom=1e0)
        else:
            ax.set_ylim(bottom=0)
        ax.set_ylabel(y_label, labelpad=-1, loc="top")
        ax.text(0.99, 0.98, panel, transform=ax.transAxes,
                va="top", ha="right", fontsize=20)

    output_file = CURR_DIR / TAG / "diskbased-hardware-combined.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.06)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_legend(results):
    impls_present = [
        impl
        for impl in implementations
        if any(impl in results[d] for d in DEVICES)
    ]

    legend_elements = []
    for impl in impls_present:
        key = normalize_name(impl)
        if key is None or key not in line_styles:
            continue
        style = line_styles[key]
        color = style["color"]
        legend_elements.append(
            Patch(
                facecolor=color if impl == "vector-preallocated" else "none",
                edgecolor=color,
                hatch=hatch_map.get(key, None),
                label=style["label"],
                linewidth=1.5,
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
        labelspacing=0.1,
        borderpad=0,
        columnspacing=2,
        handletextpad=0.2,
    )

    legend_file = CURR_DIR / TAG / "diskbased-legend.pdf"
    fig_legend.savefig(legend_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"Saved: {legend_file}")


def dump_csv(results):
    fields = ["device", "impl", "insert_kops", "pq_ops", "rq_ops"]
    output_file = CURR_DIR / TAG / "diskbased-hardware.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for dev in DEVICES:
            for impl in implementations:
                tps = results[dev].get(impl)
                if tps is None:
                    continue
                writer.writerow({
                    "device":      dev,
                    "impl":        impl,
                    "insert_kops": f"{tps[0]/1e3:.2f}",
                    "pq_ops":      f"{tps[1]:.2f}",
                    "rq_ops":      f"{tps[2]:.2f}",
                })
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    results = collect_data()
    for dev in DEVICES:
        print(f"{DEVICE_LABELS[dev]}: {len(results[dev])} implementations loaded")
    plot_insert_throughput(results)
    plot_pq_throughput(results)
    plot_rq_throughput(results)
    plot_combined(results)
    plot_legend(results)
    dump_csv(results)
