import json
import os
from pathlib import Path

import matplotlib.pyplot as plt

from plot import *
from plot.style import line_styles
from plot.rocksdb_stats import parse_rocksdb_log

TAG = "diskbased-kvsize-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / TAG

KV_SIZES = [32, 64, 128, 256, 512, 1024]

implementations = [
    "vector-preallocated",
    "unsortedvector-preallocated",
    "sortedvector-preallocated",
    "skiplist",
    "simpleskiplist",
    "hashlinkedlist-H100000-X6",
    "hashskiplist-H100000-X6",
    "hashvector-H100000-X6",
    "art",
    "btree",
]


def normalize_name(name):
    name = name.lower()
    if name == "art":
        return "art"
    if name == "btree":
        return "btree"
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


def parse_specs_totals(specs_path: Path) -> tuple[int, int]:
    """Total op count across the whole workload, and the insert-only subset."""
    if not specs_path.exists():
        return 0, 0
    with open(specs_path) as f:
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
    return total_ops, total_inserts


def read_metrics(workload_log: Path, kv_size: int, total_ops: int, total_inserts: int) -> dict:
    if not workload_log.exists():
        return {}
    phases = parse_rocksdb_log(str(workload_log))
    if not phases:
        return {}

    exec_time_ns = phases[0]["meta"].get("workload_time", 0)
    if exec_time_ns > 0 and total_ops > 0:
        exec_time_sec = exec_time_ns / 1e9
        kops = (total_ops / exec_time_sec) / 1000.0
        mbs = (total_inserts * kv_size) / (1024 * 1024 * exec_time_sec)
    else:
        kops, mbs = 0.0, 0.0

    write_p99 = phases[0]["histograms"].get("rocksdb.db.write.micros", {}).get("P99", 0.0)
    get_p99 = phases[0]["histograms"].get("rocksdb.db.get.micros", {}).get("P99", 0.0)

    return {
        "kops": kops,
        "mbs": mbs,
        "write_micros_p99": write_p99,
        "get_micros_p99": get_p99,
    }


def load_data() -> dict[str, dict[str, list[float]]]:
    data = {
        impl: {"kops": [], "mbs": [], "write_micros_p99": [], "get_micros_p99": []}
        for impl in implementations
    }
    for size in KV_SIZES:
        size_dir = EXP_DIR / f"{size}B"
        total_ops, total_inserts = parse_specs_totals(size_dir / "workload.specs.json")
        for impl in implementations:
            log = size_dir / impl / "workload.log"
            metrics = read_metrics(log, size, total_ops, total_inserts)
            if not metrics:
                print(f"Missing: {log}")
            for key in data[impl]:
                data[impl][key].append(metrics.get(key, 0.0))
    return data


def _plot_single(data, op_key, ylabel, output_name, fig_label):
    fig, ax = plt.subplots(figsize=(4, 3.2))
    for impl in implementations:
        key = normalize_name(impl)
        if key is None or key not in line_styles:
            continue
        s = line_styles[key]
        ax.plot(
            KV_SIZES, data[impl][op_key],
            color=s["color"],
            linestyle=s.get("linestyle", "-"),
            linewidth=s.get("linewidth", 2),
            marker=s.get("marker", "o"),
            markersize=s.get("markersize", 6),
            markerfacecolor=s.get("markerfacecolor", "none"),
            label=s["label"],
        )

    ax.set_xscale("log", base=2)
    ax.set_xticks(KV_SIZES)
    ax.set_xticklabels([str(s) for s in KV_SIZES])
    ax.set_xlabel("key-value size (B)", labelpad=-1)
    ax.set_yscale("log")
    ax.set_ylabel(ylabel, labelpad=-0.5, y=0.38)
    ax.text(0.99, 0.85, fig_label, transform=ax.transAxes,
            fontsize=20, va="bottom", ha="right")

    output_file = DROPBOX_PATH / output_name
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_throughput_kops(data):
    _plot_single(data, "kops", "throughput (kOPS)", "diskbased-kvsize-throughput-kops.pdf", "(A)")


def plot_throughput_mbs(data):
    _plot_single(data, "mbs", "write throughput (MB/s)", "diskbased-kvsize-throughput-mbs.pdf", "(B)")


def plot_write_p99(data):
    _plot_single(data, "write_micros_p99", "write tail latency ($\\mu$s)", "diskbased-kvsize-write-p99.pdf", "(C)")


def plot_get_p99(data):
    _plot_single(data, "get_micros_p99", "get tail latency ($\\mu$s)", "diskbased-kvsize-get-p99.pdf", "(D)")


def plot_legend():
    fig, ax = plt.subplots(figsize=(0.1, 0.05))
    for impl in implementations:
        key = normalize_name(impl)
        if key is None or key not in line_styles:
            continue
        s = line_styles[key]
        ax.plot(
            [], [],
            color=s["color"],
            linestyle=s.get("linestyle", "-"),
            linewidth=s.get("linewidth", 2),
            marker=s.get("marker", "o"),
            markersize=s.get("markersize", 6),
            markerfacecolor=s.get("markerfacecolor", "none"),
            label=s["label"],
        )
    handles, labels = ax.get_legend_handles_labels()
    leg_fig, leg_ax = plt.subplots(figsize=(8, 0.8))
    leg_ax.axis("off")
    leg_ax.legend(handles, labels, frameon=False, ncol=5,
                  loc="center", labelspacing=0.2, handlelength=1.4,
                  columnspacing=1, handletextpad=0.3)
    plt.close(fig)
    output_file = DROPBOX_PATH / "diskbased-kvsize-legend.pdf"
    leg_fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(leg_fig)
    print(f"Saved: {output_file}")


def dump_csv(data):
    import csv

    fields = ["impl", "kv_size_b", "insert_kops", "write_mbs", "write_p99_us", "get_p99_us"]
    output_file = CURR_DIR / TAG / "diskbased-kvsize.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for impl in implementations:
            for i, size in enumerate(KV_SIZES):
                writer.writerow({
                    "impl":         impl,
                    "kv_size_b":    size,
                    "insert_kops":  f"{data[impl]['kops'][i]:.6f}",
                    "write_mbs":    f"{data[impl]['mbs'][i]:.6f}",
                    "write_p99_us": f"{data[impl]['write_micros_p99'][i]:.6f}",
                    "get_p99_us":   f"{data[impl]['get_micros_p99'][i]:.6f}",
                })
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    data = load_data()
    plot_throughput_kops(data)
    plot_throughput_mbs(data)
    plot_write_p99(data)
    plot_get_p99(data)
    plot_legend()
    dump_csv(data)
