import os
from pathlib import Path

import matplotlib.pyplot as plt

from plot import *
from plot.style import line_styles
from plot.rocksdb_stats import parse_rocksdb_log

TAG = "vary-size-ratio-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / TAG

T_VALUES = [2, 4, 6, 8, 10]

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


def safe_tp(count, time_ns, scale=1e6):
    if time_ns == 0:
        return 0.0
    return count / (time_ns / 1e9) / scale


def read_throughput(workload_log: Path) -> tuple[float, float, float]:
    if not workload_log.exists():
        return 0.0, 0.0, 0.0
    phases = parse_rocksdb_log(str(workload_log))
    if not phases:
        return 0.0, 0.0, 0.0

    insert_tp, pq_tp, rq_tp = 0.0, 0.0, 0.0
    for ph in phases:
        meta = ph["meta"]
        t = meta.get("insert_time", 0)
        if t > 0 and insert_tp == 0.0:
            insert_tp = safe_tp(ph["tickers"].get("rocksdb.number.keys.written", 0), t, scale=1e3)
        t = meta.get("point_query_time", 0)
        if t > 0 and pq_tp == 0.0:
            pq_tp = safe_tp(ph["tickers"].get("rocksdb.number.keys.read", 0), t, scale=1e3)
        t = meta.get("range_query_time", 0)
        if t > 0 and rq_tp == 0.0:
            rq_count = ph["tickers"].get("rocksdb.number.multiget.keys.read", 0) or 1000
            rq_tp = safe_tp(rq_count, t, scale=1)

    return insert_tp, pq_tp, rq_tp


def load_data() -> dict[str, dict[str, list[float]]]:
    data = {impl: {"insert": [], "pq": [], "rq": []} for impl in implementations}
    for t in T_VALUES:
        for impl in implementations:
            log = EXP_DIR / f"T{t}" / impl / "workload.log"
            if not log.exists():
                print(f"Missing: {log}")
                data[impl]["insert"].append(0.0)
                data[impl]["pq"].append(0.0)
                data[impl]["rq"].append(0.0)
                continue
            ins, pq, rq = read_throughput(log)
            data[impl]["insert"].append(ins)
            data[impl]["pq"].append(pq)
            data[impl]["rq"].append(rq)
    return data


def _plot_single(data, op_key, ylabel, output_name, fig_label, exp_text=None, ypad=1.0):
    fig, ax = plt.subplots(figsize=(4, 3.2))
    for impl in implementations:
        key = normalize_name(impl)
        if key is None or key not in line_styles:
            continue
        s = line_styles[key]
        ax.plot(T_VALUES, data[impl][op_key],
                color=s["color"],
                linestyle=s.get("linestyle", "-"),
                linewidth=s.get("linewidth", 2),
                marker=s.get("marker", "o"),
                markersize=s.get("markersize", 6),
                markerfacecolor=s.get("markerfacecolor", "none"),
                label=s["label"])

    ax.set_xticks(T_VALUES)
    ax.set_xlabel("size ratio (T)", labelpad=-1)
    ax.set_ylabel(ylabel, labelpad=-0.5, y=0.38)
    ax.set_ylim(bottom=0)
    if ypad > 1.0:
        ax.set_ylim(top=ax.get_ylim()[1] * ypad)
    ax.text(0.99, 0.85, fig_label, transform=ax.transAxes,
            fontsize=20, va="bottom", ha="right")
    if exp_text:
        ax.text(0.02, 0.97, exp_text, transform=ax.transAxes,
                va="top", ha="left", fontsize=20)

    output_file = DROPBOX_PATH / output_name
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_insert(data):
    _plot_single(data, "insert", "insert throughput (kOPS)", "size-ratio-insert.pdf", "(A)", ypad=1.15)


def plot_pq(data):
    _plot_single(data, "pq", "PQ throughput (kOPS)", "size-ratio-pq.pdf", "(B)", ypad=1.15)


def plot_rq(data):
    _plot_single(data, "rq", "RQ throughput (OPS)", "size-ratio-rq.pdf", "(C)", ypad=1.15)


def plot_legend():
    fig, ax = plt.subplots(figsize=(0.1, 0.05))
    for impl in implementations:
        key = normalize_name(impl)
        if key is None or key not in line_styles:
            continue
        s = line_styles[key]
        ax.plot([], [],
                color=s["color"],
                linestyle=s.get("linestyle", "-"),
                linewidth=s.get("linewidth", 2),
                marker=s.get("marker", "o"),
                markersize=s.get("markersize", 6),
                markerfacecolor=s.get("markerfacecolor", "none"),
                label=s["label"])
    handles, labels = ax.get_legend_handles_labels()
    leg_fig, leg_ax = plt.subplots(figsize=(2, 0.5))
    leg_ax.axis("off")
    leg_ax.legend(handles, labels, frameon=False, ncol=4,
                  loc="center", labelspacing=0.2, handlelength=1.4,
                  columnspacing=2, handletextpad=0.3)
    plt.close(fig)
    output_file = DROPBOX_PATH / "size-ratio-legend.pdf"
    leg_fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(leg_fig)
    print(f"Saved: {output_file}")


def dump_csv(data):
    import csv

    fields = ["impl", "size_ratio", "insert_kops", "pq_kops", "rq_mops"]
    output_file = CURR_DIR / TAG / "vary-size-ratio.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for impl in implementations:
            for i, t in enumerate(T_VALUES):
                writer.writerow({
                    "impl":         impl,
                    "size_ratio":   t,
                    "insert_kops":  f"{data[impl]['insert'][i]:.6f}",
                    "pq_kops":      f"{data[impl]['pq'][i]:.6f}",
                    "rq_mops":      f"{data[impl]['rq'][i]:.6f}",
                })
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    # data = load_data()
    # plot_insert(data)
    # plot_pq(data)
    # plot_rq(data)
    plot_legend()
    # dump_csv(data)
