import os
from pathlib import Path

import matplotlib.pyplot as plt

from plot import *
from plot.style import line_styles
from plot.rocksdb_stats import parse_rocksdb_log

TAG = "vary-bucket-count-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / TAG

PREFIX_LENGTH = 6
BUCKET_COUNTS = [10, 100, 1000, 10000, 100000] #, 250000, 500000, 1000000]. 1, 2, 5, , 

IMPLS = ["hashlinkedlist", "hashskiplist", "hashvector"]


def read_throughput(workload_log: Path) -> tuple[float, float]:
    phases = parse_rocksdb_log(str(workload_log))
    insert_time_ns = sum(p["meta"].get("insert_time", 0) for p in phases)
    pq_time_ns     = sum(p["meta"].get("point_query_time", 0) for p in phases)
    insert_count   = sum(p["tickers"].get("rocksdb.number.keys.written", 0) for p in phases)
    pq_count       = sum(p["tickers"].get("rocksdb.number.keys.read", 0) for p in phases)
    ins_tput = insert_count / (insert_time_ns / 1e9) / 1e6 if insert_time_ns > 0 else 0.0
    qry_tput = pq_count    / (pq_time_ns     / 1e9) / 1e6 if pq_time_ns     > 0 else 0.0
    return ins_tput, qry_tput


def load_data() -> dict[str, dict[str, list[float]]]:
    data = {impl: {"insert": [], "PQ": []} for impl in IMPLS}
    for bucket in BUCKET_COUNTS:
        subdir = EXP_DIR / f"B{bucket}-X{PREFIX_LENGTH}"
        for impl in IMPLS:
            workload_log = subdir / impl / "workload.log"
            if workload_log.exists():
                ins, qry = read_throughput(workload_log)
            else:
                print(f"Missing: {workload_log}")
                ins, qry = 0.0, 0.0
            data[impl]["insert"].append(ins)
            data[impl]["PQ"].append(qry)
    return data


def _apply_style(ax, ylabel):
    ax.set_xscale("log")
    ax.set_xticks(BUCKET_COUNTS, ["0.01", "0.1", "1", "10", "100"])
    # ax.set_xticklabels([str(b) if b < 1000 else f"{b//1000}k" if b < 1000000 else f"{b//1000000}M"
    #                     for b in BUCKET_COUNTS], rotation=45, ha="right", fontsize=12)
    ax.set_ylim(bottom=0)
    ax.set_xlabel("bucket count (k)", labelpad=-1)
    ax.set_ylabel(ylabel, labelpad=-0.5)
    ax.set_yticks([0, 0.5, 1.0, 1.5], ["0", "0.5", "1", "1.5"])
    ax.text(0.02, 0.97, r"$\times10^{6}$", transform=ax.transAxes,
            va="top", ha="left", fontsize=20)


def _plot_single(data, key, ylabel, output_name, fig_label):
    fig, ax = plt.subplots(figsize=(4, 2.8))
    for impl in IMPLS:
        s = line_styles[impl]
        ax.plot(BUCKET_COUNTS, data[impl][key],
                color=s["color"],
                linestyle=s.get("linestyle", "-"),
                linewidth=s.get("linewidth", 2),
                marker=s.get("marker", "o"),
                markersize=s.get("markersize", 6),
                markerfacecolor=s.get("markerfacecolor", "none"),
                label=s["label"])
    _apply_style(ax, ylabel)
    output_file = DROPBOX_PATH / output_name
    ax.text(0.99, 0.85, fig_label, transform=ax.transAxes,
                    fontsize=20, va="bottom", ha="right")
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_throughput():
    data = load_data()
    _plot_single(data, "insert", "insert throughput", "bucket-count-insert.pdf", "(A)")
    _plot_single(data, "PQ",     "PQ throughput",     "bucket-count-query.pdf", "(B)")


def plot_throughput_legend():
    fig, ax = plt.subplots(figsize=(0.1, 0.1))
    for impl in IMPLS:
        s = line_styles[impl]
        ax.plot([], [],
                color=s["color"],
                linestyle=s.get("linestyle", "-"),
                linewidth=s.get("linewidth", 2),
                marker=s.get("marker", "o"),
                markersize=s.get("markersize", 6),
                markerfacecolor=s.get("markerfacecolor", "none"),
                label=s["label"])
    handles, labels = ax.get_legend_handles_labels()
    leg_fig, leg_ax = plt.subplots(figsize=(4, 0.4))
    leg_ax.axis("off")
    leg_ax.legend(handles, labels, frameon=False, ncol=len(IMPLS),
                  loc="center", labelspacing=0.2, handlelength=1.4, columnspacing=1.0)
    plt.close(fig)
    output_file = DROPBOX_PATH / "bucket-count-legend.pdf"
    leg_fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(leg_fig)
    print(f"Saved: {output_file}")


def dump_csv():
    import csv

    data = load_data()
    fields = ["impl", "bucket_count"] + ["insert_mops", "pq_mops"]
    output_file = CURR_DIR / TAG / "vary-bucket-count.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for impl in IMPLS:
            for i, bucket in enumerate(BUCKET_COUNTS):
                writer.writerow({
                    "impl":         impl,
                    "bucket_count": bucket,
                    "insert_mops":  f"{data[impl]['insert'][i]:.6f}",
                    "pq_mops":      f"{data[impl]['PQ'][i]:.6f}",
                })
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    plot_throughput()
    plot_throughput_legend()
    dump_csv()
