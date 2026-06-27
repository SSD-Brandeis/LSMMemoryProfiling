import os
from pathlib import Path

import matplotlib.pyplot as plt

from plot import *
from plot.style import line_styles
from plot.rocksdb_stats import parse_rocksdb_log

TAG = "vary-common-prefix-exp-new"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / TAG

PREFIX_LENGTH  = 6
BUCKET_COUNT   = 100000
COMMON_PREFIX_LENS = [0, 1, 2, 3, 4, 5, 6, 7, 8]
RQ_COUNT = 1000

IMPLS = ["hashlinkedlist", "hashskiplist", "hashvector", "skiplist"]


def read_rq_throughput(workload_log: Path) -> float:
    phases = parse_rocksdb_log(str(workload_log))
    rq_time_ns = sum(p["meta"].get("range_query_time", 0) for p in phases)
    if rq_time_ns > 0:
        return RQ_COUNT / (rq_time_ns / 1e9) / 1e6  # MOPS
    return 0.0


def load_data() -> dict[str, list[float]]:
    data = {impl: [] for impl in IMPLS}
    for cpl in COMMON_PREFIX_LENS:
        subdir = EXP_DIR / f"CPL{cpl}-X{PREFIX_LENGTH}-B{BUCKET_COUNT}"
        for impl in IMPLS:
            workload_log = subdir / impl / "workload.log"
            if workload_log.exists():
                tput = read_rq_throughput(workload_log)
            else:
                print(f"Missing: {workload_log}")
                tput = 0.0
            data[impl].append(tput)
    return data


def plot_rq_throughput(fig_label="(E)"):
    data = load_data()

    fig, ax = plt.subplots(figsize=(4, 2.8))
    for impl in IMPLS:
        s = line_styles[impl]
        ax.plot(COMMON_PREFIX_LENS, data[impl],
                color=s["color"],
                linestyle=s.get("linestyle", "-"),
                linewidth=s.get("linewidth", 2),
                marker=s.get("marker", "o"),
                markersize=s.get("markersize", 6),
                markerfacecolor=s.get("markerfacecolor", "none"),
                label=s["label"])

    ax.set_xticks(COMMON_PREFIX_LENS)
    ax.set_ylim(bottom=0)
    ax.set_xlabel("common prefix length", labelpad=-1)
    ax.set_ylabel("RQ throughput", labelpad=-0.5)
    ax.set_yticks([0, 0.5, 1.0, 1.5], ["0", "0.5", "1", "1.5"])
    ax.text(0.02, 0.97, r"$\times10^{6}$", transform=ax.transAxes,
            va="top", ha="left", fontsize=20)
    ax.text(0.99, 0.85, fig_label, transform=ax.transAxes,
                    fontsize=20, va="bottom", ha="right")

    output_file = DROPBOX_PATH / "common-prefix-rq.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_rq_throughput_legend():
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
    output_file = DROPBOX_PATH / "common-prefix-legend.pdf"
    leg_fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(leg_fig)
    print(f"Saved: {output_file}")


def dump_csv():
    import csv

    data = load_data()
    fields = ["impl", "common_prefix_length", "rq_mops"]
    output_file = CURR_DIR / TAG / "vary-common-prefix.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for impl in IMPLS:
            for i, cpl in enumerate(COMMON_PREFIX_LENS):
                writer.writerow({
                    "impl":                 impl,
                    "common_prefix_length": cpl,
                    "rq_mops":              f"{data[impl][i]:.6f}",
                })
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    plot_rq_throughput()
    plot_rq_throughput_legend()
    dump_csv()
