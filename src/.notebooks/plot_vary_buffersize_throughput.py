import os
from pathlib import Path

import matplotlib.pyplot as plt

from plot import *
from plot.style import line_styles
from plot.rocksdb_stats import parse_rocksdb_log

TAG = "vary-buffersize-throughput-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / "vary-buffersize-throughput-exp"

BUFFER_SIZES_MB = [2, 4, 8, 16, 32, 64, 128]

N_INSERTS  = [80_000_000, 10_000_000, 10_000_000]  # per phase
N_PQ       = 10_000
N_RQ       = 1_000

implementations = [
    "vector-preallocated",
    "unsortedvector-preallocated",
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


def mean_latency_ns(time_ns, count):
    if time_ns == 0 or count == 0:
        return 0.0
    return time_ns / count


def read_phases(workload_log: Path):
    if not workload_log.exists():
        return None
    return parse_rocksdb_log(str(workload_log))


def load_data():
    insert = {impl: {0: [], 1: [], 2: []} for impl in implementations}
    pq     = {impl: [] for impl in implementations}
    rq     = {impl: [] for impl in implementations}

    for mb in BUFFER_SIZES_MB:
        for impl in implementations:
            log = EXP_DIR / f"{mb}MB" / impl / "workload_run.log"
            phases = read_phases(log)
            if phases is None:
                print(f"Missing: {log}")
                for p in range(3):
                    insert[impl][p].append(0.0)
                pq[impl].append(0.0)
                rq[impl].append(0.0)
                continue

            for p in range(3):
                if p < len(phases):
                    t = phases[p]["meta"].get("insert_time", 0)
                    insert[impl][p].append(mean_latency_ns(t, N_INSERTS[p]))
                else:
                    insert[impl][p].append(0.0)

            pq_lat = 0.0
            for ph in phases:
                t = ph["meta"].get("point_query_time", 0)
                if t > 0:
                    pq_lat = mean_latency_ns(t, N_PQ)
                    break
            pq[impl].append(pq_lat)

            rq_lat = 0.0
            for ph in phases:
                t = ph["meta"].get("range_query_time", 0)
                if t > 0:
                    rq_lat = mean_latency_ns(t, N_RQ)
                    break
            rq[impl].append(rq_lat)

    return insert, pq, rq


def _apply_style(ax, ylabel, ymax, log_scale=False):
    ax.set_xscale("log", base=2)
    ax.set_xticks(BUFFER_SIZES_MB[::2])
    ax.set_xticklabels([f"{mb}" for mb in BUFFER_SIZES_MB[::2]])
    ax.set_xlabel("buffer size (MB)", labelpad=-1)
    ax.set_ylabel(ylabel, labelpad=-0.5, loc="top")
    if log_scale:
        ax.set_yscale("log")
        ax.set_ylim(bottom=1e0, top=ymax)
    else:
        ax.set_ylim(bottom=0, top=ymax)


def _plot_lines(ax, data_per_impl):
    for impl in implementations:
        key = normalize_name(impl)
        if key is None or key not in line_styles:
            continue
        s = line_styles[key]
        ax.plot(BUFFER_SIZES_MB, data_per_impl[impl],
                color=s["color"],
                linestyle=s.get("linestyle", "-"),
                linewidth=s.get("linewidth", 2),
                marker=s.get("marker", "o"),
                markersize=s.get("markersize", 6),
                markerfacecolor=s.get("markerfacecolor", "none"),
                label=s["label"])


def plot_insert_phases(insert_data):
    phase_labels = ["(A)", "(A)", "(A)"]
    for phase_idx in range(3):
        fig, ax = plt.subplots(figsize=(4, 2.8))
        _plot_lines(ax, {impl: insert_data[impl][phase_idx] for impl in implementations})
        _apply_style(ax, "insert latency (ns)", ymax=1e6, log_scale=True)
        ax.text(0.99, 0.85, phase_labels[phase_idx], transform=ax.transAxes,
                fontsize=20, va="bottom", ha="right")
        output_file = DROPBOX_PATH / f"buffersize-insert-phase{phase_idx + 1}.pdf"
        fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
        plt.close(fig)
        print(f"Saved: {output_file}")


def plot_pq(pq_data, fig_label="(B)"):
    fig, ax = plt.subplots(figsize=(4, 2.8))
    _plot_lines(ax, pq_data)
    _apply_style(ax, "PQ latency (ns)", ymax=1e12, log_scale=True)
    ax.text(0.99, 0.85, fig_label, transform=ax.transAxes,
            fontsize=20, va="bottom", ha="right")
    output_file = DROPBOX_PATH / "buffersize-pq.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_rq(rq_data, fig_label="(C)"):
    fig, ax = plt.subplots(figsize=(4, 2.8))
    _plot_lines(ax, rq_data)
    _apply_style(ax, "RQ latency (ns)", ymax=1e12, log_scale=True)
    ax.text(0.99, 0.85, fig_label, transform=ax.transAxes,
            fontsize=20, va="bottom", ha="right")
    output_file = DROPBOX_PATH / "buffersize-rq.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.03)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_legend():
    fig, ax = plt.subplots(figsize=(0.1, 0.1))
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
    leg_fig, leg_ax = plt.subplots(figsize=(8, 0.8))
    leg_ax.axis("off")
    leg_ax.legend(handles, labels, frameon=False, ncol=4,
                  loc="center", labelspacing=0.2, handlelength=1.4,
                  columnspacing=2, handletextpad=0.3)
    plt.close(fig)
    output_file = DROPBOX_PATH / "buffersize-throughput-legend.pdf"
    leg_fig.savefig(output_file, bbox_inches="tight", pad_inches=0.03)
    plt.close(leg_fig)
    print(f"Saved: {output_file}")


def load_tail_compaction_data():
    tail_write = {impl: [] for impl in implementations}
    compaction  = {impl: [] for impl in implementations}

    for mb in BUFFER_SIZES_MB:
        for impl in implementations:
            log = EXP_DIR / f"{mb}MB" / impl / "workload_run.log"
            phases = read_phases(log)
            if phases is None:
                print(f"Missing: {log}")
                tail_write[impl].append(0.0)
                compaction[impl].append(0.0)
                continue

            # aggregate P99 across all phases (take the max — worst-case tail)
            tw = max(
                (p["histograms"].get("rocksdb.db.write.micros", {}).get("P99", 0.0)
                 for p in [phases[0]]),
                default=0.0,
            )
            cp = max(
                (p["histograms"].get("rocksdb.compaction.times.micros", {}).get("P99", 0.0) / 1e6
                 for p in [phases[0]]),
                default=0.0,
            )
            
            tail_write[impl].append(tw)
            compaction[impl].append(cp)

    return tail_write, compaction


def plot_tail_write_latency(tail_write_data, fig_label=""):
    ymax = 15
    fig, ax = plt.subplots(figsize=(4, 2.8))
    _plot_lines(ax, tail_write_data)
    _apply_style(ax, "P99 write latency ($\\mu$s)", ymax=ymax)

    # annotate clipped spikes (values above ymax)
    for impl in implementations:
        for mb, val in zip(BUFFER_SIZES_MB, tail_write_data[impl]):
            if val > ymax:
                ax.annotate(
                    f"{val:.0f}",
                    xy=(mb, ymax),
                    xytext=(10, -4),
                    textcoords="offset points",
                    ha="center", va="top",
                    fontsize=16,
                    color=line_styles[normalize_name(impl)]["color"],
                    clip_on=False,
                )

    if fig_label:
        ax.text(0.99, 0.85, fig_label, transform=ax.transAxes,
                fontsize=20, va="bottom", ha="right")
    output_file = DROPBOX_PATH / "buffersize-tail-write.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_compaction_latency(compaction_data, fig_label=""):
    fig, ax = plt.subplots(figsize=(4, 2.8))
    _plot_lines(ax, compaction_data)
    _apply_style(ax, "P99 compaction (s)", ymax=100)
    if fig_label:
        ax.text(0.99, 0.85, fig_label, transform=ax.transAxes,
                fontsize=20, va="bottom", ha="right")
    output_file = DROPBOX_PATH / "buffersize-compaction.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def load_write_stall_data():
    stalls = {impl: [] for impl in implementations}

    for mb in BUFFER_SIZES_MB:
        for impl in implementations:
            log = EXP_DIR / f"{mb}MB" / impl / "workload_run.log"
            phases = read_phases(log)
            if phases is None:
                print(f"Missing: {log}")
                stalls[impl].append(0.0)
                continue

            # total stall micros accumulated during the insert phase
            v = phases[0]["histograms"].get("rocksdb.db.write.stall", {}).get("SUM", 0.0) / 1e6
            stalls[impl].append(float(v))

    return stalls


def plot_write_stalls(stall_data, fig_label=""):
    fig, ax = plt.subplots(figsize=(4, 2.8))
    _plot_lines(ax, stall_data)
    _apply_style(ax, "total write stall (s)", ymax=1500)
    if fig_label:
        ax.text(0.99, 0.85, fig_label, transform=ax.transAxes,
                fontsize=20, va="bottom", ha="right")
    output_file = DROPBOX_PATH / "buffersize-write-stalls.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def dump_csv(insert_data, pq_data, rq_data):
    import csv

    fields = ["impl", "buffer_size_mb",
              "insert_phase1_ns", "insert_phase2_ns", "insert_phase3_ns",
              "pq_ns", "rq_ns"]
    output_file = CURR_DIR / TAG / "vary-buffersize-latency.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for impl in implementations:
            for i, mb in enumerate(BUFFER_SIZES_MB):
                writer.writerow({
                    "impl":              impl,
                    "buffer_size_mb":    mb,
                    "insert_phase1_ns":  f"{insert_data[impl][0][i]:.4f}",
                    "insert_phase2_ns":  f"{insert_data[impl][1][i]:.4f}",
                    "insert_phase3_ns":  f"{insert_data[impl][2][i]:.4f}",
                    "pq_ns":             f"{pq_data[impl][i]:.4f}",
                    "rq_ns":             f"{rq_data[impl][i]:.4f}",
                })
    print(f"Saved: {output_file}")


def dump_tail_stall_csv(tail_write_data, compaction_data, stall_data):
    import csv

    fields = ["impl", "buffer_size_mb",
              "tail_write_p99_us", "compaction_p99_us", "write_stall_s"]
    output_file = CURR_DIR / TAG / "vary-buffersize-tail-stall.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for impl in implementations:
            for i, mb in enumerate(BUFFER_SIZES_MB):
                writer.writerow({
                    "impl":                impl,
                    "buffer_size_mb":      mb,
                    "tail_write_p99_us":   f"{tail_write_data[impl][i]:.4f}",
                    "compaction_p99_us":   f"{compaction_data[impl][i]:.4f}",
                    "write_stall_s":       f"{stall_data[impl][i]:.4f}",
                })
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    # insert_data, pq_data, rq_data = load_data()
    # tail_write, compaction = load_tail_compaction_data()
    # stall_data = load_write_stall_data()
    # plot_insert_phases(insert_data)
    # plot_pq(pq_data)
    # plot_rq(rq_data)
    # plot_compaction_latency(compaction, fig_label="(D)")
    # plot_write_stalls(stall_data, fig_label="(E)")
    # plot_tail_write_latency(tail_write, fig_label="(F)")
    plot_legend()
    # dump_csv(insert_data, pq_data, rq_data)
    # dump_tail_stall_csv(tail_write, compaction, stall_data)
