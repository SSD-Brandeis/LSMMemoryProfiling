import os
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt

from plot.style import line_styles

TAG = "snapshot-compare-sort-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / TAG

WINDOW = 200

# (label, stats_dir, snapshot_log or None, sort_log or None)
IMPLS = [
    ("\\texttt{V-Qsort}",  "vector-preallocated",        "snapshot_ns.log",                "sort_ns.log"),
    ("\\texttt{V-Qscan}", "unsortedvector-preallocated", "snapshot_ns_unsortedvector.log", None),
    ("\\texttt{V-Sorted}", "sortedvector-preallocated",   "snapshot_ns_sortedvector.log",   None),
]


def read_q_latencies(stats_file: Path) -> np.ndarray:
    latencies = []
    with open(stats_file) as f:
        for line in f:
            if line.startswith("Q:"):
                latencies.append(int(line[2:].strip()))
    return np.array(latencies)


def read_snap_latencies(snap_file: Path) -> np.ndarray:
    latencies = []
    with open(snap_file) as f:
        for line in f:
            line = line.strip()
            if line.startswith("SNAP:"):
                latencies.append(int(line[5:].strip()))
    return np.array(latencies)


def read_sort_latencies(sort_file: Path) -> np.ndarray:
    latencies = []
    with open(sort_file) as f:
        for line in f:
            line = line.strip()
            if line.startswith("SORT:"):
                latencies.append(int(line[5:].strip()))
    return np.array(latencies)


def rolling_stats(arr: np.ndarray, window: int):
    half = window // 2
    n = len(arr)
    x, p0, p95, mean = [], [], [], []
    for i in range(half, n - half):
        w = arr[i - half : i + half]
        x.append(i + 1)
        p0.append(np.percentile(w, 0))
        p95.append(np.percentile(w, 95))
        mean.append(np.mean(w))
    return np.array(x), np.array(p0), np.array(p95), np.array(mean)


def plot_snapshot_vs_latency():
    snap_file = EXP_DIR / "vector-preallocated" / "snapshot_ns.log"

    vec_q = read_q_latencies(EXP_DIR / "vector-preallocated" / "stats.log")
    skip_q = read_q_latencies(EXP_DIR / "skiplist" / "stats.log")
    snap_ns = read_snap_latencies(snap_file)

    print(f"vector PQs:   {len(vec_q)}")
    print(f"skiplist PQs: {len(skip_q)}")
    print(f"SNAP entries: {len(snap_ns)}")

    vec_color = line_styles["vector"]["color"]
    skip_color = line_styles["skiplist"]["color"]
    snap_color = "#d62728"

    fig, ax = plt.subplots(figsize=(5, 2.8))

    series = [
        (vec_q, vec_color, "solid", "vector (total PQ)"),
        (skip_q, skip_color, "--", "skiplist (total PQ)"),
    ]
    for arr, color, ls, label in series:
        x, p0, p95, mean = rolling_stats(arr, WINDOW)
        ax.fill_between(x, p0, p95, color=color, alpha=0.15)
        ax.plot(x, mean, color=color, linestyle=ls, linewidth=1.4, label=label)

    if len(snap_ns) > WINDOW:
        x, p0, p95, mean = rolling_stats(snap_ns, WINDOW)
        ax.fill_between(x, p0, p95, color=snap_color, alpha=0.15)
        ax.plot(
            x,
            mean,
            color=snap_color,
            linestyle=":",
            linewidth=1.4,
            label="vector (snapshot copy)",
        )
    elif len(snap_ns) > 0:
        ax.scatter(
            np.arange(1, len(snap_ns) + 1),
            snap_ns,
            color=snap_color,
            s=4,
            label="vector (snapshot copy)",
            zorder=3,
        )

    n_pqs = max(len(vec_q), len(skip_q))
    ax.set_xlim(0, n_pqs)
    ax.set_ylim(bottom=0)
    ax.set_xticks(
        [0, n_pqs // 2, n_pqs],
        ["0", str(n_pqs // 2 // 1000) + "k", str(n_pqs // 1000) + "k"],
    )
    ax.set_xlabel("point query index", labelpad=2)
    ax.set_ylabel("latency (ns)", labelpad=-1, loc="top")
    ax.legend(frameon=False, fontsize=7, loc="upper right")

    output_file = CURR_DIR / TAG / "snapshot-vs-latency.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_snapshot_stacked():
    bar_labels, mean_snap, mean_sort, mean_rest, mean_total = [], [], [], [], []

    for label, subdir, snap_fname, sort_fname in IMPLS:
        stats_file = EXP_DIR / subdir / "stats.log"
        if not stats_file.exists():
            print(f"Missing: {stats_file}")
            continue

        q = read_q_latencies(stats_file)
        mean_q = float(np.mean(q))

        if snap_fname is not None:
            snap_file = EXP_DIR / subdir / snap_fname
            mean_s = float(np.mean(read_snap_latencies(snap_file))) if snap_file.exists() else 0.0
        else:
            mean_s = 0.0

        if sort_fname is not None:
            sort_file = EXP_DIR / subdir / sort_fname
            if not sort_file.exists():
                print(f"  WARNING: sort log missing: {sort_file} — re-run the experiment after adding sort timing")
                mean_so = 0.0
            else:
                entries = read_sort_latencies(sort_file)
                mean_so = float(np.mean(entries)) if len(entries) > 0 else 0.0
                print(f"  sort log: {len(entries)} entries, mean={mean_so:.0f} ns")
        else:
            mean_so = 0.0

        mean_r = max(mean_q - mean_s - mean_so, 0.0)

        print(
            f"{label:16s}  mean_Q={mean_q:.0f} ns  snap={mean_s:.0f} ns  "
            f"sort={mean_so:.0f} ns  rest={mean_r:.0f} ns"
        )

        bar_labels.append(label)
        mean_snap.append(mean_s)
        mean_sort.append(mean_so)
        mean_rest.append(mean_r)
        mean_total.append(mean_q)

    snap_color = "#006d2c"
    sort_color = "#e60a0a"
    rest_color = "#601fb4"

    n = len(bar_labels)
    x = np.arange(n)
    width = 0.22
    offsets = [-width, 0, width]  # snapshot, sort, rest

    fig, ax1 = plt.subplots(figsize=(4, 2.8))

    def _annotate(ax, cx, val):
        if val < 0.1:
            ax.text(cx, val + 0.01, f"{val:.4f}",
                    ha="center", va="bottom", fontsize=14, rotation=90)

    for i, (mt, ms, mso, mr) in enumerate(zip(mean_total, mean_snap, mean_sort, mean_rest)):
        t = mt if mt > 0 else 1.0
        fs  = ms  / t
        fso = mso / t
        fr  = mr  / t

        ax1.bar(x[i] + offsets[0], fs,  width, facecolor="none",
                edgecolor=snap_color, hatch="/", linewidth=1.2,
                label="snapshot" if i == 0 else None)
        _annotate(ax1, x[i] + offsets[0], fs)

        ax1.bar(x[i] + offsets[1], fso, width, facecolor="none",
                edgecolor=sort_color, hatch="\\\\\\\\", linewidth=1.2,
                label="sort"     if i == 0 else None)
        _annotate(ax1, x[i] + offsets[1], fso)

        ax1.bar(x[i] + offsets[2], fr,  width, facecolor="none",
                edgecolor=rest_color, hatch="--", linewidth=1.2,
                label="search"   if i == 0 else None)
        _annotate(ax1, x[i] + offsets[2], fr)

    for i, mt in enumerate(mean_total):
        ax1.text(x[i], 1.02, f"{mt/(10**6):.0f} ms",
                 ha="center", va="bottom", fontsize=18)

    ax1.set_xticks(x)
    ax1.set_xticklabels(bar_labels, fontsize=19)
    ax1.set_ylabel("PQ latency split", labelpad=2)
    ax1.set_ylim(0, 1.2)
    ax1.set_yticks([0, 0.5, 1], ["0", "0.5", "1"])

    ax1.legend(
        frameon=False,
        ncol=3,
        loc="upper center",
        bbox_to_anchor=(0.42, 1.15),
        labelspacing=0.1,
        handlelength=1.2,
        columnspacing=0.1,
        borderaxespad=0,
        borderpad=0,
        handletextpad=0.2,
    )

    output_file = CURR_DIR / TAG / "snapshot-stacked.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    plot_snapshot_vs_latency()
    plot_snapshot_stacked()
