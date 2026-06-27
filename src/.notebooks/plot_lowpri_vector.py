import os
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

from plot.style import line_styles

TAG = "lowpri-vector-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / TAG

WINDOW = 2

# (lpdir, impl, lowpri, label)
series_config = [
    ("lowpri-0", "vector-preallocated", 0, "vector-static"),
    # ("lowpri-1", "vector-preallocated", 1, "vector (lowpri=1)"),
    ("lowpri-0", "vector-dynamic",      0, "vector-dynamic"),
    # ("lowpri-1", "vector-dynamic",      1, "vector-dynamic (lowpri=1)"),
]

colors = {
    "vector-preallocated": line_styles["vector"]["color"],    # dark green
    "vector-dynamic":      "#601fb4",                         # blue
}

markers = {
    "vector-preallocated": "o",
    "vector-dynamic":      "^",
}


def read_insert_latencies(stats_file: Path) -> np.ndarray:
    cache = stats_file.with_suffix(".I10k.npy")
    if cache.exists() and cache.stat().st_mtime >= stats_file.stat().st_mtime:
        return np.load(cache)
    latencies = []
    read_only_first_10k = 10_000
    with open(stats_file) as f:
        for line in f:
            if len(latencies) >= read_only_first_10k:
                break
            if line.startswith("I:"):
                latencies.append(int(line[2:].strip()))
    arr = np.array(latencies)
    np.save(cache, arr)
    return arr


def rolling_stats(arr: np.ndarray, window: int):
    half = window // 2
    n = len(arr)
    x, p0, p95, mean = [], [], [], []
    for i in range(half, n - half):
        w = arr[i - half: i + half]
        x.append(i + 1)
        p0.append(np.percentile(w, 0))
        p95.append(np.percentile(w, 95))
        mean.append(np.mean(w))
    return np.array(x), np.array(p0), np.array(p95), np.array(mean)


def plot_insert_latency():
    fig, ax = plt.subplots(figsize=(4, 2.8))

    linestyles = {0: "solid", 1: "--"}
    peaks = []  # (x_peak, peak_val, color) for series that exceed Y_MAX
    for lpdir, impl, lowpri, label in series_config:
        stats_file = EXP_DIR / lpdir / impl / "rocksdb_stats.log"
        if not stats_file.exists():
            print(f"Missing: {stats_file}")
            continue

        arr = read_insert_latencies(stats_file)
        print(f"  {lpdir}/{impl}: {len(arr)} inserts")

        color = colors[impl]
        ax.plot(np.arange(1, len(arr) + 1), arr, color=color, linestyle=linestyles[lowpri], linewidth=1, label=label)

    ax.set_xticks([0, 5_000, 10_000], ["0", "5", "10"])
    ax.set_yticks([0, 20_000, 40_000], ["0", "20", "40"])
    ax.set_xlabel("insert (k)", labelpad=-1)
    ax.set_ylabel("latency (ns)", labelpad=-0.5)
    ax.legend(loc="upper left", bbox_to_anchor=(0.03, 0.85), handlelength=1.0,
                frameon=False, borderaxespad=0, handletextpad=0.1, borderpad=0, labelspacing=0.1, fontsize=20)
    ax.text(
        0.02,
        0.97,
        r"$\times10^{3}$",
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=18,
    )
    ax.text(0.99, 0.98, "(A)", transform=ax.transAxes,
            va="top", ha="right", fontsize=20)

    output_file = CURR_DIR / TAG / "lowpri-insert-latency.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_insert_latency_scatter():
    fig, ax = plt.subplots(figsize=(4, 2.8))

    for lpdir, impl, lowpri, label in series_config:
        stats_file = EXP_DIR / lpdir / impl / "rocksdb_stats.log"
        if not stats_file.exists():
            print(f"Missing: {stats_file}")
            continue

        arr = read_insert_latencies(stats_file)
        print(f"  {lpdir}/{impl}: {len(arr)} inserts")

        color = colors[impl]
        marker = markers[impl]
        facecolor = color if lowpri == 1 else "none"
        alpha = 0.4 if lowpri == 1 else 1.0
        x = np.arange(1, len(arr) + 1)
        ax.scatter(x, arr, marker=marker, facecolors=facecolor, edgecolors=color,
                   s=50, linewidths=1.5, alpha=alpha, label=label, rasterized=True)

    ax.set_ylim(bottom=0)
    ax.set_xticks([0, 300_000, 600_000, 900_000], ["0", "300", "600", "900"])
    ax.set_yticks([0, 2_000_000, 4_000_000], ["0", "2", "4"])
    ax.set_xlabel("insert (k)", labelpad=2)
    ax.set_ylabel("latency (ns)")
    ax.text(
        0.01,
        0.97,
        r"$\times10^{6}$",
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=20,
    )


    output_file = CURR_DIR / TAG / "lowpri-insert-latency-scatter.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02, dpi=300)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_insert_latency_raw():
    fig, ax = plt.subplots(figsize=(4, 2.8))

    linestyles = {0: "solid", 1: "--"}
    for lpdir, impl, lowpri, label in series_config:
        stats_file = EXP_DIR / lpdir / impl / "rocksdb_stats.log"
        if not stats_file.exists():
            print(f"Missing: {stats_file}")
            continue

        arr = read_insert_latencies(stats_file)
        print(f"  {lpdir}/{impl}: {len(arr)} inserts")

        color = colors[impl]
        x = np.arange(1, len(arr) + 1)
        ax.plot(x, arr, color=color, linestyle=linestyles[lowpri], linewidth=2, label=label)

    ax.set_ylim(bottom=0)
    ax.set_xticks([0, 300_000, 600_000, 900_000], ["0", "300", "600", "900"])
    ax.set_yticks([0, 2_000_000, 4_000_000], ["0", "2", "4"])
    ax.set_xlabel("insert (k)", labelpad=2)
    ax.set_ylabel("latency (ns)", labelpad=-1)
    ax.text(
        0.02,
        0.97,
        r"$\times10^{6}$",
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=20,
    )

    output_file = CURR_DIR / TAG / "lowpri-insert-latency-raw.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_scatter_legend():
    vec_c  = colors["vector-preallocated"]
    dyn_c  = colors["vector-dynamic"]
    legend_elements = [
        Line2D([0], [0], marker="o", color="w", label="vector-static (priority writes)",
               markerfacecolor="none", markeredgecolor=vec_c,
               markersize=8, markeredgewidth=1.5),
        Line2D([0], [0], marker="o", color="w", label="vector-static (priority compaction)",
               markerfacecolor=vec_c, markeredgecolor=vec_c,
               markersize=8, markeredgewidth=1.5, alpha=0.4),
        Line2D([0], [0], marker="^", color="w", label="vector-dynamic (priority writes)",
               markerfacecolor="none", markeredgecolor=dyn_c,
               markersize=8, markeredgewidth=1.5),
        Line2D([0], [0], marker="^", color="w", label="vector-dynamic (priority compactions)",
               markerfacecolor=dyn_c, markeredgecolor=dyn_c,
               markersize=8, markeredgewidth=1.5, alpha=0.4),
    ]

    fig_legend = plt.figure(figsize=(5, 0.6))
    ax_legend = fig_legend.add_subplot(111)
    ax_legend.axis("off")
    ax_legend.legend(
        handles=legend_elements,
        loc="center",
        frameon=False,
        ncol=2,
        borderaxespad=0,
        labelspacing=0.1,
        borderpad=0,
        columnspacing=0.1,
        handletextpad=0.1,
    )

    legend_file = CURR_DIR / TAG / "lowpri-scatter-legend.pdf"
    fig_legend.savefig(legend_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"Saved: {legend_file}")


def dump_csv():
    import csv

    fields = ["series", "impl", "lowpri", "n_inserts", "mean_ns", "median_ns", "p95_ns", "p99_ns", "max_ns"]
    output_file = CURR_DIR / TAG / "lowpri-insert-latency.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for lpdir, impl, lowpri, label in series_config:
            stats_file = EXP_DIR / lpdir / impl / "rocksdb_stats.log"
            if not stats_file.exists():
                print(f"Missing: {stats_file}")
                continue
            arr = read_insert_latencies(stats_file)
            writer.writerow({
                "series":    label,
                "impl":      impl,
                "lowpri":    lowpri,
                "n_inserts": len(arr),
                "mean_ns":   f"{arr.mean():.2f}",
                "median_ns": f"{np.percentile(arr, 50):.2f}",
                "p95_ns":    f"{np.percentile(arr, 95):.2f}",
                "p99_ns":    f"{np.percentile(arr, 99):.2f}",
                "max_ns":    f"{arr.max():.2f}",
            })
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    plot_insert_latency()
    dump_csv()
    # plot_insert_latency_scatter()
    # plot_scatter_legend()
    # plot_insert_latency_raw()
