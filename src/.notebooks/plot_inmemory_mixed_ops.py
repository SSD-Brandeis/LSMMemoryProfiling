import os
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.patches import Patch

from plot import *
from plot.rocksdb_stats import parse_rocksdb_log
from plot.style import hatch_map, line_styles

TAG = "inmemory-mixed-ops-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / TAG

# Op counts matching inmemory-mixed-ops-exp.sh
BUFFER_SIZE_MB = 128
ENTRY_SIZE = 128
HASHSKIPLIST_OVERHEAD_PCT = 29
INSERTS = (
    BUFFER_SIZE_MB
    * 1024
    * 1024
    * (100 - HASHSKIPLIST_OVERHEAD_PCT)
    // (ENTRY_SIZE * 100)
)
TOTAL_PQ = 50_000 + 50_000  # empty + non-empty, logged together
RQ = 1_000

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


def safe_tp(ops, time_ns):
    if time_ns == 0:
        return 0.0
    return ops / (time_ns / 1e9)


def process_impl(args):
    """
    Each workload.log has a single logged phase.
    Returns per-operation throughput: [insert_tp, pq_tp, rq_tp].
    Empty and non-empty PQs are combined into pq_tp (logged as one metric).
    """
    workload_type, impl = args
    log_file = EXP_DIR / workload_type / impl / "workload.log"
    if not log_file.exists():
        return workload_type, impl, None

    phases = parse_rocksdb_log(str(log_file))
    if not phases:
        return workload_type, impl, None

    meta = phases[0]["meta"]
    insert_tp = safe_tp(INSERTS, meta.get("insert_time", 0))
    pq_tp = safe_tp(TOTAL_PQ, meta.get("point_query_time", 0))
    rq_tp = safe_tp(RQ, meta.get("range_query_time", 0))
    workload_sec = meta.get("workload_time", 0) / 1e9

    return workload_type, impl, [insert_tp, pq_tp, rq_tp, workload_sec]


def collect_data():
    tasks = [
        (wl, impl)
        for wl in ("sequential", "mixed")
        for impl in implementations
        if (EXP_DIR / wl / impl / "workload.log").exists()
    ]

    results = {"sequential": {}, "mixed": {}}
    with ProcessPoolExecutor(max_workers=4) as executor:
        for wl, impl, tps in executor.map(process_impl, tasks):
            if tps is not None:
                results[wl][impl] = tps
                print(
                    f"  [{wl}] {impl}: "
                    f"I={tps[0]:.0f}  PQ={tps[1]:.0f}  RQ={tps[2]:.0f} ops/s"
                )

    return results


def plot_insert_throughput(results):
    _plot_single_op(
        results,
        op_index=0,
        title="inserts",
        y_label="throughput (MOpS)",
        y_ticks=[0, 500_000, 1_000_000],
        y_tick_labels=["0", "0.5", "1"],
        output_name="inmemory-insert-throughput.pdf",
    )


def _plot_single_op(
    results,
    op_index,
    title,
    y_label,
    y_ticks,
    y_tick_labels,
    output_name,
    is_log_scale=False,
    filter_impls=None,
):
    impls = [
        impl
        for impl in (filter_impls or implementations)
        if impl in results.get("sequential", {}) or impl in results.get("mixed", {})
    ]
    num_impls = len(impls)

    workload_types = ["sequential", "mixed"]
    x = np.arange(len(workload_types))
    bar_width = 0.9 / num_impls

    fig, ax = plt.subplots(figsize=(3, 2.8))

    for j, impl in enumerate(impls):
        key = normalize_name(impl)
        if key is None or key not in line_styles:
            continue
        style = line_styles[key]
        color = style["color"]

        heights = [results[wl].get(impl, [0, 0, 0])[op_index] for wl in workload_types]
        xpos = x + j * bar_width

        ax.bar(
            xpos,
            heights,
            bar_width - 0.008,
            facecolor="none" if impl != "vector-preallocated" else color,
            edgecolor=color,
            hatch=hatch_map.get(key, None),
            label=style["label"],
        )

    ax.set_xticks(x + bar_width * (num_impls - 1) / 2)
    ax.set_xticklabels(workload_types)
    if not is_log_scale:
        ax.set_yticks(y_ticks)
        ax.set_yticklabels(y_tick_labels)
        ax.set_ylim(0, None)
    # ax.set_title(title, fontsize=20)
    ax.set_ylabel(f"{y_label}", labelpad=-1, loc="top")

    if is_log_scale:
        ax.set_yscale("log")
        ax.set_ylim(1e0)

    output_file = CURR_DIR / TAG / output_name
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.06)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_pq_throughput(results):
    _plot_single_op(
        results,
        op_index=1,
        title="PQ",
        y_label="throughput (OPS)",
        y_ticks=[0, 50_000, 100_000, 150_000],
        y_tick_labels=["0", "50", "100", "150"],
        output_name="inmemory-pq-throughput.pdf",
        is_log_scale=True,
    )


def plot_rq_throughput(results):
    _plot_single_op(
        results,
        op_index=2,
        title="RQ",
        y_label="throughput (OPS)",
        y_ticks=[0, 50, 100],
        y_tick_labels=["0", "50", "100"],
        output_name="inmemory-rq-throughput.pdf",
    )


def plot_workload_time(results):
    _plot_single_op(
        results,
        op_index=3,
        title="total workload time",
        y_label="execution time (s)",
        y_ticks=[0, 1, 2, 3],
        y_tick_labels=["0", "1", "2", "3"],
        output_name="inmemory-workload-time.pdf",
        is_log_scale=True,
        # filter_impls=["vector-preallocated"],
    )


def plot_pq_latency_over_time():
    WINDOW = 5
    # (subdir relative to EXP_DIR, label, color, linestyle)
    series = [
        ("sequential/vector-preallocated", "sequential",       "#601fb4", "--"),
        ("mixed/vector-preallocated",      "mixed",            "#006d2c", "solid"),
        ("sequential/vector-optimized",    "optimized", "#601fb4", ":"),
    ]

    fig, ax = plt.subplots(figsize=(4, 2.8))

    for subdir, label, color, ls in series:
        stats_file = EXP_DIR / subdir / "stats.log"
        if not stats_file.exists():
            print(f"Missing: {stats_file}")
            continue

        latencies = []
        with open(stats_file) as f:
            for line in f:
                if line.startswith("Q:"):
                    latencies.append(int(line[2:].strip()))

        arr = np.array(latencies)
        n = len(arr)
        half = WINDOW // 2

        x_win, p0_win, p95_win, mean_win = [], [], [], []
        for i in range(half, n - half):
            window = arr[i - half : i + half]
            x_win.append(i + 1)
            p0_win.append(np.percentile(window, 0))
            p95_win.append(np.percentile(window, 95))
            mean_win.append(np.mean(window))

        x_win = np.array(x_win)

        ax.fill_between(x_win, p0_win, p95_win, color=color, alpha=0.3, rasterized=True)
        ax.plot(
            x_win, mean_win, color=color, linestyle=ls, 
            linewidth=5 if "optimized" in label else 1.4, 
            label=None if "optimized" in label else label
        )

    ax.set_xticks([0, 50_000, 100_000], ["0", "50", "100"])
    ax.set_yticks([0, 2 * (10**8), 4 * (10**8), 6 * (10**8)], ["0", "2", "4", "6"])
    ax.set_xlabel("point query (k)", labelpad=-1)
    ax.set_ylabel("latency (ns)")
    ax.set_ylim(bottom=0)
    ax.set_xlim(left=0, right=100000)
    ax.text(
        0.02,
        0.97,
        r"$\times10^{8}$",
        transform=ax.transAxes,
        va="top",
        ha="left",
        fontsize=20,
    )
    ax.legend(
        bbox_to_anchor=(0.6, 0.7),
        frameon=False,
        ncol=1,
        borderaxespad=0,
        labelspacing=0.1,
        borderpad=0,
        columnspacing=2,
        handletextpad=0.2,
        fontsize=20,
    )
    ax.text(0.99, 0.02, f"rolling window: {WINDOW}pts", transform=ax.transAxes,
            va="bottom", ha="right", fontsize=18)
    ax.text(0.99, 0.98, "(C)", transform=ax.transAxes,
            va="top", ha="right", fontsize=20)

    output_file = CURR_DIR / TAG / "inmemory-pq-latency.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


VECTOR_IMPLS = [
    "vector-preallocated",
    "unsortedvector-preallocated",
    "sortedvector-preallocated",
]

VECTOR_LABELS = {
    "vector":             "vector",
    "unsortedvector":     "unsorted",
    "alwayssortedvector": "sorted",
}


def _read_latencies(stats_file: Path, prefix: str, cache_suffix: str) -> np.ndarray:
    cache = stats_file.with_suffix(cache_suffix)
    if cache.exists() and cache.stat().st_mtime >= stats_file.stat().st_mtime:
        return np.load(cache)
    latencies = []
    with open(stats_file) as f:
        for line in f:
            if line.startswith(prefix):
                latencies.append(int(line[len(prefix):].strip()))
    arr = np.array(latencies)
    np.save(cache, arr)
    return arr


def _bxp_stats(arr: np.ndarray) -> dict:
    return {
        "med":    float(np.percentile(arr, 50)),
        "q1":     float(np.percentile(arr, 25)),
        "q3":     float(np.percentile(arr, 75)),
        "whislo": float(np.percentile(arr, 5)),
        "whishi": float(np.percentile(arr, 95)),
        "fliers": [],
    }


def _draw_boxes(ax, prefix, cache_sfx):
    """Draw bxp boxes for all VECTOR_IMPLS on a single axis."""
    workloads     = ["sequential", "mixed"]
    n_impls       = len(VECTOR_IMPLS)
    box_width     = 0.22
    group_gap     = 1.0
    group_centers = np.arange(len(workloads)) * group_gap
    offsets       = np.linspace(-(n_impls - 1) / 2, (n_impls - 1) / 2, n_impls) * box_width

    for j, impl in enumerate(VECTOR_IMPLS):
        key   = normalize_name(impl)
        color = line_styles[key]["color"] if key in line_styles else "gray"
        hatch = hatch_map.get(key, None)

        bxp_data, positions = [], []
        for g, wl in enumerate(workloads):
            stats_file = EXP_DIR / wl / impl / "stats.log"
            if not stats_file.exists():
                print(f"Missing: {stats_file}")
                continue
            arr = _read_latencies(stats_file, prefix, cache_sfx)
            if len(arr) == 0:
                continue
            bxp_data.append(_bxp_stats(arr))
            positions.append(group_centers[g] + offsets[j])

        if not bxp_data:
            continue

        bp = ax.bxp(
            bxp_data,
            positions=positions,
            widths=box_width * 0.85,
            showfliers=False,
            patch_artist=True,
        )
        for patch in bp["boxes"]:
            patch.set_facecolor("none")
            patch.set_edgecolor(color)
            patch.set_hatch(hatch)
            patch.set_linewidth(1.2)
        for part in ("medians", "whiskers", "caps"):
            for line in bp[part]:
                line.set_color(color)
                line.set_linewidth(1.5)

    return group_centers, group_gap


def _plot_vector_boxplot_panel(ax_top, ax_bot, prefix, cache_sfx, ylabel, panel_title,
                               ylim_top, ylim_bot, fig_label=None):
    workloads = ["sequential", "mixed"]
    group_centers, group_gap = _draw_boxes(ax_top, prefix, cache_sfx)
    _draw_boxes(ax_bot, prefix, cache_sfx)

    xlim = (group_centers[0] - group_gap * 0.55, group_centers[-1] + group_gap * 0.55)

    # Y limits and log scale
    ax_top.set_yscale("log")
    ax_bot.set_yscale("log")
    ax_top.set_ylim(*ylim_top)
    ax_bot.set_ylim(*ylim_bot)
    ax_bot.minorticks_off()

    # Spine treatment for broken axis
    ax_top.spines["bottom"].set_visible(False)
    ax_bot.spines["top"].set_visible(False)
    ax_top.tick_params(axis="x", bottom=False, labelbottom=False)

    # Small diagonal break markers (d=0.02 keeps them tight)
    d = 0.02
    for ax, y0, y1 in [(ax_top, -d, d), (ax_bot, 1-d, 1+d)]:
        for x0 in (0, 1):
            ax.plot((x0 - d, x0 + d), (y0, y1),
                    transform=ax.transAxes, color="k", clip_on=False, linewidth=1)

    # X ticks only on bottom stub; single y-tick at bottom to mark 10^0
    ax_bot.set_xticks(group_centers)
    ax_bot.set_xticklabels(workloads)
    ax_bot.set_yticks([1])
    # ax_bot.set_yticklabels(["1"])
    ax_top.set_xlim(*xlim)
    ax_bot.set_xlim(*xlim)

    ax_top.set_ylabel(ylabel, labelpad=-1, y=0.4)
    if fig_label:
        ax_bot.text(0.95, 0.2, fig_label, transform=ax_bot.transAxes,
                    fontsize=20, va="bottom", ha="right")


def plot_vector_boxplots_legend():
    legend_patches = []
    for impl in VECTOR_IMPLS:
        key = normalize_name(impl)
        color = line_styles[key]["color"] if key in line_styles else "gray"
        hatch = hatch_map.get(key, None)
        legend_patches.append(
            Patch(facecolor="none",
                  edgecolor=color, hatch=hatch,
                  linewidth=1.2, label=VECTOR_LABELS.get(key, key))
        )

    fig_legend = plt.figure(figsize=(5, 0.5))
    ax_legend = fig_legend.add_subplot(111)
    ax_legend.axis("off")
    ax_legend.legend(
        handles=legend_patches,
        loc="center",
        frameon=False,
        ncol=len(legend_patches),
        borderaxespad=0,
        labelspacing=0.2,
        borderpad=0,
        columnspacing=0.6,
        handletextpad=0.3,
    )
    legend_file = CURR_DIR / TAG / "inmemory-vector-boxplots-legend.pdf"
    fig_legend.savefig(legend_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"Saved: {legend_file}")


def plot_vector_boxplots_writes():
    fig = plt.figure(figsize=(3, 2.8))
    gs  = fig.add_gridspec(2, 1, height_ratios=[9, 1], hspace=0.05)
    ax_top = fig.add_subplot(gs[0])
    ax_bot = fig.add_subplot(gs[1])
    _plot_vector_boxplot_panel(
        ax_top, ax_bot, "I:", ".I.npy", "insert latency (ns)", "writes",
        ylim_top=(1e2, None), ylim_bot=(1e0, 30), fig_label="(A)",
    )
    output_file = DROPBOX_PATH / "inmemory-vector-boxplots-writes.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_vector_boxplots_gets():
    fig = plt.figure(figsize=(3, 2.8))
    gs  = fig.add_gridspec(2, 1, height_ratios=[9, 1], hspace=0.05)
    ax_top = fig.add_subplot(gs[0])
    ax_bot = fig.add_subplot(gs[1])
    _plot_vector_boxplot_panel(
        ax_top, ax_bot, "Q:", ".P.npy", "PQ latency (ns)", "gets",
        ylim_top=(1e3, None), ylim_bot=(1e0, 30), fig_label="(B)",
    )
    output_file = DROPBOX_PATH / "inmemory-vector-boxplots-gets.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def plot_legend(results):
    # Single shared legend for all plots in this script.
    impls_present = [
        impl
        for impl in implementations
        if impl in results.get("sequential", {}) or impl in results.get("mixed", {})
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
                facecolor="none" if impl != "vector-preallocated" else color,
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

    legend_file = CURR_DIR / TAG / "inmemory-legend.pdf"
    fig_legend.savefig(legend_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"Saved: {legend_file}")


def dump_csv(results):
    import csv

    fields = ["impl", "workload", "insert_ops", "pq_ops", "rq_ops"]
    output_file = CURR_DIR / TAG / "inmemory-mixed-ops.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for wl in ("sequential", "mixed"):
            for impl in implementations:
                tps = results[wl].get(impl)
                if tps is None:
                    continue
                writer.writerow({
                    "impl":       impl,
                    "workload":   wl,
                    "insert_ops": f"{tps[0]:.2f}",
                    "pq_ops":     f"{tps[1]:.2f}",
                    "rq_ops":     f"{tps[2]:.2f}",
                })
    print(f"Saved: {output_file}")


def dump_pq_latency_csv():
    import csv

    series = [
        ("sequential/vector-preallocated", "sequential"),
        ("mixed/vector-preallocated",      "mixed"),
        ("sequential/vector-optimized",    "vector-optimized"),
    ]
    fields = ["workload", "n_queries", "mean_ns", "median_ns", "p95_ns", "p99_ns", "max_ns"]
    output_file = CURR_DIR / TAG / "inmemory-pq-latency-mean.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for subdir, label in series:
            stats_file = EXP_DIR / subdir / "stats.log"
            if not stats_file.exists():
                print(f"Missing: {stats_file}")
                continue
            latencies = []
            with open(stats_file) as f2:
                for line in f2:
                    if line.startswith("Q:"):
                        latencies.append(int(line[2:].strip()))
            arr = np.array(latencies)
            writer.writerow({
                "workload":  label,
                "n_queries": len(arr),
                "mean_ns":   f"{arr.mean():.2f}",
                "median_ns": f"{np.percentile(arr, 50):.2f}",
                "p95_ns":    f"{np.percentile(arr, 95):.2f}",
                "p99_ns":    f"{np.percentile(arr, 99):.2f}",
                "max_ns":    f"{arr.max():.2f}",
            })
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    results = collect_data()
    print(f"\nSequential: {len(results['sequential'])} implementations loaded")
    print(f"Mixed:      {len(results['mixed'])} implementations loaded")
    plot_insert_throughput(results)
    # plot_pq_throughput(results)
    # plot_rq_throughput(results)
    # dump_csv(results)
    # plot_workload_time(results)
    # plot_pq_latency_over_time()
    # dump_pq_latency_csv()
    plot_legend(results)
    # plot_vector_boxplots_writes()
    # plot_vector_boxplots_gets()
    # plot_vector_boxplots_legend()
