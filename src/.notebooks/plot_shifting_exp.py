import os
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
from concurrent.futures import ProcessPoolExecutor
from matplotlib.ticker import MaxNLocator

from plot.rocksdb_stats import parse_rocksdb_log
from plot.style import hatch_map, line_styles  # , hatch_map

# --- CONFIGURATION PARAMETERS ---
Y_AXIS_LOG = False  # Set to True for log scale, False for linear scale
Y_AXIS_MAX = 150000
# Y_AXIS_MAX = 1e6
# -------------------------------

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
# Updated path for the shifting workload data
ROOT_DIR = CURR_DIR / "data_new" / "shifting_workload_small"
EXP_DIR = ROOT_DIR / "multiphase-diskbased-1mb-buffer-t6"

implementations = [
    "vector-preallocated",
    "unsortedvector-preallocated",
    # "sortedvector-preallocated",
    "skiplist",
    # "simpleskiplist",
    # "linkedlist",
    # "hashlinkedlist-H100000-X6",
    # "hashskiplist-H100000-X6",
    # "hashvector-H100000-X6",
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


def process_single_dir_for_PQs(args):
    impl, path = args
    log_file = os.path.join(path, "stats.log")
    cache_file = os.path.join(path, "insert_latencies_pqq.npy")

    if os.path.exists(cache_file):
        arr = np.load(cache_file, mmap_mode="r")
        print(f"[CACHE HIT] {impl}")
    else:
        print(f"[PARSING] {impl}")
        latencies = []
        with open(log_file, "r") as f:
            for _, line in enumerate(f):
                if not line.startswith("Q:"):
                    continue
                parts = line.split()
                latency_ms = float(parts[1]) / 1_000_000  # convert ns to ms
                latencies.append(latency_ms)
        arr = np.array(latencies, dtype=np.float64)
        np.save(cache_file, arr)

    stats = {
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "data": arr,
    }
    return impl, stats


def process_single_dir_for_RQs(args):
    impl, path = args
    log_file = os.path.join(path, "stats.log")
    cache_file = os.path.join(path, "insert_latencies_rq.npy")

    if os.path.exists(cache_file):
        arr = np.load(cache_file, mmap_mode="r")
        print(f"[CACHE HIT] {impl}")
    else:
        print(f"[PARSING] {impl}")
        latencies = []
        with open(log_file, "r") as f:
            for _, line in enumerate(f):
                if not line.startswith("S:"):
                    continue
                parts = line.split()
                latency_ms = float(parts[1]) / 1_000_000  # convert ns to ms
                latencies.append(latency_ms)
        arr = np.array(latencies, dtype=np.float64)
        np.save(cache_file, arr)

    stats = {
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "data": arr,
    }
    return impl, stats


def process_single_dir_for_Is(args):
    impl, path = args
    log_file = os.path.join(path, "stats.log")
    cache_file = os.path.join(path, "insert_latencies_all.npy")

    if os.path.exists(cache_file):
        arr = np.load(cache_file, mmap_mode="r")
        print(f"[CACHE HIT] {impl}")
    else:
        print(f"[PARSING] {impl}")
        latencies = []
        with open(log_file, "r") as f:
            for _, line in enumerate(f):
                if not line.startswith("I:"):
                    continue
                parts = line.split()
                latency_ms = float(parts[1]) / 1_000  # convert ns to μs
                latencies.append(latency_ms)
        arr = np.array(latencies, dtype=np.float64)
        np.save(cache_file, arr)

    stats = {
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "data": arr,
    }
    return impl, stats


# def plot_insert_latencies():
#     tasks = [(impl, os.path.join(EXP_DIR, impl)) for impl in implementations
#              if os.path.isdir(os.path.join(EXP_DIR, impl))]

#     print(f"Processing {len(tasks)} implementations...")

#     results = {}
#     with ProcessPoolExecutor(max_workers=3) as executor:
#         for impl, stats in executor.map(process_single_dir_for_Is, tasks):
#             if stats is not None:
#                 results[impl] = stats

#     labels = list(results.keys())
#     data = [results[impl]["data"] for impl in labels]
#     _, ax = plt.subplots(figsize=(4, 3))

#     # --- Create boxplot ---
#     bp = ax.boxplot(
#         data,
#         patch_artist=True,
#         showfliers=False,
#         whis=[0, 98],
#     )

#     # --- Apply colors and hatches ---
#     for i, (patch, impl) in enumerate(zip(bp["boxes"], labels)):
#         key = normalize_name(impl)
#         style = line_styles[key]
#         color = style["color"]
#         hatch = hatch_map.get(key, None)

#         # Box
#         patch.set_facecolor("none")
#         patch.set_edgecolor(color)
#         patch.set_hatch(hatch)
#         patch.set_linewidth(1.5)

#         # Whiskers
#         for whisker in bp["whiskers"][2*i:2*i+2]:
#             whisker.set_color(color)
#             whisker.set_linewidth(1.5)

#         # Caps
#         for cap in bp["caps"][2*i:2*i+2]:
#             cap.set_color(color)
#             cap.set_linewidth(1.5)

#         # Median
#         median = bp["medians"][i]
#         median.set_color(color)
#         median.set_linewidth(2)

#     ax.set_xticks([1, 2, 3, 4, 5, 6, 7, 8], ["", "", "", "", "", "", "", ""])
#     ax.set_yticks([0, 5, 10, 15], ["0", "5", "10", "15"])
#     ax.set_ylabel("insert latency ($\\mu$s)", labelpad=-1)
#     ax.set_xlabel("buffer", labelpad=-1)
#     ax.set_ylim(0, None)

#     output_file = CURR_DIR / "disk-based-insert-latency-inserts.pdf"
#     plt.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
#     print(f"Saved plot to {output_file}")

#     # --- Build legend ---
#     from matplotlib.patches import Patch
#     legend_elements = []
#     for key, style in line_styles.items():
#         color = style["color"]
#         hatch = hatch_map.get(key, None)
#         label = style["label"]
#         legend_elements.append(
#             Patch(facecolor="none", edgecolor=color, hatch=hatch, label=label, linewidth=1.5)
#         )

#     fig_legend = plt.figure(figsize=(6, 1.5))
#     ax_legend = fig_legend.add_subplot(111)
#     ax_legend.axis("off")

#     ax_legend.legend(
#         handles=legend_elements,
#         loc="center",
#         frameon=False,
#         ncol=4,
#         labelspacing=0.2,
#         borderaxespad=0,
#         borderpad=0,
#     )

#     legend_file = CURR_DIR / "disk-based-insert-latency-legend.pdf"
#     fig_legend.savefig(legend_file, bbox_inches="tight", pad_inches=0.02)
#     plt.close(fig_legend)
#     print(f"Saved legend to {legend_file}")


def plot_PQ_latencies():
    tasks = [
        (impl, os.path.join(EXP_DIR, impl))
        for impl in implementations
        if os.path.isdir(os.path.join(EXP_DIR, impl))
    ]

    print(f"Processing {len(tasks)} implementations...")

    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, stats in executor.map(process_single_dir_for_PQs, tasks):
            if stats is not None:
                results[impl] = stats

    labels = list(results.keys())
    data = [results[impl]["data"] for impl in labels]

    clipped_data = []
    for d in data:
        p0, p98 = np.percentile(d, [0, 99])
        clipped_data.append(d[(d >= p0) & (d <= p98)])

    _, ax = plt.subplots(figsize=(4, 3))

    # --- Create violin plot ---
    vp = ax.violinplot(
        clipped_data,
        showmeans=True,
        showmedians=False,
        showextrema=False,
        widths=0.5,
    )

    colors = []
    for i, impl in enumerate(labels):
        key = normalize_name(impl)
        color = line_styles[key]["color"]
        colors.append(color)

        body = vp["bodies"][i]
        body.set_facecolor(color)
        body.set_edgecolor(color)
        body.set_alpha(0.5)

    if "cmeans" in vp:
        vp["cmeans"].set_colors(colors)
        vp["cmeans"].set_linewidth(1.5)

    ax.set_xticks([1, 2, 3, 4, 5, 6, 7, 8], ["", "", "", "", "", "", "", ""])
    ax.set_yticks([0, 10, 20], ["0", "10", "20"])
    ax.set_ylabel("PQ latency (ms)", labelpad=-1)  # $\\mu$      , labelpad=-1
    ax.set_xlabel("buffer", labelpad=-1)
    ax.set_ylim(0, None)

    output_file = CURR_DIR / "disk-based-insert-latency-pq.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    print(f"Saved plot to {output_file}")


def plot_RQ_latencies():
    tasks = [
        (impl, os.path.join(EXP_DIR, impl))
        for impl in implementations
        if os.path.isdir(os.path.join(EXP_DIR, impl))
    ]

    print(f"Processing {len(tasks)} implementations...")

    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, stats in executor.map(process_single_dir_for_RQs, tasks):
            if stats is not None:
                results[impl] = stats

    labels = list(results.keys())
    data = [results[impl]["data"] for impl in labels]

    clipped_data = []
    for d in data:
        p0, p98 = np.percentile(d, [0, 99])
        clipped_data.append(d[(d >= p0) & (d <= p98)])

    _, ax = plt.subplots(figsize=(4, 3))

    # --- Create violin plot ---
    vp = ax.violinplot(
        clipped_data,
        showmeans=True,
        showmedians=False,
        showextrema=False,
        widths=0.5,
    )

    colors = []
    for i, impl in enumerate(labels):
        key = normalize_name(impl)
        color = line_styles[key]["color"]
        colors.append(color)

        body = vp["bodies"][i]
        body.set_facecolor(color)
        body.set_edgecolor(color)
        body.set_alpha(0.5)

    if "cmeans" in vp:
        vp["cmeans"].set_colors(colors)
        vp["cmeans"].set_linewidth(1.5)

    ax.set_xticks([1, 2, 3, 4, 5, 6, 7, 8], ["", "", "", "", "", "", "", ""])
    ax.set_yticks([0, 10, 20], ["0", "10", "20"])
    ax.set_ylabel("RQ latency (ms)", labelpad=-1)  # $\\mu$      , labelpad=-1
    ax.set_xlabel("buffer", labelpad=-1)
    ax.set_ylim(0, None)

    output_file = CURR_DIR / "disk-based-insert-latency-rq.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    print(f"Saved plot to {output_file}")

    # --- Build legend (Separate File) ---
    from matplotlib.patches import Patch

    legend_elements = []
    for key, style in line_styles.items():
        color = style["color"]
        label = style["label"]
        legend_elements.append(
            Patch(
                facecolor=color, edgecolor=color, alpha=0.5, label=label, linewidth=1.5
            )
        )

    fig_legend = plt.figure(figsize=(6, 1.5))
    ax_legend = fig_legend.add_subplot(111)
    ax_legend.axis("off")

    ax_legend.legend(
        handles=legend_elements,
        loc="center",
        frameon=False,
        ncol=4,
        labelspacing=0.2,
        borderaxespad=0,
        borderpad=0,
    )

    legend_file = CURR_DIR / "disk-based-latency-legend-violin.pdf"
    fig_legend.savefig(legend_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"Saved legend to {legend_file}")


def plot_insert_latencies_violin():
    tasks = [
        (impl, os.path.join(EXP_DIR, impl))
        for impl in implementations
        if os.path.isdir(os.path.join(EXP_DIR, impl))
    ]

    print(f"Processing {len(tasks)} implementations...")

    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, stats in executor.map(process_single_dir_for_Is, tasks):
            if stats is not None:
                results[impl] = stats

    labels = list(results.keys())
    data = [results[impl]["data"] for impl in labels]

    clipped_data = []
    for d in data:
        p98 = np.percentile(d, 98)
        clipped_data.append(d[d <= p98])

    _, ax = plt.subplots(figsize=(4, 3))

    # --- Create Violin Plot ---
    vp = ax.violinplot(
        clipped_data,
        showmeans=True,
        showmedians=False,
        showextrema=False,
        widths=0.5,
    )

    colors = []
    for i, impl in enumerate(labels):
        key = normalize_name(impl)
        color = line_styles[key]["color"]
        colors.append(color)

        body = vp["bodies"][i]
        body.set_facecolor(color)
        body.set_edgecolor(color)
        body.set_alpha(0.5)

    if "cmeans" in vp:
        vp["cmeans"].set_colors(colors)
        vp["cmeans"].set_linewidth(1.5)

    # --- Formatting ---
    ax.set_xticks(range(1, len(labels) + 1))
    ax.set_xticklabels([""] * len(labels))
    ax.set_yticks([0, 5, 10, 15])
    ax.set_yticklabels(["0", "5", "10", "15"])

    ax.set_ylabel("insert latency ($\\mu$s)", labelpad=-1)
    ax.set_xlabel("buffer", labelpad=-1)
    ax.set_ylim(0, None)

    # --- Save Plot ---
    output_file = CURR_DIR / "disk-based-insert-latency-violin.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    print(f"Saved violin plot to {output_file}")


def process_impl(impl):
    path = os.path.join(EXP_DIR, impl)
    log_file = os.path.join(path, "workload.log")

    phases = parse_rocksdb_log(log_file)
    phasewise_wkl_time = [p["meta"].get("workload_time", 0) for p in phases]
    phasewise_flush_counts = sum(
        [p["histograms"]["rocksdb.db.flush.micros"]["COUNT"] for p in phases]
    )
    phasewise_compaction_counts = sum(
        [p["histograms"]["rocksdb.compaction.times.micros"]["COUNT"] for p in phases]
    )

    # Updated operation counts based on ith_op reset triggers in runworkload.cc
    # P1: 8,000,000
    # P2: 1,000 (8,001,000 - 8,000,000)
    # P3: 10,000 (8,011,000 - 8,001,000)
    # P4: 1,000 (8,012,000 - 8,011,000)
    # P5: 1,000,000 (9,012,000 - 8,012,000)
    # P6: 100 (9,012,100 - 9,012,000)
    # P7: 100 (9,012,200 - 9,012,100)
    # P8: 1,000,000 (Assuming final phase matches P5 scale based on log)
    phasewise_wkl_ops = [8000000, 1000, 10000, 1000, 1000000, 99, 100, 1000000]

    def safe_tp(ops, time_ns):
        if time_ns == 0:
            return 0
        return ops / (time_ns / 1e9)  # convert ns to seconds

    return (
        impl,
        [
            safe_tp(phasewise_wkl_ops[i], phasewise_wkl_time[i])
            for i in range(len(phases))
        ],
        phasewise_flush_counts,
        phasewise_compaction_counts,
    )


def plot_flush_and_compaction_counts():
    tasks = [
        impl for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))
    ]

    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, _, flush_counts, compaction_counts in executor.map(
            process_impl, tasks
        ):
            results[impl] = (flush_counts, compaction_counts)

    labels = list(results.keys())
    num_impls = len(labels)
    num_phases = len(next(iter(results.values())))  # e.g., 3

    _, ax = plt.subplots(figsize=(4, 3))

    x = np.arange(num_phases)  # <-- phases on x-axis
    bar_width = 0.8 / num_impls

    for j, impl in enumerate(labels):
        key = normalize_name(impl)
        style = line_styles[key]
        color = style["color"]

        heights = results[impl]
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

    # --- center ticks under groups ---
    ax.set_xticks(x + bar_width * (num_impls - 1) / 2)
    ax.set_xticklabels(["flush", "compaction"])

    ax.set_yticks([0, 50_000, 100_000], ["0", "5", "10"])

    ax.set_ylabel("count", labelpad=-6)

    output_file = CURR_DIR / "disk-based-flush-and-compaction-counts.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.02)

    print(f"Saved plot to {output_file}")

    # --- Build legend ---
    from matplotlib.patches import Patch

    legend_elements = []
    for key, style in line_styles.items():
        color = style["color"]
        hatch = hatch_map.get(key, None)
        label = style["label"]
        legend_elements.append(
            Patch(
                facecolor="none" if label != "vector" else color,
                edgecolor=color,
                hatch=hatch,
                label=label,
                linewidth=1.5,
            )
        )

    fig_legend = plt.figure(figsize=(10, 1.5))
    ax_legend = fig_legend.add_subplot(111)
    ax_legend.axis("off")

    ax_legend.legend(
        handles=legend_elements,
        loc="center",
        ncol=8,
        frameon=False,
        borderaxespad=0,
        labelspacing=0,
        borderpad=0,
        columnspacing=0.5,
        handletextpad=0.2,
    )

    legend_file = CURR_DIR / "disk-based-latency-legend.pdf"
    fig_legend.savefig(legend_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"Saved legend to {legend_file}")


def plot_throughput_legend(results):
    from matplotlib.lines import Line2D

    legend_elements = []
    labels = list(results.keys())

    for impl in labels:
        key = normalize_name(impl)
        style = line_styles[key]
        legend_elements.append(Line2D([0], [0], **style))

    fig_legend = plt.figure(figsize=(3, 0.5))
    ax_legend = fig_legend.add_subplot(111)
    ax_legend.axis("off")

    ax_legend.legend(
        handles=legend_elements,
        loc="center",
        ncol=3,
        frameon=False,
        borderaxespad=0,
        labelspacing=0.2,
        borderpad=0,
    )

    legend_file = CURR_DIR / "disk-based-throughput-legend.pdf"
    fig_legend.savefig(legend_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"Saved throughput legend to {legend_file}")


def plot_throughput():
    tasks = [
        impl for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))
    ]

    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, stats, _, _ in executor.map(process_impl, tasks):
            results[impl] = stats  # list of phase throughputs

    labels = list(results.keys())
    num_phases = len(next(iter(results.values())))

    fig, ax = plt.subplots(figsize=(6, 3))

    x = np.arange(num_phases)

    for impl in labels:
        key = normalize_name(impl)
        style = line_styles[key]
        # color = style["color"]
        heights = results[impl]

        ax.plot(x, heights, **style)

    # --- Y-AXIS CONTROL ---
    if Y_AXIS_LOG:
        ax.set_yscale("log")
        ax.set_ylim(bottom=1)
    else:
        ax.set_ylim(bottom=0)

    # Manual Y-Max override
    if Y_AXIS_MAX is not None:
        ax.set_ylim(top=Y_AXIS_MAX)

    ax.relim()
    ax.autoscale_view()

    # Strictly fetch the current max to ensure it's shown
    y_max = int(ax.get_ylim()[1])

    if not Y_AXIS_LOG:
        # Use MaxNLocator to get clean, integer base ticks
        ax.yaxis.set_major_locator(MaxNLocator(integer=True, nbins=5))
        fig.canvas.draw()

        current_ticks = ax.get_yticks().tolist()
        # Prevent overlap: Prune ticks within 12% of the max value
        gap_threshold = y_max * 0.12

        final_ticks = [
            int(t) for t in current_ticks if t >= 0 and (y_max - t) > gap_threshold
        ]
        final_ticks.append(y_max)

        ax.set_yticks(sorted(list(set(final_ticks))))
        ax.set_yticklabels([f"{int(t)}" for t in ax.get_yticks()])

    # Set x-ticks
    ax.set_xticks(x)
    ax.set_xticklabels([f"P{i+1}" for i in range(num_phases)])

    ax.set_yticks([0, 50000, 100000, 150000], ["0", "50", "100", "150"])

    ax.set_ylabel("throughput (kOPS)", labelpad=-1)
    # ax.set_xlabel("phase", labelpad=-1)

    output_file = CURR_DIR / "disk-based-throughput.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close()

    # Generate separate legend PDF
    plot_throughput_legend(results)
    print(f"Saved line plot to {output_file}")


if __name__ == "__main__":
    print(
        ">>>>  plot_insert_latencies_* functions are slow due to log parsing. Run them individually if needed. <<<<"
    )
    # plot_insert_latencies_violin()
    # plot_insert_latencies()
    # plot_PQ_latencies()
    # plot_RQ_latencies()
    plot_throughput()
    # plot_flush_and_compaction_counts()
