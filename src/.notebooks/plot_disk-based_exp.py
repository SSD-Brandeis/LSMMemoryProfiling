import os
from pathlib import Path
import numpy as np
import matplotlib.pyplot as plt
from concurrent.futures import ProcessPoolExecutor

from plot.rocksdb_stats import parse_rocksdb_log
from plot.style import hatch_map, line_styles  # , hatch_map

TAG = "diskbased-full-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
ROOT_DIR = PROJECT_ROOT / ".vstats"
EXP_DIR = ROOT_DIR / "diskbased-1mb-buffer-t6"

implementations = [
    "vector-preallocated",
    "unsortedvector-preallocated",
    "sortedvector-preallocated",
    "skiplist",
    "simpleskiplist",
    # "linkedlist",
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

    _, ax = plt.subplots(figsize=(3.5, 2.5))

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

    ax.set_xticks([1, 2, 3, 4, 5, 6, 7, 8], ["vec", "uvec", "svec", "skip", "iskip", "hlink", "hskip", "hvec"], rotation=90)
    ax.set_yticks([0, 10, 20], ["0", "10", "20"])
    ax.set_ylabel("PQ latency (ms)", labelpad=-1)  # $\\mu$      , labelpad=-1
    # ax.set_xlabel("buffer", labelpad=-1)
    ax.set_ylim(0, None)

    output_file = CURR_DIR / TAG / "disk-based-insert-latency-pq.pdf"
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

    _, ax = plt.subplots(figsize=(3.5, 2.5))

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

    ax.set_xticks([1, 2, 3, 4, 5, 6, 7, 8], ["vec", "uvec", "svec", "skip", "iskip", "hlink", "hskip", "hvec"], rotation=90)
    ax.set_yticks([0, 10, 20], ["0", "10", "20"])
    ax.set_ylabel("RQ latency (ms)", labelpad=-1)  # $\\mu$      , labelpad=-1
    # ax.set_xlabel("buffer", labelpad=-1)
    ax.set_ylim(0, None)

    output_file = CURR_DIR / TAG / "disk-based-insert-latency-rq.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    print(f"Saved plot to {output_file}")

    # --- Build legend (Separate File) ---
    from matplotlib.patches import Patch

    legend_elements = []
    for key, style in line_styles.items():
        if key == "linkedlist": continue
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
        borderaxespad=0,
        labelspacing=0.2,
        borderpad=0,
        columnspacing=0.5,
        handletextpad=0.2,
    )

    legend_file = CURR_DIR / TAG / "disk-based-latency-legend-violin.pdf"
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

    _, ax = plt.subplots(figsize=(3.5, 2.5))

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
    ax.set_xticks([1, 2, 3, 4, 5, 6, 7, 8], ["vec", "uvec", "svec", "skip", "iskip", "hlink", "hskip", "hvec"], rotation=90)
    ax.set_yticks([0, 5, 10, 15])
    ax.set_yticklabels(["0", "5", "10", "15"])

    ax.set_ylabel("insert latency ($\\mu$s)", labelpad=-1)
    # ax.set_xlabel("buffer", labelpad=-1)
    ax.set_ylim(0, None)

    # --- Save Plot ---
    output_file = CURR_DIR / TAG / "disk-based-insert-latency-violin.pdf"
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

    phasewise_wkl_ops = [80_000_000, 10_010_000, 10_010_000]

    def safe_tp(ops, time_us):
        if time_us == 0:
            return 0
        return ops / (time_us / 1e9)  # convert ns to seconds

    return (
        impl,
        [
            safe_tp(phasewise_wkl_ops[i], phasewise_wkl_time[i])
            for i in range(len(phases))
        ],
        phasewise_flush_counts,
        phasewise_compaction_counts,
    )


def process_data_movement(impl):
    path = os.path.join(EXP_DIR, impl)
    log_file = os.path.join(path, "workload.log")

    phases = parse_rocksdb_log(log_file)
    phase_wise_overall_data_movement_bytes = [
        p["tickers"].get("rocksdb.bytes.read", 0)
        + p["tickers"].get("rocksdb.bytes.written", 0)
        + p["tickers"].get("rocksdb.compact.write.bytes", 0)
        + p["tickers"].get("rocksdb.compact.read.bytes", 0)
        for p in phases
    ]

    return (impl, phase_wise_overall_data_movement_bytes)


def process_write_stalls(impl):
    path = os.path.join(EXP_DIR, impl)
    log_file = os.path.join(path, "workload.log")

    phases = parse_rocksdb_log(log_file)
    phase_wise_write_stalls = [
        p["tickers"].get("rocksdb.stall.micros", 0) for p in phases
    ]

    return impl, phase_wise_write_stalls


def process_block_read_count_for_PQs(impl):
    path = os.path.join(EXP_DIR, impl)
    log_file = os.path.join(path, "workload.log")

    phases = parse_rocksdb_log(log_file)
    phase_wise_block_reads = [p["perf"].get("block_read_count", 0) for p in phases]

    return impl, phase_wise_block_reads


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

    _, ax = plt.subplots(figsize=(3.3, 3))

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

    output_file = CURR_DIR / TAG / "disk-based-flush-and-compaction-counts.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.02)

    print(f"Saved plot to {output_file}")

    # --- Build legend ---
    from matplotlib.patches import Patch

    legend_elements = []
    for key, style in line_styles.items():
        if key == "linkedlist":
            continue  # skip these two for legend since they are not in the throughput plot
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

    legend_file = CURR_DIR / TAG / "disk-based-latency-legend.pdf"
    fig_legend.savefig(legend_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"Saved legend to {legend_file}")


def plot_throughput():
    tasks = [
        impl for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))
    ]

    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, stats, _, _ in executor.map(process_impl, tasks):
            results[impl] = stats  # list of phase throughputs

    labels = list(results.keys())
    num_impls = len(labels)
    num_phases = len(next(iter(results.values())))  # e.g., 3

    _, ax = plt.subplots(figsize=(4.5, 3))

    x = np.arange(num_phases)  # <-- phases on x-axis
    bar_width = 0.9 / num_impls

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
    ax.set_xticklabels([f"P{i+1}" for i in range(num_phases)])

    ax.set_yticks([0, 50_000, 100_000], ["0", "50", "100"])

    ax.set_ylabel("throughput (kOPS)", labelpad=-1, loc="top")
    # ax.set_xlabel("phase", labelpad=-1)

    output_file = CURR_DIR / TAG / "disk-based-throughput.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.02)

    print(f"Saved plot to {output_file}")


def plot_overall_datamovement():
    GB_1 = 1024**3
    tasks = [
        impl for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))
    ]
    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, stats in executor.map(process_data_movement, tasks):
            results[impl] = stats

    labels = list(results.keys())
    num_impls = len(labels)
    num_phases = len(next(iter(results.values())))

    _, ax = plt.subplots(figsize=(4.5, 3))
    x = np.arange(num_phases)
    bar_width = 0.9 / num_impls

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

    ax.set_xticks(x + bar_width * (num_impls - 1) / 2)
    ax.set_xticklabels([f"P{i+1}" for i in range(num_phases)])

    ax.set_yticks([0, 100 * GB_1, 200 * GB_1], ["0", "100", "200"])

    ax.set_ylabel("data movement (GB)", y=0.45)

    output_file = CURR_DIR / TAG / "disk-based-overall-datamovement.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.06)
    print(f"Saved plot to {output_file}")


def plot_write_stall():
    convert_to_sec = lambda micros: micros / 1_000_000
    tasks = [
        impl for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))
    ]
    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, stats in executor.map(process_write_stalls, tasks):
            if stats:
                results[impl] = stats

    labels = list(results.keys())
    num_impls = len(labels)
    num_phases = len(next(iter(results.values())))

    _, ax = plt.subplots(figsize=(4.5, 3))
    x = np.arange(num_phases)
    bar_width = 0.9 / num_impls

    for j, impl in enumerate(labels):
        key = normalize_name(impl)
        style = line_styles[key]
        color = style["color"]
        heights = [convert_to_sec(stall) for stall in results[impl]]
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
    ax.set_xticklabels([f"P{i+1}" for i in range(num_phases)])

    ax.set_yticks([0, 1000, 2000], ["0", "1", "2"])

    ax.set_ylabel("write stall (s)")

    output_file = CURR_DIR / TAG / "disk-based-write-stall.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.06)
    print(f"Saved plot to {output_file}")


def plot_block_read_count():
    tasks = [
        impl for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))
    ]
    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, stats in executor.map(process_block_read_count_for_PQs, tasks):
            if stats:
                results[impl] = stats

    labels = list(results.keys())
    num_impls = len(labels)
    phases_to_show = [1, 2]  # P2 and P3 (0-indexed), skip P1 (no block reads)

    _, ax = plt.subplots(figsize=(3.3, 3))
    x = np.arange(len(phases_to_show))
    bar_width = 0.9 / num_impls

    for j, impl in enumerate(labels):
        key = normalize_name(impl)
        style = line_styles[key]
        color = style["color"]
        heights = [results[impl][i] for i in phases_to_show]
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
    ax.set_xticklabels([f"P{i+1}" for i in phases_to_show])

    ax.set_yticks([0, 10000, 20000], ["0", "10", "20"])

    ax.set_ylabel("read query I/Os")

    output_file = CURR_DIR / TAG / "disk-based-block-read-count.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.06)
    print(f"Saved plot to {output_file}")


def process_level_hits_for_PQs(impl):
    path = os.path.join(EXP_DIR, impl)
    log_file = os.path.join(path, "workload.log")

    phases = parse_rocksdb_log(log_file)
    p = phases[1]  # PQ phase only

    return impl, {
        "l0": p["tickers"].get("rocksdb.l0.hit", 0),
        "l1": p["tickers"].get("rocksdb.l1.hit", 0),
        "l2andup": p["tickers"].get("rocksdb.l2andup.hit", 0),
    }


def plot_level_hits():
    tasks = [
        impl for impl in implementations if os.path.isdir(os.path.join(EXP_DIR, impl))
    ]
    results = {}
    with ProcessPoolExecutor(max_workers=3) as executor:
        for impl, hits in executor.map(process_level_hits_for_PQs, tasks):
            results[impl] = hits

    labels = list(results.keys())
    num_impls = len(labels)
    levels = ["l0", "l1", "l2andup"]

    _, ax = plt.subplots(figsize=(4, 3))
    x = np.arange(len(levels))
    bar_width = 0.9 / num_impls

    for j, impl in enumerate(labels):
        key = normalize_name(impl)
        style = line_styles[key]
        color = style["color"]
        heights = [results[impl][lvl] for lvl in levels]
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
    ax.set_xticklabels(["L0", "L1", "L2+"])

    ax.set_ylabel("PQ hit")
    ax.set_yscale("log")
    ax.set_ylim(1e0)

    output_file = CURR_DIR / TAG / "disk-based-level-hits-pq.pdf"
    plt.savefig(output_file, bbox_inches="tight", pad_inches=0.06)
    print(f"Saved plot to {output_file}")


if __name__ == "__main__":
    print(
        ">>>>  plot_insert_latencies_* functions are slow due to log parsing. Run them individually if needed. <<<<"
    )
    # plot_insert_latencies_violin()
    # plot_insert_latencies()
    # plot_PQ_latencies()
    plot_RQ_latencies()
    # plot_throughput()
    # plot_flush_and_compaction_counts()
    # plot_overall_datamovement()
    # plot_write_stall()
    # plot_block_read_count()
    # plot_level_hits()
