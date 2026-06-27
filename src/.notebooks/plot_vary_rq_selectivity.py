import os
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt

from plot import *
from plot.style import line_styles

TAG = "vary-rq-selectivity-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / TAG

WINDOW = 30

SELECTIVITIES = ["0.1", "0.01", "0.001", "0.0001", "0.00001", "0.000001", "0.0000001"]

# User-defined per-selectivity axis config.
# yticks:      raw numeric tick values passed to ax.set_yticks()
# yticklabels: string labels matching yticks (same length)
# exp_text:    multiplier annotation shown top-left, e.g. r"$\times10^{6}$" (empty string = skip)
# fig_label:   corner label shown top-right, e.g. "(A)" (empty string = skip)
SEL_CONFIG = {
    "0.1":       {"yticks": [0, 1e7, 2e7, 3e7], "yticklabels": ["0", "1", "2", "3"], "exp_text": r"$\times10^{7}$", "fig_label": "(A)", "inset_text": f"rolling window: {WINDOW}pts\nsolid: mean\nrange: p0-p95"},
    "0.01":      {"yticks": [0, 2e6, 4e6],   "yticklabels": ["0", "2", "4"],                    "exp_text": r"$\times10^{6}$",                "fig_label": "(B)"},
    "0.001":     {"yticks": [0, 3e5, 6e5], "yticklabels": ["0", "3", "6"], "exp_text": r"$\times10^{5}$", "fig_label": "(C)"},
    "0.0001":    {"yticks": [0, 2e4, 4e4, 6e4], "yticklabels": ["0", "2", "4", "6"], "exp_text": r"$\times10^{4}$", "fig_label": "(C)"},
    "0.00001":   {"yticks": [0, 1e4, 2e4], "yticklabels": ["0", "1", "2"], "exp_text": r"$\times10^{4}$", "fig_label": "(D)"},
    "0.000001":  {"yticks": [0, 1e4, 2e4], "yticklabels": ["0", "1", "2"], "exp_text": r"$\times10^{4}$", "fig_label": "(E)"},
    "0.0000001": {"yticks": [0, 0.5e4, 1e4, 1.5e4], "yticklabels": ["0", "0.5", "1", "1.5"], "exp_text": r"$\times10^{4}$", "fig_label": "(F)"},
}

# Every other implementation from the full set
implementations = [
    # "vector-preallocated",
    # "unsortedvector-preallocated",
    "sortedvector-preallocated",
    "skiplist",
    "simpleskiplist",
    "hashlinkedlist-H100000-X6",
    "hashskiplist-H100000-X6",
    "hashvector-H100000-X6",
    # "hashlinkedlist-H100000-X2",
    # "hashskiplist-H100000-X2",
    # "hashvector-H100000-X2",
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


def read_rq_latencies(stats_file: Path) -> np.ndarray:
    cache = stats_file.with_suffix(".S.npy")
    if cache.exists() and cache.stat().st_mtime >= stats_file.stat().st_mtime:
        return np.load(cache)
    latencies = []
    with open(stats_file) as f:
        for line in f:
            if line.startswith("S:"):
                latencies.append(int(line[2:].strip()))
    arr = np.array(latencies)
    np.save(cache, arr)
    return arr



def rolling_stats(arr: np.ndarray, window: int):
    half = window // 2
    n = len(arr)
    x, p0, p95, mean = [], [], [], []
    for i in range(n):
        w = arr[max(0, i - half): min(n, i + half)]
        x.append(i + 1)
        p0.append(np.percentile(w, 0))
        p95.append(np.percentile(w, 95))
        mean.append(np.mean(w))
    return np.array(x), np.array(p0), np.array(p95), np.array(mean)



def plot_rq_latency_by_selectivity():
    for sel in SELECTIVITIES:
        sel_dir = EXP_DIR / f"sel-{sel}"

        fig, ax = plt.subplots(figsize=(4, 2.8))

        vector_keys = {"vector", "unsortedvector", "alwayssortedvector"}
        non_vec_ymax = 0

        for impl in implementations:
            stats_file = sel_dir / impl / "stats.log"
            if not stats_file.exists():
                print(f"Missing: {stats_file}")
                continue

            key = normalize_name(impl)
            if key is None or key not in line_styles:
                continue
            style = line_styles[key]

            arr = read_rq_latencies(stats_file)
            if len(arr) < WINDOW:
                continue

            x, p0, p95, mean = rolling_stats(arr, WINDOW)
            ax.fill_between(x, p0, p95, color=style["color"], alpha=0.15)
            ax.plot(x, mean, color=style["color"], linestyle=style["linestyle"],
                    linewidth=2, label=style["label"])

            if key not in vector_keys:
                non_vec_ymax = max(non_vec_ymax, np.max(p95))

        cfg = SEL_CONFIG.get(sel, {})

        ax.set_xlim(0, 1000)
        ax.set_ylim(0, non_vec_ymax * 1.1 if non_vec_ymax > 0 else None)
        ax.set_xticks([0, 500, 1000], ["0", "0.5", "1"])
        ax.set_xlabel("range query (k)")
        ax.set_ylabel("latency (ns)")

        if cfg.get("yticks"):
            ax.set_yticks(cfg["yticks"])
            ax.set_yticklabels(cfg["yticklabels"])
            ax.yaxis.offsetText.set_visible(False)

        ax.text(0.5, 0.97, f"s={sel}", transform=ax.transAxes, fontsize=20,
                va="top", ha="center")

        if cfg.get("exp_text"):
            ax.text(0.02, 0.97, cfg["exp_text"], transform=ax.transAxes,
                    fontsize=20, va="top", ha="left")
        if cfg.get("inset_text"):
            ax.text(0.02, 0.82, cfg["inset_text"], transform=ax.transAxes,
                    fontsize=18, va="top", ha="left", linespacing=1.4)
        if cfg.get("fig_label"):
            ax.text(0.98, 0.97, cfg["fig_label"], transform=ax.transAxes,
                    fontsize=20, va="top", ha="right")

        safe_sel = sel.replace(".", "_")
        output_file = DROPBOX_PATH / f"rq-latency-sel-{safe_sel}.pdf"
        fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
        plt.close(fig)
        print(f"Saved: {output_file}")


def plot_rq_legend():
    from matplotlib.lines import Line2D

    legend_elements = []
    for impl in implementations:
        key = normalize_name(impl)
        if key is None or key not in line_styles:
            continue
        style = line_styles[key]
        legend_elements.append(
            Line2D([0], [0], color=style["color"], linestyle=style["linestyle"],
                   linewidth=1.5, label=style["label"])
        )

    fig_legend = plt.figure(figsize=(8, 0.6))
    ax_legend = fig_legend.add_subplot(111)
    ax_legend.axis("off")
    ax_legend.legend(
        handles=legend_elements,
        loc="center",
        frameon=False,
        ncol=len(legend_elements),
        borderaxespad=0,
        labelspacing=0.2,
        borderpad=0,
        columnspacing=0.6,
        handletextpad=0.3,
    )

    legend_file = DROPBOX_PATH / "rq-legend.pdf"
    fig_legend.savefig(legend_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig_legend)
    print(f"Saved: {legend_file}")


def dump_csv():
    import csv

    fields = ["selectivity", "impl", "n_queries", "mean_ns", "median_ns", "p95_ns", "p99_ns", "max_ns"]
    output_file = CURR_DIR / TAG / "rq-latency-selectivity.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for sel in SELECTIVITIES:
            for impl in implementations:
                stats_file = EXP_DIR / f"sel-{sel}" / impl / "stats.log"
                if not stats_file.exists():
                    continue
                arr = read_rq_latencies(stats_file)
                if len(arr) == 0:
                    continue
                writer.writerow({
                    "selectivity": sel,
                    "impl":        impl,
                    "n_queries":   len(arr),
                    "mean_ns":     f"{arr.mean():.2f}",
                    "median_ns":   f"{np.percentile(arr, 50):.2f}",
                    "p95_ns":      f"{np.percentile(arr, 95):.2f}",
                    "p99_ns":      f"{np.percentile(arr, 99):.2f}",
                    "max_ns":      f"{arr.max():.2f}",
                })
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    # plot_rq_latency_by_selectivity()
    plot_rq_legend()
    # dump_csv()
