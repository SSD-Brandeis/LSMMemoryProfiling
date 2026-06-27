import re
from pathlib import Path

from plot import *

import numpy as np
import matplotlib.pyplot as plt

from plot.style import line_styles

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / "vary-prefix-length-mean-capacity"

# Must match the bash script
ENTRY_SIZE     = 128
BUFFER_SIZE_MB = 128
BUCKET_COUNT   = 100000
N_FLUSHES      = 10
PREFIX_LENGTHS = [1, 2, 3, 4, 5, 6]

CONFIGURED_CAPACITY_MB = BUFFER_SIZE_MB  # 128 MB

# Hash hybrids — vary with prefix length; dir: B{bucket}-X{prefix}/{impl}/workload.log
HASH_IMPLS = [
    ("hashskiplist",   "hashskiplist"),
    ("hashvector",     "hashvector"),
    ("hashlinkedlist", "hashlinkedlist"),
]

# Baselines — run once; dir: {impl}/workload.log
BASELINE_IMPLS = [
    ("vector",         "vector"),
    ("skiplist",       "skiplist"),
    # ("simpleskiplist", "simpleskiplist"),
]

_FLUSH_RE = re.compile(r"num_entries\]:\s*(\d+)")


def mean_capacity_mb(log_path: Path, n_flushes: int = N_FLUSHES) -> float | None:
    entries = []
    with open(log_path) as f:
        for line in f:
            m = _FLUSH_RE.search(line)
            if not m:
                continue
            entries.append(int(m.group(1)))
            if len(entries) >= n_flushes:
                break
    if not entries:
        return None
    return float(np.mean(entries)) * ENTRY_SIZE / (1024 * 1024)


def collect_data():
    # Hash hybrids: {style_key: [(prefix_length, capacity_mb), ...]}
    hash_data = {key: [] for key, _ in HASH_IMPLS}
    for pl in PREFIX_LENGTHS:
        ddir = EXP_DIR / f"B{BUCKET_COUNT}-X{pl}"
        for style_key, subdir in HASH_IMPLS:
            log = ddir / subdir / "workload.log"
            if not log.exists():
                print(f"Missing: {log}")
                continue
            cap = mean_capacity_mb(log)
            if cap is not None:
                hash_data[style_key].append((pl, cap))

    # Baselines: {style_key: capacity_mb or None}
    baseline_data = {}
    for style_key, subdir in BASELINE_IMPLS:
        log = EXP_DIR / subdir / "workload.log"
        if not log.exists():
            print(f"Missing: {log}")
            baseline_data[style_key] = None
        else:
            baseline_data[style_key] = mean_capacity_mb(log)

    return hash_data, baseline_data


def plot_mean_capacity():
    hash_data, baseline_data = collect_data()

    fig, ax = plt.subplots(figsize=(4.5, 2.8))

    # Baseline horizontal lines (constant across all prefix lengths)
    x_min, x_max = PREFIX_LENGTHS[0], PREFIX_LENGTHS[-1]
    for style_key, _ in BASELINE_IMPLS:
        cap = baseline_data.get(style_key)
        if cap is None:
            continue
        s = line_styles[style_key]
        ax.hlines(cap, x_min, x_max,
                  colors=s["color"], linestyles=s["linestyle"],
                  linewidth=s.get("linewidth", 2),
                  label=s["label"])

    # Hash hybrid lines (vary with prefix length)
    for style_key, _ in HASH_IMPLS:
        points = hash_data[style_key]
        if not points:
            continue
        s  = line_styles[style_key]
        local_label = s["label"]
        if "hash" in style_key:
            local_label = local_label + f" H={BUCKET_COUNT//1000}K"
        xs = [pl  for pl, _   in points]
        ys = [cap for _,  cap in points]
        ax.plot(xs, ys,
                color=s["color"], linestyle=s["linestyle"],
                marker=s.get("marker"), markersize=s.get("markersize", 8),
                markerfacecolor=s.get("markerfacecolor", "none"),
                linewidth=s.get("linewidth", 2),
                label=local_label)

    # Configured buffer capacity reference line
    ax.axhline(CONFIGURED_CAPACITY_MB, color="black", linestyle=":",
               linewidth=1.5)

    # ax.set_xlim(x_min, x_max)
    ax.set_xticks(PREFIX_LENGTHS)
    ax.set_xticklabels([str(pl) for pl in PREFIX_LENGTHS])
    ax.set_xlabel("prefix length", labelpad=-1)
    ax.set_ylabel("mean capacity (MB)", labelpad=-1, loc="top")
    ax.set_ylim(bottom=0, top=CONFIGURED_CAPACITY_MB * 1.05)

    ax.text(0.995, 0.02, "(A)", transform=ax.transAxes,
            va="bottom", ha="right", fontsize=20)

    ax.text(0.99, 0.83, "buffer", transform=ax.transAxes,
            va="bottom", ha="right", fontsize=19)

    ax.legend(loc="center", bbox_to_anchor=(0.45, 0.36), frameon=False,
                     ncol=1, borderaxespad=0,
                     labelspacing=0.005, borderpad=0,
                     columnspacing=0.2, handletextpad=0.2, handlelength=1.0, fontsize=19.5)

    output_file = DROPBOX_PATH / "vary-prefix-length-mean-capacity.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")

if __name__ == "__main__":
    plot_mean_capacity()
