import re
from pathlib import Path

from plot import *

import numpy as np
import matplotlib.pyplot as plt

from plot.style import line_styles

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / "vary-bucket-count-mean-capacity-exp"

# Must match the bash script
ENTRY_SIZE     = 128
BUFFER_SIZE_MB = 128
PREFIX_LENGTH  = 6
N_FLUSHES      = 10
BUCKET_COUNTS  = [1_000, 200_000, 400_000, 600_000, 800_000, 1_000_000]

CONFIGURED_CAPACITY_MB = BUFFER_SIZE_MB  # 128 MB

# Hash hybrids — vary with bucket count; dir: B{n}-X{prefix}/{impl}/workload.log
HASH_IMPLS = [
    ("hashskiplist",   "hashskiplist"),
    ("hashvector",     "hashvector"),
    ("hashlinkedlist", "hashlinkedlist"),
]

# Baselines — run once; dir: {impl}/workload.log
BASELINE_IMPLS = [
    ("vector",       "vector"),
    ("skiplist",     "skiplist"),
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
    # Hash hybrids: {style_key: [(bucket_count, capacity_mb), ...]}
    hash_data = {key: [] for key, _ in HASH_IMPLS}
    for bc in BUCKET_COUNTS:
        ddir = EXP_DIR / f"B{bc}-X{PREFIX_LENGTH}"
        for style_key, subdir in HASH_IMPLS:
            log = ddir / subdir / "workload.log"
            if not log.exists():
                print(f"Missing: {log}")
                continue
            cap = mean_capacity_mb(log)
            if cap is not None:
                hash_data[style_key].append((bc, cap))

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

    # Baseline horizontal lines (constant, independent of bucket count)
    x_min, x_max = BUCKET_COUNTS[0], BUCKET_COUNTS[-1]
    for style_key, _ in BASELINE_IMPLS:
        cap = baseline_data.get(style_key)
        if cap is None:
            continue
        s = line_styles[style_key]
        ax.hlines(cap, x_min, x_max,
                  colors=s["color"], linestyles=s["linestyle"],
                  linewidth=s.get("linewidth", 2),
                  label=s["label"])

    # Hash hybrid lines (vary with bucket count)
    for style_key, _ in HASH_IMPLS:
        points = hash_data[style_key]
        if not points:
            continue
        s  = line_styles[style_key]
        local_label = s["label"]
        if "hash" in style_key:
            local_label = local_label + f" X={PREFIX_LENGTH}"
        xs = [bc  for bc, _   in points]
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
    ax.set_xticks(BUCKET_COUNTS)
    ax.set_xticklabels(["1", "200", "400", "600", "800", "1000"])
    ax.set_xlabel("bucket count (k)", labelpad=-1)
    ax.set_ylabel("mean capacity (MB)", labelpad=-1, loc="top")
    ax.set_ylim(bottom=0, top=CONFIGURED_CAPACITY_MB * 1.05)

    # ax.text(0.995, 0.02, "(B)", transform=ax.transAxes,
    #         va="bottom", ha="right", fontsize=20)

    ax.text(0.99, 0.83, "buffer", transform=ax.transAxes,
            va="bottom", ha="right", fontsize=19)

    ax.legend(loc="center", bbox_to_anchor=(0.28, 0.28), frameon=False,
                     ncol=1, borderaxespad=0,
                     labelspacing=0.005, borderpad=0,
                     columnspacing=0.2, handletextpad=0.2, handlelength=1.0, fontsize=18)

    output_file = DROPBOX_PATH / "vary-bucket-count-mean-capacity.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")

def dump_csv():
    import csv

    hash_data, baseline_data = collect_data()

    # bucket_count → {impl: capacity_mb}
    rows: dict[int, dict[str, float]] = {bc: {} for bc in BUCKET_COUNTS}
    for style_key, _ in HASH_IMPLS:
        for bc, cap in hash_data[style_key]:
            rows[bc][style_key] = cap

    impl_cols = [k for k, _ in HASH_IMPLS] + [k for k, _ in BASELINE_IMPLS]
    fields = ["bucket_count"] + impl_cols + ["configured_capacity_mb"]

    output_file = CURR_DIR / "vary-bucket-count-mean-capacity.csv"
    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for bc in BUCKET_COUNTS:
            row = {"bucket_count": bc, "configured_capacity_mb": CONFIGURED_CAPACITY_MB}
            for k in [key for key, _ in HASH_IMPLS]:
                row[k] = f"{rows[bc].get(k, ''):.4f}" if rows[bc].get(k) is not None else ""
            for k, _ in BASELINE_IMPLS:
                v = baseline_data.get(k)
                row[k] = f"{v:.4f}" if v is not None else ""
            writer.writerow(row)

    print(f"Saved: {output_file}")


if __name__ == "__main__":
    plot_mean_capacity()
    dump_csv()
