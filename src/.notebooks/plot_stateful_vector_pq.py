import os
from pathlib import Path

from plot import *

import numpy as np
import matplotlib.pyplot as plt

TAG = "stateful-vector-pq-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / TAG

STYLES = {
    "vector": {
        "label": r"\texttt{V-Qsort}",
        "color": "#006d2c",
        "linestyle": "solid",
        "linewidth": 0.8,
        "alpha": 0.85,
    },
    "statefulvector": {
        "label": r"\texttt{V-Snap}",
        "color": "#1f78b4",
        "linestyle": "solid",
        "linewidth": 0.8,
        "alpha": 0.85,
    },
}


def load_pq_latencies(log_file: Path):
    """
    Parse stats.log and return:
      pq_ns        — int64 array of per-PQ latencies (ns), in arrival order
      spike_idx    — indices into pq_ns where a snapshot rebuild was triggered
                     (first PQ ever, and first PQ after each Update)
    """
    cache_pq     = log_file.parent / "cache_pq_ns.npy"
    cache_spikes = log_file.parent / "cache_spike_idx.npy"
    src_mtime    = log_file.stat().st_mtime

    if (cache_pq.exists() and cache_spikes.exists() and
            cache_pq.stat().st_mtime > src_mtime):
        pq     = np.load(cache_pq)
        spikes = np.load(cache_spikes)
        print(f"Loaded {len(pq)} PQs from cache ({log_file.parent.name})")
        return pq, spikes

    print(f"Parsing {log_file} ...")
    pq_ns        = []
    spike_indices = []
    next_is_spike = True   # very first PQ triggers snapshot creation

    with open(log_file) as f:
        for line in f:
            if line.startswith("Q:"):
                val = int(line[2:])
                if next_is_spike:
                    spike_indices.append(len(pq_ns))
                    next_is_spike = False
                pq_ns.append(val)
            elif line.startswith("U:"):
                next_is_spike = True   # invalidated; next PQ must rebuild

    pq_arr     = np.array(pq_ns,         dtype=np.int64)
    spike_arr  = np.array(spike_indices, dtype=np.int64)
    np.save(cache_pq,     pq_arr)
    np.save(cache_spikes, spike_arr)
    print(f"  {len(pq_arr)} PQs, {len(spike_arr)} snapshot rebuilds")
    return pq_arr, spike_arr


def plot_pq_latency_comparison():
    vec_log  = EXP_DIR / "vector-preallocated"          / "stats.log"
    snap_log = EXP_DIR / "statefulvector-preallocated"  / "stats.log"

    missing = [p for p in (vec_log, snap_log) if not p.exists()]
    if missing:
        print(f"Missing: {missing}")
        return

    vec_pq,  _           = load_pq_latencies(vec_log)
    snap_pq, snap_spikes = load_pq_latencies(snap_log)

    n = min(len(vec_pq), len(snap_pq))
    x = np.arange(1, n + 1)

    fig, ax = plt.subplots(figsize=(6, 3))

    sty = STYLES["vector"]
    ax.plot(x, vec_pq[:n],
            linewidth=sty["linewidth"], label=sty["label"],
            color=sty["color"], linestyle=sty["linestyle"], alpha=sty["alpha"])

    sty = STYLES["statefulvector"]
    ax.plot(x, snap_pq[:n],
            linewidth=sty["linewidth"], label=sty["label"],
            color=sty["color"], linestyle=sty["linestyle"], alpha=sty["alpha"])

    # Mark snapshot-rebuild points on the stateful-vector line
    valid_spikes = snap_spikes[snap_spikes < n]
    if len(valid_spikes):
        ax.scatter(valid_spikes + 1, snap_pq[valid_spikes],
                   color="red", s=12, zorder=5, label="snapshot rebuild")

    ax.set_yscale("log")

    step = n // 4
    ax.set_xticks([1, step, step * 2, step * 3, n],
                  ["0",
                   f"{step // 1000}k",
                   f"{step * 2 // 1000}k",
                   f"{step * 3 // 1000}k",
                   f"{n // 1000}k"])
    ax.set_xlabel("point query", labelpad=2)
    ax.set_ylabel("latency (ns)", labelpad=2)
    ax.legend(frameon=False, loc="upper right", fontsize=16,
              labelspacing=0.2, handlelength=1.2)

    output_file = CURR_DIR / TAG / "pq-latency-comparison.pdf"
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    plot_pq_latency_comparison()
