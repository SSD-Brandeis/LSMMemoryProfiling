import re
from typing import List, Dict, Any
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

# Assuming these are defined in your 'plot' module
from plot import *
from plot.utils import process_LOG_file

# --- CONFIGURATION ---
CURR_DIR = Path(__file__).parent

DATA_DIR = EXP_DIR / "fig8_newbuffer_scatter"

# DATA_DIR = EXP_DIR / "filter_result_mar10__fig8_varybuffer_E32"

PREFIX_LEN = 6
BUCKET_COUNT = 100000
FIGSIZE = (4, 3.2)

# --- SWITCH PARAMETERS ---
USE_LOG_Y = False  # Parameter to toggle log scale
# ------------------------

# Comment/Uncomment buffers here to control the plot
BUFFERS_TO_PLOT = [
    "vector-dynamic",
    # "unsortedvector-dynamic",
    # "alwayssortedVector-dynamic",
    "skiplist",
    "linkedlist",
    "hash_skip_list",
    "hash_linked_list",
    "hash_vector",
]


def get_experiment_data() -> pd.DataFrame:
    records = []

    if not DATA_DIR.exists():
        print(f"Directory {DATA_DIR} does not exist.")
        return pd.DataFrame()

    for exp_dir in DATA_DIR.rglob("*sanitycheck-fig_8_metadata_varybuffer*-B*KB*"):
        if not exp_dir.is_dir():
            continue

        exp_match = re.search(r"-B(\d+)KB", exp_dir.name)
        if not exp_match:
            continue

        # X-axis remains in KB for granularity (binary conversion)
        buffer_size_kb = int(exp_match.group(1))
        buf_bytes = buffer_size_kb * 1024

        for buffer_path in exp_dir.glob("buffer-*"):
            if not buffer_path.is_dir():
                continue

            match = re.match(r"buffer-\d+-(.*?)(?:-H\d+)?-B\d+KB.*", buffer_path.name)
            if not match:
                continue

            name = match.group(1)

            log_file = buffer_path / "LOG_rocksdb"
            if not log_file.exists():
                log_file = buffer_path / "LOG1"

            if not log_file.exists():
                continue

            actual_bytes = process_LOG_file(str(log_file))
            if actual_bytes <= 0:
                continue

            # NEW: Calculate overhead in MB to reduce zeros on Y-axis
            # Using 1024 * 1024 for a strict binary conversion
            overhead_mb = (buf_bytes - actual_bytes) / (1024 * 1024)

            records.append(
                {
                    "buffer": name,
                    "buffer_size_kb": buffer_size_kb,
                    "metadata_over_mb": overhead_mb,
                }
            )

    return pd.DataFrame(records)


def main():
    df = get_experiment_data()
    if df.empty:
        print("No data found to plot.")
        return

    # Grouping by KB-based x-axis and MB-based y-axis
    df = df.groupby(["buffer", "buffer_size_kb"], as_index=False).mean()
    df = df[df["buffer"].isin(BUFFERS_TO_PLOT)]

    fig, ax = plt.subplots(figsize=FIGSIZE)

    for buffer_name in BUFFERS_TO_PLOT:
        subset = df[df["buffer"] == buffer_name].sort_values("buffer_size_kb")
        if subset.empty:
            continue

        style = line_styles.get(buffer_name, {}).copy()

        if "hash" in buffer_name and "X=" not in style.get("label", ""):
            style["label"] = (
                f"{style.get('label', buffer_name)} X={PREFIX_LEN} H={BUCKET_COUNT//1000}K"
            )

        # Plotting: X in KB, Y in MB
        ax.plot(subset["buffer_size_kb"], subset["metadata_over_mb"], **style)

    ax.set_xlabel("buffer size (KB)")
    # Updated label to MB
    ax.set_ylabel("metadata overhead (MB)", labelpad=0.1, loc="top")

    # Toggle logic for y-axis scale
    if USE_LOG_Y:
        ax.set_yscale("log")
        ax.set_ylim(1e-1, 1e3)
        ax.set_yticks([0.1, 1, 10, 100, 1000])
    else:
        ax.set_ylim(bottom=0)
        # Explicit y-tick control
        ax.set_yticks([0, 50, 100, 150, 200])

    # X-axis scale using log base 2 for binary memory increments
    ax.set_xscale("log", base=2)
    ax.set_xticks([2**6, 2**10, 2**14, 2**18])
    # ax.set_xticklabels(["$$0.06$$", "$$1$$", "$$16$$", "$$256$$"])
    ax.set_xticklabels(["$$2^{6}$$", "$$2^{10}$$", "$$2^{14}$$", "$$2^{18}$$"])

    plt.tight_layout()

    save_dir = CURR_DIR / "output_plots"
    save_dir.mkdir(parents=True, exist_ok=True)

    plot_path = save_dir / "metadata_overhead_buffer_size.pdf"
    plt.savefig(plot_path, bbox_inches="tight", pad_inches=0.02)
    print(f"[saved] {plot_path}")

    handles, labels = ax.get_legend_handles_labels()
    if handles:
        legend_fig = plt.figure(figsize=(10, 2))
        legend_fig.legend(handles, labels, loc="center", ncol=3, frameon=False)
        legend_path = save_dir / "metadata_overhead_vs_buffer_legend.pdf"
        legend_fig.savefig(legend_path, bbox_inches="tight", pad_inches=0.012)
        print(f"[saved] {legend_path}")
        plt.close(legend_fig)

    plt.close(fig)
    print("\n--- Plotting Complete ---")


if __name__ == "__main__":
    main()