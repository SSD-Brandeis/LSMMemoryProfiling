from typing import List, Dict, Any, Optional
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Assuming these are defined in your local 'plot' module
from plot import *
from plot.utils import process_LOG_file

# --- CONFIGURATION ---
TAG = "vary1to4prefix_tectonic_single_run"
STATS_DIR = Path(
    "/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_mar16_vary1to4prefix_fig18A/vary1to4prefix_tectonic_single_run-128M-P4096-E128"
)

# --- HARD-CODE Y-AXIS LIMITS HERE ---
Y_BOTTOM = 0
Y_TOP = 136

BASE_PLOT_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot")
SCRIPT_NAME = Path(__file__).stem
SAVE_DIR = BASE_PLOT_DIR / SCRIPT_NAME
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# Constants
BUFFER_SIZE_IN_MB = 128
PREFIX_LENGTHS = [1, 2, 3, 4]
BUCKET_COUNTS = [100_000]

BUFFERS_TO_PLOT = [
    "vector-dynamic",
    "skiplist",
    "hash_skip_list",
    "hash_linked_list",
    "hash_vector",
]

FIGSIZE = (5, 3.6)


def get_data() -> List[Dict[str, Any]]:
    data = list()
    for buffer in BUFFERS_TO_PLOT:
        for prefix_len in PREFIX_LENGTHS:
            for bucket_count in BUCKET_COUNTS:
                if buffer in ["skiplist", "vector-dynamic"]:
                    log_dir = STATS_DIR / buffer
                else:
                    log_dir = STATS_DIR / f"{buffer}-X{prefix_len}-H{bucket_count}"

                if not log_dir.exists():
                    continue

                total_data_size = 0
                total_run = 0
                for run in range(1, 4):
                    log_path = log_dir / f"LOG{run}"
                    if log_path.exists():
                        total_data_size += process_LOG_file(log_path)
                        total_run += 1

                if total_run == 0:
                    continue

                mean_capacity = total_data_size / total_run
                data.append(
                    {
                        "buffer": buffer,
                        "prefix_length": prefix_len,
                        "bucket_count": bucket_count,
                        "mean_capacity": mean_capacity / (1024 * 1024),
                    }
                )
    return data


def main():
    data = get_data()

    if not data:
        print("No data found to plot.")
        return

    df = pd.DataFrame(data)
    fig, ax = plt.subplots(figsize=FIGSIZE)

    file_name = f"capacity_vs_prefix_{TAG}.pdf"
    x_col = "prefix_length"

    for buffer in BUFFERS_TO_PLOT:
        buffer_data = df[df["buffer"] == buffer]
        if buffer_data.empty:
            continue

        style = line_styles.get(buffer, {}).copy()

        if "hash" in buffer:
            style["label"] = (
                style.get("label", buffer)
                + f" H={buffer_data['bucket_count'].iloc[0]//1000}K"
            )
        else:
            style["label"] = style.get("label", buffer)
            style["marker"] = None

        ax.plot(buffer_data[x_col], buffer_data["mean_capacity"], **style)

    # --- ADD HORIZONTAL LINE FOR BUFFER LIMIT ---
    # linestyle='--' creates the dashed effect, zorder=1 keeps it behind data lines
    ax.axhline(
        y=BUFFER_SIZE_IN_MB, color="black", linestyle="--", linewidth=1, zorder=1
    )

    ax.set_xlabel("prefix length", labelpad=-1)
    ax.set_ylabel("mean capacity (MB)", labelpad=-1)
    ax.set_xticks(PREFIX_LENGTHS)

    # --- Y-AXIS CONTROL ---
    ax.set_ylim(Y_BOTTOM, Y_TOP)
    # ax.set_yticks([0, 50, 100, Y_TOP])

    # --- LEGEND REPOSITIONING ---
    fig.legend(
        loc="upper center",
        ncol=1,
        bbox_to_anchor=(0.58, 0.70),
        frameon=False,
        labelspacing=0.04,
        handletextpad=0.5,
    )

    plt.tight_layout()

    full_path = SAVE_DIR / file_name
    plt.savefig(full_path, bbox_inches="tight", pad_inches=0.02)

    print("-" * 30)
    print(f"PLOT SUCCESSFUL")
    print(f"Folder: {SAVE_DIR}")
    print(f"File:   {file_name}")
    print("-" * 30)


if __name__ == "__main__":
    main()
