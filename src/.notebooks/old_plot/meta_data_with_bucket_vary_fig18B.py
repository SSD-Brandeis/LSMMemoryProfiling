from typing import List, Dict, Any, Optional
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Keeping your original imports exactly as requested for style consistency
from plot import *
from plot.utils import process_LOG_file

# --- CONFIGURATION ---
# Points to the new data location
STATS_DIR = Path(
    "/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_mar16_fig18C/fig18_partc_single-I800000_hashvec_makeup-lowpri_false-128M-P32768-E128"
)


Y_BOTTOM = 0
Y_TOP = 136

BASE_PLOT_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot")
SCRIPT_NAME = Path(__file__).stem
SAVE_DIR = BASE_PLOT_DIR / SCRIPT_NAME
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# Constants
BUFFER_SIZE_IN_MB = 128
PREFIX_LEN = 6 # From the X6 in your folder names
BUCKET_COUNTS = [1000, 200000, 400000, 600000, 800000, 1000000]

# Buffers based on your new directory tree
BUFFERS_TO_PLOT = [
    "Vector-dynamic",
    # "Vector-preallocated",
    "skiplist",
    "hash_skip_list",
    "hash_linked_list",
    "hash_vector",
]

FIGSIZE = (5, 3.6)

def get_data() -> List[Dict[str, Any]]:
    data = list()
    for buffer in BUFFERS_TO_PLOT:
        # Determine if the buffer iterates buckets or is a static baseline
        search_buckets = BUCKET_COUNTS if "hash" in buffer else [None]
        
        for bc in search_buckets:
            if bc is not None:
                # Matches folders like: hash_linked_list-X6-H1
                log_dir = STATS_DIR / f"{buffer}-X{PREFIX_LEN}-H{bc}"
            else:
                # Matches folders like: Vector-dynamic or skiplist
                log_dir = STATS_DIR / buffer

            if not log_dir.exists():
                continue

            total_data_size = 0
            total_run = 0
            # Check for LOG1, LOG2, LOG3 as in your standard logic
            for run in range(1, 4):
                log_path = log_dir / f"LOG{run}"
                if log_path.exists():
                    total_data_size += process_LOG_file(log_path)
                    total_run += 1

            if total_run == 0:
                continue

            mean_capacity = total_data_size / total_run
            data.append({
                "buffer": buffer,
                "bucket_count": bc if bc is not None else 0,
                "mean_capacity": mean_capacity / (1024 * 1024),
            })
    return data

def main():
    data = get_data()

    if not data:
        print("No data found to plot.")
        return

    df = pd.DataFrame(data)
    fig, ax = plt.subplots(figsize=FIGSIZE)

    file_name = f"capacity_vs_bucket_count_fig18b.pdf"
    x_col = "bucket_count"

    for buffer in BUFFERS_TO_PLOT:
        buffer_data = df[df["buffer"] == buffer]
        if buffer_data.empty:
            continue

        style = line_styles.get(buffer, {}).copy()
        # Fallback to base name if specific Vector-dynamic/preallocated key is missing
        if not style:
            base_key = "Vector" if "Vector" in buffer else buffer
            style = line_styles.get(base_key, {}).copy()

        # Build label inside style dict to avoid "got multiple values for label" error
        if "hash" in buffer:
            style["label"] = style.get("label", buffer) + f" X={PREFIX_LEN}"
        else:
            style["label"] = style.get("label", buffer)
            style["marker"] = None

        if "hash" not in buffer:
            # Baseline horizontal lines
            val = buffer_data["mean_capacity"].iloc[0]
            ax.axhline(y=val, **style)
        else:
            # Main data plot
            ax.plot(buffer_data[x_col], buffer_data["mean_capacity"], **style)

    # --- STYLE ELEMENTS FROM FIG18A ---
    # Dashed line for buffer limit
    ax.axhline(
        y=BUFFER_SIZE_IN_MB, color="black", linestyle="--", linewidth=1, zorder=1
    )

    ax.set_xlabel("bucket count (K)", labelpad=-1)
    ax.set_ylabel("mean capacity (MB)", labelpad=-1)
    
    # --- STRICT TICK CONTROL ---
    ax.set_xticks(BUCKET_COUNTS)
    ax.set_xticklabels([f"{v//1000}" if v >= 1000 else str(v) for v in BUCKET_COUNTS])
    
    ax.set_ylim(Y_BOTTOM, Y_TOP)

    # Legend repositioning from fig18A script
    fig.legend(
        loc="upper center",
        ncol=1,
        bbox_to_anchor=(0.58, 0.62),
        frameon=False,
        labelspacing=0.04,
        handletextpad=0.5,
    )

    plt.tight_layout()

    full_path = SAVE_DIR / file_name
    plt.savefig(full_path, bbox_inches="tight", pad_inches=0.02)

    print("-" * 30)
    print(f"PLOT SUCCESSFUL: {full_path}")
    print("-" * 30)

if __name__ == "__main__":
    main()