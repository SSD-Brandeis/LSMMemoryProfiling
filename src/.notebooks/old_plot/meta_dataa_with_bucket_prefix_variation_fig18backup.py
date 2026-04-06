from typing import List, Dict, Any, Optional
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Assuming these are defined in your local 'plot' module
from plot import *
from plot.utils import process_LOG_file, buffer_dir

# TAG configurations
TAG = "vary1to4prefixnew-lowpri_false"

# Directory Setup
# This points to /Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot
BASE_PLOT_DIR = Path("/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot")

# Automatically get the script name (e.g., meta_data_with_bucket_prefix_variation_fig18)
# .stem removes the .py extension
SCRIPT_NAME = Path(__file__).stem 

# Define the final nested directory: .../paper_plot/meta_data_with_bucket_prefix_variation_fig18
SAVE_DIR = BASE_PLOT_DIR / SCRIPT_NAME

# Create the folder and any missing parents (like paper_plot itself)
SAVE_DIR.mkdir(parents=True, exist_ok=True)

# Note: EXP_DIR must be imported from 'plot' or defined here
STATS_DIR = EXP_DIR / "filter_result_bucket_prefix"

# Constants
INSERTS = 500_000
UPDATES = 0
POINT_QUERIES = 0
RANGE_QUERIES = 0
SELECTIVITY = 0
SIZE_RATIO = 10
ENTRY_SIZE = 1024
ENTRIES_PER_PAGE = 4
BUFFER_SIZE_IN_MB = 15

# Logic for TAG-based parameters
if TAG == "varyprefix-lowpri_false":
    PREFIX_LENGTHS = [2, 4, 6, 8, 10]
    BUCKET_COUNTS = [100_000]
elif TAG == "varybucketcount-lowpri_true":
    PREFIX_LENGTHS = [4]
    BUCKET_COUNTS = [1, 200_000, 400_000, 600_000, 800_000, 1_000_000]
elif TAG == "vary1to4prefixnew-lowpri_false":
    PREFIX_LENGTHS = [1, 2, 3, 4]
    BUCKET_COUNTS = [100_000]

BUFFERS_TO_PLOT = [
    "Vector",
    "skiplist",
    "hash_skip_list",
    "hash_linked_list",
]

FIGSIZE = (5, 3.6)

def get_data() -> List[Dict[str, Any]]:
    data = list()
    for buffer in BUFFERS_TO_PLOT:
        for prefix_len in PREFIX_LENGTHS:
            for bucket_count in BUCKET_COUNTS:
                num_pages = (
                    BUFFER_SIZE_IN_MB * 1024 * 1024 // (ENTRY_SIZE * ENTRIES_PER_PAGE)
                )
                log_dir = (
                    STATS_DIR
                    / f"{TAG}-{BUFFER_SIZE_IN_MB}M-I{INSERTS}-U{UPDATES}-Q{POINT_QUERIES}-S{RANGE_QUERIES}-Y{SELECTIVITY}-T{SIZE_RATIO}-P{num_pages}-B{ENTRIES_PER_PAGE}-E{ENTRY_SIZE}"
                    / f"{buffer_dir(buffer, prefix_len, bucket_count)}"
                )

                if not log_dir.exists():
                    print(f"Skipping missing directory: {log_dir.name}")
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
                data.append({
                    "buffer": buffer,
                    "prefix_length": prefix_len,
                    "bucket_count": bucket_count,
                    "mean_capacity": mean_capacity / (1024 * 1024), 
                })
    return data

def main():
    data = get_data()

    if not data:
        print("No data found to plot.")
        return
    
    df = pd.DataFrame(data)

    # Plotting Logic
    fig, ax = plt.subplots(figsize=FIGSIZE)
    
    # Filename prefix based on TAG logic
    if TAG in ["varyprefix-lowpri_false", "vary1to4prefixnew-lowpri_false"]:
        file_name = f"capacity_vs_prefix_{TAG}.pdf"
        x_col = "prefix_length"
        ax.set_xlabel("prefix length")
        ax.set_xticks(PREFIX_LENGTHS)
    else:
        file_name = f"capacity_vs_bucket_count_{TAG}.pdf"
        x_col = "bucket_count"
        ax.set_xlabel("bucket count (K)")
        ax.set_xticks(BUCKET_COUNTS)
        ax.set_xticklabels([1] + [f"{v//1000}" for v in BUCKET_COUNTS[1:]])

    for buffer in BUFFERS_TO_PLOT:
        buffer_data = df[df["buffer"] == buffer]
        if buffer_data.empty:
            continue

        style = line_styles.get(buffer, {}).copy()
        
        # Append dynamic labels for legend
        if buffer in ("hash_skip_list", "hash_linked_list"):
            if x_col == "prefix_length":
                style["label"] = style.get("label", "") + f" H={buffer_data['bucket_count'].iloc[0]//1000}K"
            else:
                style["label"] = style.get("label", "") + f" X={buffer_data['prefix_length'].iloc[0]}"

        ax.plot(buffer_data[x_col], buffer_data["mean_capacity"], **style)

    ax.set_ylabel("mean capacity (MB)")
    ax.set_ylim(0, BUFFER_SIZE_IN_MB + 1)

    fig.legend(loc="upper center", ncol=1, bbox_to_anchor=(0.58, 0.65), frameon=False, 
               labelspacing=0.04, handletextpad=0.5)

    plt.tight_layout()
    
    # Save using the absolute path in the nested folder
    full_path = SAVE_DIR / file_name
    plt.savefig(full_path, bbox_inches="tight", pad_inches=0.02)
    
    print("-" * 30)
    print(f"PLOT SUCCESSFUL")
    print(f"Folder: {SAVE_DIR}")
    print(f"File:   {file_name}")
    print("-" * 30)

if __name__ == "__main__":
    main()