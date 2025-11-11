from typing import List, Dict, Any, Optional

import pandas as pd

from plot import *
from plot.utils import process_LOG_file, buffer_dir

# TAG = TAG_prefix = "varyprefix-lowpri_false"
TAG = TAG_vary_1_to_4 = "vary1to4prefixnew-lowpri_false"
# TAG = TAG_vary_bucket_count = "varybucketcount-lowpri_true"

STATS_DIR = EXP_DIR / "filter_result_bucket_prefix"
CURR_DIR = Path(__file__).parent

INSERTS = 500_000
UPDATES = 0
POINT_QUERIES = 0
RANGE_QUERIES = 0
SELECTIVITY = 0
SIZE_RATIO = 10

if TAG == "varyprefix-lowpri_false":
    PREFIX_LENGTHS = [2, 4, 6, 8, 10]
    BUCKET_COUNTS = [100_000]
elif TAG == "varybucketcount-lowpri_true":
    PREFIX_LENGTHS = [4]
    BUCKET_COUNTS = [1, 200_000, 400_000, 600_000, 800_000, 1_000_000]
elif TAG == "vary1to4prefixnew-lowpri_false":
    PREFIX_LENGTHS = [1, 2, 3, 4]
    BUCKET_COUNTS = [100_000]


ENTRY_SIZE = 1024
ENTRIES_PER_PAGE = 4
BUFFER_SIZE_IN_MB = 15

BUFFERS_TO_PLOT = [
    # "AlwaysSortedVector",
    # "UnsortedVector",
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
                    raise FileNotFoundError(f"Log file {log_dir} does not exist.")

                total_data_size = 0
                total_run = 0
                for run in range(1, 4):
                    log_path = log_dir / f"LOG{run}"
                    if log_path.exists():
                        total_data_size += process_LOG_file(log_path)
                        total_run += 1

                mean_capacity = total_data_size / total_run

                data.append({
                    "buffer": buffer,
                    "prefix_length": prefix_len,
                    "bucket_count": bucket_count,
                    "mean_capacity": mean_capacity / (1024 * 1024),  # Convert to MB,
                })
    return data

def main():
    data = get_data()

    if not data:
        print("No data found.")
        return
    
    df = pd.DataFrame(data)

    if TAG in ["varyprefix-lowpri_false", "vary1to4prefixnew-lowpri_false"]:
        fig, ax = plt.subplots(figsize=FIGSIZE)
        for buffer in BUFFERS_TO_PLOT:
            buffer_data = df[df["buffer"] == buffer]

            if buffer_data.empty:
                print(f"No data for buffer {buffer}.")
                continue

            if buffer in ("hash_skip_list", "hash_linked_list"):
                line_styles[buffer]["label"] = (
                    line_styles[buffer].get("label", "")
                    + f" H={buffer_data['bucket_count'].iloc[0]//1000}K"
                )

            ax.plot(
                buffer_data["prefix_length"],
                buffer_data["mean_capacity"],
                **line_styles.get(buffer, {})
            )
        ax.set_xlabel("prefix length")
        ax.set_ylabel("mean capacity (MB)")
        ax.set_ylim(0, BUFFER_SIZE_IN_MB+1)
        ax.set_xticks(PREFIX_LENGTHS)

        # if TAG == "varyprefix-lowpri_false":
        fig.legend(
            loc="upper center",
            ncol=1,
            bbox_to_anchor=(0.58, 0.65),
            frameon=False,
            labelspacing=0.04,
            # columnspacing=0.2,
            handletextpad=0.5,
        )

        plt.tight_layout()
        plt.savefig(CURR_DIR / f"capacity_vs_prefix_{TAG}.pdf",
            bbox_inches="tight",
            pad_inches=0.02,
        )
        print(f"[saved] capacity_vs_prefix_{TAG}.pdf")
    
    elif TAG == "varybucketcount-lowpri_true":
        fig, ax = plt.subplots(figsize=FIGSIZE)
        for buffer in BUFFERS_TO_PLOT:
            buffer_data = df[df["buffer"] == buffer]

            if buffer_data.empty:
                print(f"No data for buffer {buffer}.")
                continue

            if buffer in ("hash_skip_list", "hash_linked_list"):
                line_styles[buffer]["label"] = (
                    line_styles[buffer].get("label", "")
                    + f" X={buffer_data['prefix_length'].iloc[0]}"
                )

            ax.plot(
                buffer_data["bucket_count"],
                buffer_data["mean_capacity"],
                **line_styles.get(buffer, {})
            )
        ax.set_xlabel("bucket count (K)")
        ax.set_ylabel("mean capacity (MB)")
        ax.set_ylim(0, BUFFER_SIZE_IN_MB+1)
        ax.set_xticks(BUCKET_COUNTS)
        ax.set_xticklabels([1] + [f"{v//1000}" for v in BUCKET_COUNTS[1:]])

        fig.legend(
            loc="upper center",
            ncol=1,
            bbox_to_anchor=(0.58, 0.65),
            frameon=False,
            labelspacing=0.04,
            # columnspacing=0.2,
            handletextpad=0.5,
        )

        plt.tight_layout()
        plt.savefig(CURR_DIR / f"capacity_vs_bucket_count_{TAG}.pdf",
            bbox_inches="tight",
            pad_inches=0.02,
        )
        print(f"[saved] capacity_vs_bucket_count_{TAG}.pdf")


if __name__ == "__main__":
    main()
