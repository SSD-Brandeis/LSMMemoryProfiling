from typing import List, Dict, Any

import pandas as pd

from plot import *
from plot.utils import process_LOG_file, buffer_dir

TAG = "wastagebuffer-lowpri_false"

STATS_DIR = EXP_DIR / "filter_result_bufferwastage"
CURR_DIR = Path(__file__).parent

UPDATES = 0
POINT_QUERIES = 0
RANGE_QUERIES = 0
SELECTIVITY = 0
SIZE_RATIO = 10

PREFIX_LEN = 4
BUCKET_COUNT = 100000

ENTRY_SIZE = 1024
ENTRIES_PER_PAGE = 4

INSERTS = [18022, 36044, 72089, 144179, 288358, 576716, 1153433, 2306867]
BUFFER_SIZES_IN_MB = [8, 16, 32, 64, 128, 256, 512, 1024]

BUFFERS_TO_PLOT = [
    "hash_linked_list",
    "hash_linked_list_optimized",
    "hash_skip_list",
    # "AlwaysSortedVector",
    # "UnsortedVector",
    "skiplist",
    "Vector",
]

FIGSIZE = (6.5, 3.6)


def get_data() -> List[Dict[str, Any]]:
    data = list()

    for buffer_size, inserts in zip(BUFFER_SIZES_IN_MB, INSERTS):
        for buffer in BUFFERS_TO_PLOT:
            num_pages = buffer_size * 1024 * 1024 // (ENTRY_SIZE * ENTRIES_PER_PAGE)
            log_dir = (
                STATS_DIR
                / f"{TAG}-{buffer_size}M-I{inserts}-U{UPDATES}-Q{POINT_QUERIES}-S{RANGE_QUERIES}-Y{SELECTIVITY}-T{SIZE_RATIO}-P{num_pages}-B{ENTRIES_PER_PAGE}-E{ENTRY_SIZE}"
                / f"{buffer_dir(buffer, PREFIX_LEN, BUCKET_COUNT)}"
            )

            if not log_dir.exists():
                raise FileNotFoundError(f"Log directory does not exist: {log_dir}")

            total_data_size = 0
            total_run = 0
            for run in range(1, 4):
                log_file = log_dir / f"LOG{run}"
                if not log_file.exists():
                    raise FileNotFoundError(f"Log file does not exist: {log_file}")

                total_data_size += process_LOG_file(log_file)
                total_run += 1

            total_data_size_avg = total_data_size / total_run
            total_data_size_mb = (buffer_size * 1024 * 1024 - total_data_size_avg) / (
                1024 * 1024
            )
            data.append(
                {
                    "buffer_size_mb": buffer_size,
                    "buffer_type": buffer,
                    "metadata_over_mb": total_data_size_mb,
                }
            )
    return data


def main():
    data = get_data()

    if not data:
        print("No data to plot")
        return

    df = pd.DataFrame(data)

    fig, ax = plt.subplots(figsize=FIGSIZE)

    for buffer in BUFFERS_TO_PLOT:
        buffer_data = df[df.buffer_type == buffer]

        if buffer in (
            "hash_linked_list",
            "hash_skip_list",
        ) and "X" not in line_styles.get(buffer).get("label"):
            line_styles[buffer]["label"] = (
                line_styles[buffer].get("label")
                + f" X={PREFIX_LEN} H={BUCKET_COUNT//1000}K"
            )

        ax.plot(
            buffer_data.buffer_size_mb,
            buffer_data.metadata_over_mb,
            **line_styles.get(buffer, {}),
        )

    ax.set_xlabel("buffer size (MB)")
    ax.set_ylabel("metadata overhead (MB)")
    ax.set_ylim(0, 80)
    ax.set_xscale("log", base=2)
    ax.yaxis.set_label_coords(-0.08, 0.42)

    fig.legend(
        loc="upper left",
        bbox_to_anchor=(0.2, 0.84),
        ncol=1,
        frameon=False,
        borderaxespad=0,
        labelspacing=0,
        borderpad=0,
    )

    plt.tight_layout()
    plt.savefig(
        CURR_DIR / f"metadata_overhead_buffer_size.pdf",
        bbox_inches="tight",
        pad_inches=0.02,
    )
    print(f"[saved] metadata_overhead_buffer_size.pdf")

    # handles, labels = ax.get_legend_handles_labels()
    # legend_fig = plt.figure(figsize=(8, 2))
    # legend_fig.legend(
    #     handles,
    #     labels,
    #     loc="center",
    #     ncol=2,
    #     frameon=False,
    #     borderaxespad=0,
    #     labelspacing=0,
    #     borderpad=0,
    # )
    # legend_fig.savefig(
    #     CURR_DIR / "metadata_overhead_vs_buffer_legend.pdf",
    #     bbox_inches="tight",
    #     pad_inches=0.02,
    # )
    # print("[saved] metadata_overhead_vs_buffer_legend.pdf")


if __name__ == "__main__":
    main()
