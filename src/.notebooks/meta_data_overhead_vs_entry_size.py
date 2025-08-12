from typing import List, Dict, Any

import pandas as pd

from plot import *
from plot.utils import process_LOG_file, buffer_dir

TAG = "metadata-lowpri_false"

STATS_DIR = EXP_DIR / "filter_result_metadata"
CURR_DIR = Path(__file__).parent

INSERTS = 1_000_000
UPDATES = 0
POINT_QUERIES = 0
RANGE_QUERIES = 0
SELECTIVITY = 0
SIZE_RATIO = 10
NUM_PAGES = [1_024, 2_048, 4_096]
PAGE_SIZES = [2_048, 4_096, 8_192]

PREFIX_LEN = 4
BUCKET_COUNT = 100000

ENTRY_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024]  # in bytes

BUFFER_SIZE_IN_MB = 8

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
        for pages in NUM_PAGES:
            for entry_size in ENTRY_SIZES:
                entries_per_page = (
                    BUFFER_SIZE_IN_MB * 1024 * 1024 // (entry_size * pages)
                )
                log_dir = (
                    STATS_DIR
                    / f"{TAG}-I{INSERTS}-U{UPDATES}-Q{POINT_QUERIES}-S{RANGE_QUERIES}-Y{SELECTIVITY}-T{SIZE_RATIO}-P{pages}-B{entries_per_page}-E{entry_size}"
                    / f"{buffer_dir(buffer, PREFIX_LEN, BUCKET_COUNT)}"
                )

                if not log_dir.exists():
                    raise FileNotFoundError(f"Log file {log_dir} does not exist.")

                total_data_size = 0
                total_run = 0
                for run in range(1, 4):
                    log_file = log_dir / f"LOG{run}"
                    if not log_file.exists():
                        raise FileNotFoundError(f"Log file {log_file} does not exist.")

                    total_data_size += process_LOG_file(log_file)
                    total_run += 1

                total_data_size_avg = total_data_size / total_run
                total_data_size_mb = (
                    BUFFER_SIZE_IN_MB * 1024 * 1024 - total_data_size_avg
                ) / (1024 * 1024)
                data.append(
                    {
                        "buffer": buffer,
                        "page_size": entry_size * entries_per_page,
                        "entry_size": entry_size,
                        "total_data_size_mb": total_data_size_mb,
                    }
                )
    return data


def main():
    data = get_data()

    if not data:
        print("No data to plot")
        return

    df = pd.DataFrame(data)

    for page_size in PAGE_SIZES:
        sub = df[df["page_size"] == page_size]

        if sub.empty:
            print(f"No data for page size {page_size}")
            continue

        fig, ax = plt.subplots(figsize=FIGSIZE)
        for buffer in BUFFERS_TO_PLOT:
            buffer_data = sub[sub["buffer"] == buffer]
            if buffer in (
                "hash_linked_list",
                "hash_skip_list",
            ) and "X" not in line_styles.get(buffer).get("label"):
                line_styles[buffer]["label"] = (
                    line_styles[buffer].get("label")
                    + f" X={PREFIX_LEN} H={BUCKET_COUNT//1000}K"
                )
            ax.plot(
                buffer_data["entry_size"],
                buffer_data["total_data_size_mb"],
                **line_styles.get(buffer, {}),
            )

        ax.set_xlabel("entry size (Bytes)")
        ax.set_ylabel("metadata overhead (MB)")
        ax.set_ylim(0, BUFFER_SIZE_IN_MB)
        ax.set_xscale("log", base=2)
        ax.yaxis.set_label_coords(-0.07, 0.42)
        ax.text(140, 7.2, f"page size={page_size//1024}kb")

        plt.tight_layout()
        plt.savefig(
            f"{CURR_DIR}/metadata_overhead_{page_size}kb.pdf",
            bbox_inches="tight",
            pad_inches=0.02,
        )
        print(f"[saved] metadata_overhead_{page_size}kb.pdf")

    handles, labels = ax.get_legend_handles_labels()

    # Create a new figure solely for the legend
    legend_fig = plt.figure(figsize=(8, 2))
    legend_fig.legend(
        handles,
        labels,
        loc="center",
        ncol=4,
        frameon=False,
        borderaxespad=0,
        labelspacing=0,
        borderpad=0,
    )
    legend_fig.savefig(
        f"{CURR_DIR}/metadata_overhead_legend.pdf",
        bbox_inches="tight",
        pad_inches=0.012,
    )
    print("[saved] metadata_overhead_legend.pdf")


if __name__ == "__main__":
    main()
