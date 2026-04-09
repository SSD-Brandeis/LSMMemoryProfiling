# notebooks/fig_8_metadata_entrysize_vary.py

import re
from typing import List, Dict, Any
from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt

from plot import *
from plot.utils import process_LOG_file

CURR_DIR = Path(__file__).parent
DATA_DIR = EXP_DIR / "fig8_vary_entrysize"

BUFFER_SIZE_IN_MB = 128
PREFIX_LEN = 6
BUCKET_COUNT = 100000
FIGSIZE = (5, 3.6)


def extract_detailed_metrics(log_path: Path) -> Dict[str, float]:
    metrics = {
        "inserted_data_size": float("nan"),
        "entries_in_first_flush": float("nan")
    }
    
    if not log_path.exists():
        return metrics

    pattern_inserted = re.compile(r'"inserted_data_size":\s(\d+),') 
    pattern_entries = re.compile(r'"num_entries":\s(\d+),')

    with open(log_path, 'r') as f:
        for line in f:
            if pd.isna(metrics["inserted_data_size"]):
                m_inserted = pattern_inserted.search(line)
                if m_inserted:
                    metrics["inserted_data_size"] = float(m_inserted.group(1))

            if pd.isna(metrics["entries_in_first_flush"]):
                m_entries = pattern_entries.search(line)
                if m_entries:
                    metrics["entries_in_first_flush"] = float(m_entries.group(1))

            if not pd.isna(metrics["inserted_data_size"]) and not pd.isna(metrics["entries_in_first_flush"]):
                break

    return metrics


def get_data() -> List[Dict[str, Any]]:
    data = []
    buf_bytes = BUFFER_SIZE_IN_MB * 1024 * 1024

    if not DATA_DIR.exists():
        print(f"Directory {DATA_DIR} does not exist.")
        return data

    for exp_dir in DATA_DIR.glob("sanitycheck-fig_8_metadata_varyentry-I*-L*"):
        if not exp_dir.is_dir():
            continue

        for buffer_dir_path in exp_dir.glob("buffer-*"):
            if not buffer_dir_path.is_dir():
                continue

            match = re.match(r"buffer-\d+-(.*?)(?:-H\d+)?-E(\d+)", buffer_dir_path.name)
            if not match:
                continue

            buffer_name = match.group(1)
            entry_size = int(match.group(2))

            log_file = buffer_dir_path / "LOG_rocksdb"
            if not log_file.exists():
                log_file = buffer_dir_path / "LOG1" 

            detailed_metrics = extract_detailed_metrics(log_file)
            
            total_data_size = 0
            if log_file.exists():
                total_data_size = process_LOG_file(str(log_file))
            
            if total_data_size > 0:
                total_data_size_mb = (buf_bytes - total_data_size) / (1024 * 1024)
            else:
                total_data_size_mb = float("nan")

            data.append({
                "buffer": buffer_name,
                "entry_size": entry_size,
                "actual_data_size": total_data_size if total_data_size > 0 else float("nan"),
                "inserted_data_size": detailed_metrics["inserted_data_size"],
                "entries_in_first_flush": detailed_metrics["entries_in_first_flush"],
                "total_data_size_mb": total_data_size_mb
            })

    return data


def main():
    data = get_data()

    if not data:
        print("No data to plot")
        return

    df = pd.DataFrame(data)
    
    df = df.dropna(subset=["total_data_size_mb"])
    df_grouped = df.groupby(["buffer", "entry_size"], as_index=False).mean()

    save_dir = CURR_DIR / "output_plots"
    save_dir.mkdir(parents=True, exist_ok=True)
    saved_files = []

    csv_path = save_dir / "buffer_metrics_verification.csv"
    df_grouped.to_csv(csv_path, index=False)
    saved_files.append(str(csv_path))

    fig, ax = plt.subplots(figsize=FIGSIZE)
    buffers_in_sub = df_grouped["buffer"].unique()

    for buffer in buffers_in_sub:
        buffer_data = df_grouped[df_grouped["buffer"] == buffer].sort_values(by="entry_size")
        if buffer_data.empty:
            continue

        style = line_styles.get(buffer, {}).copy()

        if buffer in ("hash_linked_list", "hash_skip_list") and style and "X" not in style.get("label", ""):
            style["label"] = style.get("label", buffer) + f" X={PREFIX_LEN} H={BUCKET_COUNT//1000}K"

        ax.plot(
            buffer_data["entry_size"],
            buffer_data["total_data_size_mb"],
            **style
        )

    ax.set_xlabel("entry size (Bytes)")
    ax.set_ylabel("metadata overhead (MB)")
    ax.set_ylim(0, BUFFER_SIZE_IN_MB)
    ax.set_xscale("log", base=2)
    
    plt.tight_layout()
    save_path = save_dir / "metadata_overhead_combined.pdf"
    plt.savefig(save_path, bbox_inches="tight", pad_inches=0.02)
    saved_files.append(str(save_path))
    
    handles, labels = ax.get_legend_handles_labels()
    
    if handles:
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
        legend_save_path = save_dir / "metadata_overhead_legend.pdf"
        legend_fig.savefig(legend_save_path, bbox_inches="tight", pad_inches=0.012)
        saved_files.append(str(legend_save_path))
        plt.close(legend_fig)

    plt.close(fig)

    print("\n--- Execution Complete ---")
    for f in saved_files:
        print(f"[saved] {f}")


if __name__ == "__main__":
    main()