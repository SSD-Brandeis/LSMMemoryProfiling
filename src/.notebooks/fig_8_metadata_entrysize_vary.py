import re
from typing import List, Dict, Any
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from plot import *
from plot.utils import process_LOG_file

# --- CONFIGURATION ---
CURR_DIR = Path(__file__).parent
# DATA_DIR = EXP_DIR / "fig8_vary_entrysize"
DATA_DIR = Path("/Users/cba/Desktop/LSM/LSMMemoryProfiling/data_new/fig8_vary_entrysize")
BUFFER_SIZE_IN_MB = 128
PREFIX_LEN = 6
BUCKET_COUNT = 100000
FIGSIZE = (4, 3.2) # Matched exactly

USE_LOG_Y = False # Consistent with other plot

BUFFERS_TO_PLOT = [
    "vector-dynamic", "unsortedvector-dynamic", "alwayssortedVector-dynamic",
    "skiplist", "linkedlist", "hash_skip_list", "hash_linked_list", "hash_vector",
    "simple_skiplist",
]

# BUFFERS_TO_PLOT = [
#     "vector-dynamic", 
#     "skiplist", "linkedlist", "hash_skip_list", "hash_linked_list", "hash_vector",
# ]

def get_experiment_data() -> pd.DataFrame:
    records = []
    buf_bytes = BUFFER_SIZE_IN_MB * 1024 * 1024
    if not DATA_DIR.exists(): return pd.DataFrame()

    for buffer_path in DATA_DIR.rglob("buffer-*"):
        if not buffer_path.is_dir(): continue
        
        match = re.match(r"buffer-\d+-(.*?)(?:-X\d+)?(?:-H\d+)?(?:-E(\d+))?$", buffer_path.name)
        if not match: continue
        
        name = match.group(1)
        entry_size_str = match.group(2)
        
        if entry_size_str is None:
            parent_match = re.search(r"-E-(\d+)-", buffer_path.parent.name)
            if parent_match:
                entry_size_str = parent_match.group(1)
        
        if entry_size_str is None: continue
        entry_size = int(entry_size_str)
        
        log_file = buffer_path / "LOG_rocksdb" if (buffer_path / "LOG_rocksdb").exists() else buffer_path / "LOG1"
        if not log_file.exists(): continue

        actual_bytes = process_LOG_file(str(log_file))
        if actual_bytes <= 0: continue
        
        # Changed to MB for consistency with the buffer script
        overhead_mb = (buf_bytes - actual_bytes) / (1024 * 1024)
        records.append({"buffer": name, "entry_size": entry_size, "overhead_mb": overhead_mb})
    return pd.DataFrame(records)

def main():
    df = get_experiment_data()
    if df.empty: return
    df = df.groupby(["buffer", "entry_size"], as_index=False).mean()
    df = df[df["buffer"].isin(BUFFERS_TO_PLOT)]

    fig, ax = plt.subplots(figsize=FIGSIZE)
    for buffer_name in BUFFERS_TO_PLOT:
        subset = df[df["buffer"] == buffer_name].sort_values("entry_size")
        if subset.empty: continue
        style = line_styles.get(buffer_name, {}).copy()
        if "hash" in buffer_name and "X=" not in style.get("label", ""):
            style["label"] = f"{style.get('label', buffer_name)} X={PREFIX_LEN} H={BUCKET_COUNT//1000}K"
        ax.plot(subset["entry_size"], subset["overhead_mb"], **style)

    ax.set_xlabel("entry size (B)")
    ax.set_ylabel("metadata overhead (MB)", labelpad=0.1, loc="top") # Updated unit to MB
    # ax.set_ylabel("overhead (KB)", labelpad=0.1, loc="top") 
    if USE_LOG_Y:
        ax.set_yscale("log")
        ax.set_ylim(bottom=0.1, top=100)
        ax.set_yticks([0.1, 1, 10, 100])
    else:
        ax.set_ylim(bottom=0)
        # Explicit y-tick control for MB range
        # ax.set_yticks([0, 2, 4, 6, 8, 10])
        ax.set_yticks([0, 50, 100])

    ax.set_xscale("log", base=2)
# Ticks at 8, 32, 128, 512, 2048
    ax.set_xticks([2**3, 2**5, 2**7, 2**9, 2**11])
    ax.set_xticklabels(["$8$", "$32$", "$128$", "$512$", "$2048$"])
    plt.tight_layout()
    
    save_dir = CURR_DIR / "output_plots"
    save_dir.mkdir(parents=True, exist_ok=True)
    
    # Store path in variable and print resolved absolute path
    save_path = save_dir / "metadata_overhead_entry_size.pdf"
    plt.savefig(save_path, bbox_inches="tight", pad_inches=0.02)
    print(f"Plot saved to: {save_path.resolve()}")
    
    plt.close(fig)

if __name__ == "__main__":
    main()