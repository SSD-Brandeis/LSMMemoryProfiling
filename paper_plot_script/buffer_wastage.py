import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager
import os
import re
import math
import pandas as pd
import numpy as np

from style import line_styles

prop = font_manager.FontProperties(fname="./LinLibertine_Mah.ttf")
plt.rcParams["font.family"] = prop.get_name()
plt.rcParams["text.usetex"] = True
plt.rcParams["font.weight"] = "bold"
plt.rcParams["font.size"] = 22

DATA_ROOT   = "/Users/cba/Desktop/LSMMemoryBuffer/data"
CRAWL_ROOT  = os.path.join(DATA_ROOT, "filter_result_bufferwastage")
EXTRACT_DIR = os.path.join(DATA_ROOT, "extracted_data")
PLOTS_ROOT  = os.path.join(DATA_ROOT, "plots")

os.makedirs(EXTRACT_DIR, exist_ok=True)
os.makedirs(PLOTS_ROOT,   exist_ok=True)

BYTES_PER_MB    = 1_048_576
BUFFERS_TO_PLOT = []

def _total(log_path: str) -> int:
    pattern = re.compile(r'"total_data_size"\s*:\s*(\d+)')
    with open(log_path) as fh:
        for line in fh:
            match = pattern.search(line)
            if match:
                return int(match.group(1))
    return 0

def _buffer_mb(tokens: str) -> int:
    match = re.search(r"(\d+)M\b", tokens)
    if match:
        return int(match.group(1))
    return 0

records = []

for root, _, files in os.walk(CRAWL_ROOT):
    if "LOG1" not in files:
        continue

    buf_dir     = os.path.basename(root)
    buffer_type = buf_dir.split("-", 1)[0]

    params_dir = os.path.basename(os.path.dirname(root))
    buffer_mb  = _buffer_mb(params_dir)
    if buffer_mb == 0:
        continue

    totals = []
    for run in (1, 2, 3):
        log_file = os.path.join(root, f"LOG{run}")
        if os.path.exists(log_file):
            totals.append(_total(log_file))
    if len(totals) != 3:
        continue

    avg_bytes = sum(totals) / 3
    meta_mb   = (buffer_mb * BYTES_PER_MB - avg_bytes) / BYTES_PER_MB

    records.append({
        "buffer_mb":        buffer_mb,
        "buffer_type":      buffer_type,
        "metadata_over_mb": meta_mb,
    })

df = pd.DataFrame(records)

if not df.empty:
    csv_path = os.path.join(EXTRACT_DIR, "metadata_overhead_buffer_sizes.csv")
    df.to_csv(csv_path, index=False)

    buffer_sizes = sorted(df.buffer_mb.unique())
    x_positions  = np.arange(len(buffer_sizes))

    buffers = sorted(df.buffer_type.unique())
    if BUFFERS_TO_PLOT:
        buffers = [b for b in BUFFERS_TO_PLOT if b in buffers]

    fig, ax = plt.subplots(figsize=(9, 4.5))

    for buf in buffers:
        ys = []
        for size in buffer_sizes:
            subset = df[(df.buffer_type == buf) & (df.buffer_mb == size)]
            if subset.empty:
                ys.append(np.nan)
            else:
                ys.append(subset["metadata_over_mb"].mean())

        style = line_styles.get(buf, {})
        ax.plot(x_positions, ys, **style)

    ylim = 80
    ax.set_ylim(0, ylim)
    ax.set_yticks(range(0, ylim + 1, 10))

    ax.set_xlabel("Buffer size (MB)")
    ax.set_ylabel("Metadata overhead (MB)")

    ax.set_xticks(x_positions)
    ax.set_xticklabels([str(s) for s in buffer_sizes])

    ax.grid(True, linestyle="--", alpha=0.3)

    handles, labels = ax.get_legend_handles_labels()
    ax.legend(
        handles,
        labels,
        loc="upper left",
        frameon=False,
        fontsize=10,
        borderaxespad=0.3,
        labelspacing=0.3
    )

    fig.tight_layout()
    output_file = os.path.join(PLOTS_ROOT, "metadata_overhead_by_buffer.pdf")
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.10)
    plt.close(fig)

    print(f"[saved] {output_file}")
