import matplotlib
import matplotlib.patches as mpatches
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager
from mpl_toolkits.axes_grid1.inset_locator import inset_axes
import os, re, pandas as pd, numpy as np
from style import line_styles
prop = font_manager.FontProperties(fname="./LinLibertine_Mah.ttf")
plt.rcParams['font.family'] = prop.get_name()
plt.rcParams['text.usetex'] = True
plt.rcParams['font.weight'] = 'bold'
plt.rcParams['font.size'] = 22

DATA_ROOT   = "/Users/cba/Desktop/LSMMemoryBuffer/data"
CRAWL_ROOT  = os.path.join(DATA_ROOT, "filter_result_metadata")
EXTRACT_DIR = os.path.join(DATA_ROOT, "extracted_data")
PLOTS_ROOT  = os.path.join(DATA_ROOT, "plots")
os.makedirs(EXTRACT_DIR, exist_ok=True)
os.makedirs(PLOTS_ROOT,   exist_ok=True)

BUFFER_MB    = 8
BYTES_PER_MB = 1_048_576
ENTRY_SIZES  = [8, 16, 32, 64, 128, 256, 512, 1024]
PAGE_BUCKETS = {"2kb", "4kb", "8kb"}

BUFFERS_TO_PLOT = [
    "Vector",
    "hash_linked_list",
    "hash_skip_list",
    "skiplist",
]

def _scan(prefix: str, token: str) -> int:
    for part in token.split("-"):
        if part.startswith(prefix) and part[1:].isdigit():
            return int(part[1:])
    return 0

def _total(log_path: str) -> int:
    pat = re.compile(r'"total_data_size"\s*:\s*(\d+)')
    with open(log_path) as fh:
        for ln in fh:
            m = pat.search(ln)
            if m:
                return int(m.group(1))
    return 0

records = []
for root, _, files in os.walk(CRAWL_ROOT):
    if "LOG1" not in files:
        continue
    buf_type = os.path.basename(root).split("-", 1)[0]
    params   = os.path.basename(os.path.dirname(root))

    E = _scan("E", params)
    B = _scan("B", params)
    bucket = f"{B*E//1024}kb"
    if bucket not in PAGE_BUCKETS or not (E and B):
        continue

    totals = []
    for run in (1, 2, 3):
        fp = os.path.join(root, f"LOG{run}")
        if os.path.exists(fp):
            totals.append(_total(fp))
    if len(totals) != 3:
        continue

    avg  = sum(totals) / 3
    meta = (BUFFER_MB * BYTES_PER_MB - avg) / BYTES_PER_MB
    records.append({
        "page_bucket":      bucket,
        "buffer_type":      buf_type,
        "entry_size":       E,
        "metadata_over_mb": meta,
    })

df = pd.DataFrame(records)

if not df.empty:
    df.to_csv(os.path.join(EXTRACT_DIR, "metadata_overhead_all.csv"), index=False)

    x_pos = np.arange(len(ENTRY_SIZES))

    for bucket in ["2kb", "4kb", "8kb"]:
        sub = df[df.page_bucket == bucket]
        if sub.empty:
            continue

        pivot = sub.pivot_table(
            values="metadata_over_mb",
            index="buffer_type",
            columns="entry_size",
            aggfunc="mean"
        )

        if BUFFERS_TO_PLOT:
            buffers = [b for b in BUFFERS_TO_PLOT if b in pivot.index]
        else:
            buffers = list(pivot.index)

        fig, ax = plt.subplots(figsize=(9, 4.5))
        for buf in buffers:
            row = pivot.loc[buf]
            ys  = [row.get(sz, np.nan) for sz in ENTRY_SIZES]
            ax.plot(x_pos, ys, **line_styles.get(buf, {}))

        ax.set_xlabel("Entry size (bytes)")
        ax.set_ylabel("Metadata overhead (MB)")
        ax.set_xticks(x_pos)
        ax.set_xticklabels([str(s) for s in ENTRY_SIZES])
        ax.set_ylim(0, 8)
        ax.grid(True, linestyle="--", alpha=0.3)

        handles, labels = ax.get_legend_handles_labels()
        ax.legend(handles, labels,
                  loc="upper right",
                  frameon=False,
                  fontsize=10,
                  borderaxespad=0.3,
                  labelspacing=0.3)

        fig.tight_layout()
        out_dir = os.path.join(PLOTS_ROOT, bucket)
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"metadata_overhead_{bucket}.pdf")
        fig.savefig(out_path, bbox_inches="tight", pad_inches=0.025)
        plt.close(fig)
        print(f"[saved] {out_path}")
