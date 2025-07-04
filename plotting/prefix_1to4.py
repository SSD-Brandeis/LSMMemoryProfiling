import os
import re
import pandas as pd
import matplotlib.pyplot as plt


BASE_DIR = "/home/cc/LSMMemoryProfiling/.result/memory_footprint"

PREFIX_MAIN_DIR = "vary_prefix"
BUCKET_MAIN_DIR = "vary_bucket"

# Example subdir name: "varying_prefix_4kb_page_16mb_buffer_PL4" => "PL(\d+)"
prefix_subdir_pattern = re.compile(r"varying_prefix_4kb_page_16mb_buffer_PL(\d+)")
# Example subdir name: "varying_bucket_fix_prefixlength_4kb_page_15mb_buffer_BC(\d+)"
bucket_subdir_pattern = re.compile(r"varying_bucket_fix_prefixlength_4kb_page_15mb_buffer_BC(\d+)")

# Data structure names appear at the start of subdir, e.g. "hash_skip_list-4kb_page_entry_128b_..."
ds_pattern = re.compile(r"(skiplist|vector|linklist|hash_skip_list|hash_linked_list)")

# Regex for total_data_size in LOG
TOTAL_SIZE_REGEX = re.compile(r'"total_data_size"\s*:\s*(\d+)')

# Style definitions for each data structure (hash = special label)
base_style_map = {
    "skiplist": {
        "marker": "o",
        "linestyle": "-",
        "base_label": "skiplist"
    },
    "vector": {
        "marker": "s",
        "linestyle": "--",
        "base_label": "vector"
    },
    "linklist": {
        "marker": "v",
        "linestyle": ":",
        "base_label": "linklist"
    },
    "hash_skip_list": {
        "marker": "D",
        "linestyle": "-.",
        "base_label": "hash skip-list"
    },
    "hash_linked_list": {
        "marker": "^",
        "linestyle": (0, (3,1,1,1)),
        "base_label": "hash linked-list"
    },
}

# -------------------------------------------------------------------------
# 2) Crawl directories, parse data
# -------------------------------------------------------------------------
prefix_data = []
for root, dirs, files in os.walk(BASE_DIR):
    if "LOG" not in files:
        continue

    parts = os.path.normpath(root).split(os.sep)
    if PREFIX_MAIN_DIR not in parts:
        continue

    # parse prefix length
    pl = None
    for part in parts:
        m = prefix_subdir_pattern.match(part)
        if m:
            pl = int(m.group(1))
            break

    # parse data structure
    ds = None
    for part in parts:
        dm = ds_pattern.match(part)
        if dm:
            ds = dm.group(1)
            break
    if ds is None:
        ds = "unknown_ds"

    # read total_data_size
    with open(os.path.join(root, "LOG"), "r", encoding="utf-8", errors="ignore") as f:
        txt = f.read()
    m = TOTAL_SIZE_REGEX.search(txt)
    if not m:
        continue
    total_bytes = int(m.group(1))

    prefix_data.append({
        "data_structure": ds,
        "prefix_length": pl,
        "mean_bytes": total_bytes
    })

# -------------------------------------------------------------------------
# 3) Build DataFrame and mean-aggregate if duplicates
# -------------------------------------------------------------------------
rows = []
for (ds, pl), grp in pd.DataFrame(prefix_data).groupby(["data_structure", "prefix_length"]):
    mean_bytes = grp["mean_bytes"].mean()
    rows.append({"data_structure": ds, "prefix_length": pl, "mean_mb": mean_bytes / (1024*1024)})
df = pd.DataFrame(rows)

# -------------------------------------------------------------------------
# 4) Filter to PL 1–4 and reindex for plotting
# -------------------------------------------------------------------------
valid_pl = [1, 2, 3, 4]
df = df[df["prefix_length"].isin(valid_pl)].copy()
df["x_index"] = df["prefix_length"].apply(lambda p: valid_pl.index(p))

# -------------------------------------------------------------------------
# 5) Plot (only vary_prefix) with y-ticks [0,5,10,15,16]
# -------------------------------------------------------------------------
fig, ax = plt.subplots(figsize=(8, 5))

for ds in df["data_structure"].unique():
    sub = df[df["data_structure"] == ds].sort_values("x_index")
    style = base_style_map.get(ds, {"marker":"o","linestyle":"-","base_label":ds})
    label = style["base_label"]
    if ds in ["hash_skip_list", "hash_linked_list"]:
        label += " H=100K"
    ax.plot(
        sub["x_index"],
        sub["mean_mb"],
        marker=style["marker"],
        linestyle=style["linestyle"],
        label=label
    )

# hard-code buffer-size line at 16 MB
ax.axhline(16, color="brown", linewidth=2, label="buffer size")

ax.set_xticks(range(len(valid_pl)))
ax.set_xticklabels([str(p) for p in valid_pl])
ax.set_xlabel("Prefix Length")
ax.set_ylabel("Mean Capacity of Buffer (MB)")
ax.set_title("")

# here we include 16 as the top tick
ax.set_yticks([0, 5, 10, 15, 16])
ax.set_ylim(bottom=0, top=16)

ax.legend(loc="best")
plt.tight_layout()
plt.savefig("pl_bc__hash_label_prefix_only.png", dpi=300)
plt.show()

print("Done.")
