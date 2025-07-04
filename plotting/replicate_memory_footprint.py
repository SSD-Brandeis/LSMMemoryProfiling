import os
import re
import pandas as pd
import matplotlib.pyplot as plt


BASE_DIR = "/home/cc/LSMMemoryProfiling/.result/memory_footprint"

PREFIX_MAIN_DIR = "vary_prefix"
BUCKET_MAIN_DIR = "vary_bucket"

# Example subdir name: "varying_prefix_4kb_page_15mb_buffer_PL4" => "PL(\d+)"
prefix_subdir_pattern = re.compile(r"varying_prefix_4kb_page_16mb_buffer_PL(\d+)")
# Example subdir name: "varying_bucket_fix_prefixlength_4kb_page_15mb_buffer_BC200000" => "BC(\d+)"
bucket_subdir_pattern = re.compile(r"varying_bucket_fix_prefixlength_4kb_page_16mb_buffer_BC(\d+)")

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

def short_bc_label(bc_val):
    """Convert numeric bucket counts into short text like '200K' or '1M'."""
    if bc_val is None:
        return ""
    if bc_val >= 1000000 and (bc_val % 1000000 == 0):
        return f"{bc_val//1000000}M"
    elif bc_val >= 1000 and (bc_val % 1000 == 0):
        return f"{bc_val//1000}K"
    else:
        return str(bc_val)

# -------------------------------------------------------------------------
# 2) Crawl directories, parse data
# -------------------------------------------------------------------------
prefix_data = []  # each row: {"data_structure", "prefix_length", "bucket_count", "total_bytes"}
bucket_data = []  # each row: {"data_structure", "bucket_count", "prefix_length", "total_bytes"}

for root, dirs, files in os.walk(BASE_DIR):
    if "LOG" not in files:
        continue
    
    path_parts = os.path.normpath(root).split(os.sep)

    # Figure out which experiment set we are in
    is_prefix_experiment = (PREFIX_MAIN_DIR in path_parts)
    is_bucket_experiment = (BUCKET_MAIN_DIR in path_parts)

    # Attempt to parse prefix length
    found_prefix = None
    for part in path_parts:
        pm = prefix_subdir_pattern.match(part)
        if pm:
            found_prefix = int(pm.group(1))
            break

    # Attempt to parse bucket count
    found_bucket = None
    for part in path_parts:
        bm = bucket_subdir_pattern.match(part)
        if bm:
            found_bucket = int(bm.group(1))
            break

    # Attempt to parse data structure
    ds_name = None
    for part in path_parts:
        dsm = ds_pattern.match(part)
        if dsm:
            ds_name = dsm.group(1)
            break
    if ds_name is None:
        ds_name = "unknown_ds"

    # Read total_data_size from the LOG
    log_path = os.path.join(root, "LOG")
    with open(log_path, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()
    m = TOTAL_SIZE_REGEX.search(content)
    if not m:
        continue
    total_size_bytes = int(m.group(1))

    # Save the row in the appropriate list
    if is_prefix_experiment:
        prefix_data.append({
            "data_structure": ds_name,
            "prefix_length": found_prefix,
            "bucket_count": found_bucket,
            "total_bytes": total_size_bytes
        })
    elif is_bucket_experiment:
        bucket_data.append({
            "data_structure": ds_name,
            "bucket_count": found_bucket,
            "prefix_length": found_prefix,
            "total_bytes": total_size_bytes
        })

# -------------------------------------------------------------------------
# 3) Build DataFrames and average over duplicates
# -------------------------------------------------------------------------
df_prefix = pd.DataFrame(prefix_data)
df_bucket = pd.DataFrame(bucket_data)

# Group by (ds, prefix_length) -> average
dfp_rows = []
for (ds, pl), grp in df_prefix.groupby(["data_structure", "prefix_length"], dropna=False):
    if pd.isna(pl):
        continue
    possible_bcs = grp["bucket_count"].dropna().unique()
    bc_val = possible_bcs[0] if len(possible_bcs) > 0 else None
    mean_bytes = grp["total_bytes"].mean()
    dfp_rows.append({
        "data_structure": ds,
        "prefix_length": pl,
        "bucket_count": bc_val,
        "mean_bytes": mean_bytes
    })

df_prefix_final = pd.DataFrame(dfp_rows)
df_prefix_final["mean_mb"] = df_prefix_final["mean_bytes"] / (1024*1024)

# Group by (ds, bucket_count) -> average
dfb_rows = []
for (ds, bc), grp in df_bucket.groupby(["data_structure", "bucket_count"], dropna=False):
    if pd.isna(bc):
        continue
    possible_pl = grp["prefix_length"].dropna().unique()
    pl_val = possible_pl[0] if len(possible_pl) > 0 else None
    mean_bytes = grp["total_bytes"].mean()
    dfb_rows.append({
        "data_structure": ds,
        "bucket_count": bc,
        "prefix_length": pl_val,
        "mean_bytes": mean_bytes
    })

df_bucket_final = pd.DataFrame(dfb_rows)
df_bucket_final["mean_mb"] = df_bucket_final["mean_bytes"] / (1024*1024)

# -------------------------------------------------------------------------
# 4) Determine all prefix/bucket values we have, sort them, build x-axis
#    indices for a purely categorical plot
# -------------------------------------------------------------------------
all_prefixes = sorted(x["prefix_length"] for x in dfp_rows if x["prefix_length"] is not None)
unique_prefixes = list(dict.fromkeys(all_prefixes))  # preserve sorted order, remove duplicates

all_buckets = sorted(x["bucket_count"] for x in dfb_rows if x["bucket_count"] is not None)
unique_buckets = list(dict.fromkeys(all_buckets))  # preserve sorted order

def prefix_to_x(prefix_val):
    return unique_prefixes.index(prefix_val) if prefix_val in unique_prefixes else None

def bucket_to_x(bc_val):
    return unique_buckets.index(bc_val) if bc_val in unique_buckets else None

df_prefix_final["x_index"] = df_prefix_final["prefix_length"].apply(prefix_to_x)
df_bucket_final["x_index"] = df_bucket_final["bucket_count"].apply(bucket_to_x)

# Build string labels for the x-axis
prefix_labels = [str(x) for x in unique_prefixes]
bucket_labels = [short_bc_label(x) for x in unique_buckets]

# -------------------------------------------------------------------------
# 5) Plot: 2 panels, with "H=100K" or "X=4" appended for hash DS
# -------------------------------------------------------------------------
fig, axes = plt.subplots(1, 2, figsize=(10, 5))

# Panel (a): vary_prefix
ax0 = axes[0]
for ds_name in df_prefix_final["data_structure"].unique():
    sub = df_prefix_final[df_prefix_final["data_structure"] == ds_name].dropna(subset=["x_index"])
    if sub.empty:
        continue
    sub_sorted = sub.sort_values("x_index")

    style = base_style_map.get(ds_name, {"marker":"o","linestyle":"-","base_label":ds_name})
    base_label = style["base_label"]
    
    # **Hard-code** for hash structures: "H=100K"
    if ds_name in ["hash_skip_list", "hash_linked_list"]:
        label = base_label + " H=100K"
    else:
        label = base_label

    ax0.plot(
        sub_sorted["x_index"],
        sub_sorted["mean_mb"],
        marker=style["marker"],
        linestyle=style["linestyle"],
        label=label
    )

ax0.axhline(16, color="brown", linewidth=2, label="buffer size")
ax0.set_xticks(range(len(unique_prefixes)))
ax0.set_xticklabels(prefix_labels)
ax0.set_xlabel("Prefix Length")
ax0.set_ylabel("Mean Capacity of Buffer(MB)")
ax0.set_title("")
ax0.set_ylim(bottom=0)
ax0.legend(loc="best")

# Panel (b): vary_bucket
ax1 = axes[1]
for ds_name in df_bucket_final["data_structure"].unique():
    sub = df_bucket_final[df_bucket_final["data_structure"] == ds_name].dropna(subset=["x_index"])
    if sub.empty:
        continue
    sub_sorted = sub.sort_values("x_index")

    style = base_style_map.get(ds_name, {"marker":"o","linestyle":"-","base_label":ds_name})
    base_label = style["base_label"]

    # **Hard-code** for hash structures: "X=4"
    if ds_name in ["hash_skip_list", "hash_linked_list"]:
        label = base_label + " X=4"
    else:
        label = base_label

    ax1.plot(
        sub_sorted["x_index"],
        sub_sorted["mean_mb"],
        marker=style["marker"],
        linestyle=style["linestyle"],
        label=label
    )

ax1.axhline(16, color="brown", linewidth=2, label="buffer size")
ax1.set_xticks(range(len(unique_buckets)))
ax1.set_xticklabels(bucket_labels)
ax1.set_xlabel("Bucket Count")
ax1.set_ylabel("Mean Capacity of Buffer(MB)")
ax1.set_title("")
ax1.set_ylim(bottom=0)
ax1.legend(loc="best")

plt.tight_layout()
plt.savefig("new_buffer_16mb_pl_bc__hash_label.png", dpi=300)
plt.show()

print("Donee")
