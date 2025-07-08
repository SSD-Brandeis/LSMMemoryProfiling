#!/usr/bin/env python3
"""
Parse new-metadata-overhead results, extract CSVs, and plot metadata-overhead
versus entry-size for each algorithm.
Directory layout expected (2025-07):
.result/new_metadata_overhead/<algo>/<P4096_E###_I…>/P_16384/{workload.log,LOG_run1}
"""

import os
import re
import pandas as pd
import matplotlib.pyplot as plt

# -----------------------------------------------------------------------------#
# 1.   PATHS & CONSTANTS
# -----------------------------------------------------------------------------#
crawl_dir = os.path.join("..", ".result", "new_metadata_overhead")
os.makedirs("extracted_data", exist_ok=True)

buffer_size_mb       = 8                       # RocksDB write-buffer size
valid_entry_sizes    = [8, 16, 32, 64, 128, 256, 512, 1024]
excluded_experiments = []                      # add names to hide from plots
debug_filename       = "metadata_overhead_debug.log"

# -----------------------------------------------------------------------------#
# 2.   REGEX PATTERNS
# -----------------------------------------------------------------------------#
header_pattern        = re.compile(r'cmpt_sty\s+cmpt_pri\s+T\s+P\s+B\s+E')
data_pattern          = re.compile(r'\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+'
                                   r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)')
flush_pattern         = re.compile(
    r'buffer is full, flush finished info \[num_entries\]:\s*(\d+).*?'
    r'raw_key_size:\s*(\d+),\s*raw_value_size:\s*(\d+)', re.IGNORECASE)
total_data_size_patt  = re.compile(r'"total_data_size":\s*(\d+)')
entry_size_pattern    = re.compile(r'_E(\d+)_')        #  <<< NEW  (captures E16 → 16)

# -----------------------------------------------------------------------------#
# 3.   HELPERS (updated for new layout)
# -----------------------------------------------------------------------------#
def normalize_source(source_file: str) -> str:
    """
    Return canonical directory one level *above* P_16384, i.e.
    …/<algo>/<P4096_E###_I…>
    """
    return os.path.normpath(os.path.join(os.path.dirname(source_file), "..", ".."))

def get_top_dir(source_file: str) -> str:
    """Algorithm folder name right after new_metadata_overhead"""
    parts = os.path.normpath(source_file).split(os.sep)
    if 'new_metadata_overhead' in parts:
        idx = parts.index('new_metadata_overhead')
        if idx + 1 < len(parts):
            return parts[idx + 1]           # e.g., 'skiplist'
    return "unknown_top_dir"

def get_experiment_type(source_file: str) -> str:
    """Same as algorithm folder (may be used for styling)"""
    return get_top_dir(source_file)

# -----------------------------------------------------------------------------#
# 4.   CRAWL FILE SYSTEM & COLLECT RAW ROWS
# -----------------------------------------------------------------------------#
table_entries, flush_entries, log_entries = [], [], []

for root, _, files in os.walk(crawl_dir):
    for fname in files:
        fpath = os.path.join(root, fname)

        # ---- workload.log ----------------------------------------------------#
        if fname == "workload.log":
            with open(fpath) as f:
                lines = f.readlines()
            for i, line in enumerate(lines):
                if header_pattern.search(line):
                    # next non-blank line holds values
                    j = i + 1
                    while j < len(lines) and not lines[j].strip():
                        j += 1
                    if j < len(lines):
                        m = data_pattern.match(lines[j].strip())
                        if m:
                            keys   = ["cmpt_sty","cmpt_pri","T","P","B","E","M",
                                      "file_size","L1_size","blk_cch","BPK"]
                            values = list(map(int, m.groups()))
                            table_entries.append(dict(zip(keys, values),
                                                      source_file=fpath))
                # flush lines
                m_flush = flush_pattern.search(line)
                if m_flush:
                    n, rk, rv = map(int, m_flush.groups())
                    flush_entries.append(dict(num_entries=n,
                                              raw_key_size=rk,
                                              raw_value_size=rv,
                                              source_file=fpath))

        # ---- LOG_run1 --------------------------------------------------------#
        elif fname == "LOG_run1":
            with open(fpath) as f:
                content = f.read()
            m_tot = total_data_size_patt.search(content)
            if m_tot:
                log_entries.append(dict(total_data_size=int(m_tot.group(1)),
                                        source_file=fpath))

# -----------------------------------------------------------------------------#
# 5.   SAVE RAW CSVs (unchanged)
# -----------------------------------------------------------------------------#
pd.DataFrame(table_entries).to_csv("extracted_data/table_data.csv", index=False)
pd.DataFrame(flush_entries).to_csv("extracted_data/flush_info.csv", index=False)
pd.DataFrame(log_entries)  .to_csv("extracted_data/log_data.csv",  index=False)

# -----------------------------------------------------------------------------#
# 6.   MERGE flush_info  ✕  log_data  →  df_combined
# -----------------------------------------------------------------------------#
df_flush = pd.read_csv("extracted_data/flush_info.csv", header=None,
                       names=["num_entries","raw_key_size","raw_value_size","source_file"])
df_log   = pd.read_csv("extracted_data/log_data.csv")

entry_exp_map, seen_dirs = {}, set()

with open(debug_filename, "w") as dbg:
    dbg.write("=== Metadata Overhead Debug Log ===\n\n")
    dbg.write("Scanning flush_info …\n")
    for idx, row in df_flush.iterrows():
        src  = row.source_file
        nsrc = normalize_source(src)
        if nsrc in seen_dirs:          # one map per entry-size folder
            continue
        seen_dirs.add(nsrc)

        m_ent = entry_size_pattern.search(src)
        entry_sz = int(m_ent.group(1)) if m_ent else 0

        algo = get_top_dir(src)
        exp  = get_experiment_type(src)

        dbg.write(f"[flush] {idx}: nsrc={nsrc}, algo={algo}, entry={entry_sz}\n")
        entry_exp_map[nsrc] = (entry_sz, exp, algo)

# -- merge with LOG data -------------------------------------------------------#
combined = []
with open(debug_filename, "a") as dbg:
    dbg.write("\nMerging log_data …\n")
    for idx, row in df_log.iterrows():
        src  = row.source_file
        nsrc = normalize_source(src)
        if nsrc not in entry_exp_map:
            dbg.write(f"[skip] log row {idx}: no flush match for {nsrc}\n")
            continue

        entry_sz, exp, algo = entry_exp_map[nsrc]
        md_over = buffer_size_mb*1024*1024 - int(row.total_data_size)

        dbg.write(f"[ok]   log row {idx}: entry={entry_sz}, algo={algo}, md={md_over}\n")
        combined.append(dict(source_file=src, top_dir=algo, experiment_type=exp,
                             entry_size=entry_sz, total_data_size=row.total_data_size,
                             metadata_overhead=md_over))

df_combined = pd.DataFrame(combined)
df_combined = df_combined[df_combined.entry_size.isin(valid_entry_sizes)]
if excluded_experiments:
    df_combined = df_combined[~df_combined.experiment_type.isin(excluded_experiments)]
df_combined["entry_size_index"] = df_combined.entry_size.map(
    {sz:i for i, sz in enumerate(valid_entry_sizes)})

with open(debug_filename, "a") as dbg:
    dbg.write("\nFinal combined dataframe:\n")
    dbg.write(df_combined.to_string(index=False) + "\n")

# -----------------------------------------------------------------------------#
# 7.   PLOT
# -----------------------------------------------------------------------------#
style_map = {
    "skiplist":        dict(color="blue",   marker="o", linestyle="-"),
    "vector":          dict(color="green",  marker="s", linestyle="--"),
    "hash_skip_list":  dict(color="red",    marker="^", linestyle="-."),
    "hash_linked_list":dict(color="purple", marker="D", linestyle=":"),
    "linklist":        dict(color="orange", marker="v", linestyle=(0,(3,1,1,1)))
}

print("Unique top_dirs:", df_combined.top_dir.unique())

for algo in df_combined.top_dir.unique():
    sub = df_combined[df_combined.top_dir == algo]
    plt.figure(figsize=(10,6))

    for exp, grp in sub.groupby("experiment_type"):
        grp_sorted = grp.sort_values("entry_size_index")
        style = style_map.get(exp, dict(color="black", marker="o", linestyle="-"))
        plt.plot(grp_sorted.entry_size_index,
                 grp_sorted.metadata_overhead/1048576,
                 label=exp, **style)

    plt.xlabel("Entry Size (bytes)")
    plt.ylabel("Metadata Overhead (MB)")
    plt.title(f"Metadata Overhead vs Entry Size\n({algo})")
    plt.legend(title="Experiment Type")
    plt.xticks(range(len(valid_entry_sizes)), [str(sz) for sz in valid_entry_sizes])
    plt.ylim(bottom=0)
    plt.tight_layout()
    out_png = f"extracted_data/metadata_overhead_plot_{algo}.png"
    plt.savefig(out_png)
    plt.show()
    print(f"[saved] {out_png}")

print("\nData extracted and plots generated.  See extracted_data/ for CSVs & PNGs.")
print(f"Debug details in {debug_filename}")
