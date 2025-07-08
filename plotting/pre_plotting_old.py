import os
import re
import pandas as pd
import matplotlib.pyplot as plt


crawl_dir = os.path.join("..", ".result", "new_metadata_overhead")

table_entries = []
flush_entries = []
log_entries = []

header_pattern = re.compile(
    r'cmpt_sty\s+cmpt_pri\s+T\s+P\s+B\s+E\s+M\s+file_size\s+L1_size\s+blk_cch\s+BPK'
)
data_pattern = re.compile(
    r'\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)'
)
flush_pattern = re.compile(
    r'buffer is full, flush finished info \[num_entries\]:\s*(\d+).*raw_key_size:\s*(\d+),\s*raw_value_size:\s*(\d+)',
    re.IGNORECASE
)
total_data_size_pattern = re.compile(r'"total_data_size":\s*(\d+)')

for root, dirs, files in os.walk(crawl_dir):
    for file in files:
        filepath = os.path.join(root, file)
        if file == "workload.log":
            with open(filepath, 'r') as f:
                lines = f.readlines()
            for i, line in enumerate(lines):
                if header_pattern.search(line):
                    j = i + 1
                    while j < len(lines) and not lines[j].strip():
                        j += 1
                    if j < len(lines):
                        data_line = lines[j].strip()
                        m_data = data_pattern.match(data_line)
                        if m_data:
                            values = list(map(int, m_data.groups()))
                            keys = ["cmpt_sty", "cmpt_pri", "T", "P", "B", "E", "M",
                                    "file_size", "L1_size", "blk_cch", "BPK"]
                            entry = dict(zip(keys, values))
                            entry["source_file"] = filepath
                            table_entries.append(entry)
                m_flush = flush_pattern.search(line)
                if m_flush:
                    num_entries, raw_key_size, raw_value_size = map(int, m_flush.groups())
                    flush_entry = {
                        "num_entries": num_entries,
                        "raw_key_size": raw_key_size,
                        "raw_value_size": raw_value_size,
                        "source_file": filepath
                    }
                    flush_entries.append(flush_entry)
        elif file == "LOG":
            with open(filepath, 'r') as f:
                content = f.read()
            m_total = total_data_size_pattern.search(content)
            if m_total:
                total_data_size = int(m_total.group(1))
                log_entry = {
                    "total_data_size": total_data_size,
                    "source_file": filepath
                }
                log_entries.append(log_entry)

df_table = pd.DataFrame(table_entries)
df_flush = pd.DataFrame(flush_entries)
df_log = pd.DataFrame(log_entries)

os.makedirs("extracted_data", exist_ok=True)
df_table.to_csv("extracted_data/table_data.csv", index=False)
df_flush.to_csv("extracted_data/flush_info.csv", index=False)
df_log.to_csv("extracted_data/log_data.csv", index=False)

# ------------------- Step 3+4+5: Merge and Plot -------------------

buffer_size_mb = 8
debug_filename = "metadata_overhead_debug.log"

excluded_experiments = []

valid_entry_sizes = [8, 16, 32, 64, 128, 256, 512, 1024]

flush_info_file = "extracted_data/flush_info.csv"
df_flush2 = pd.read_csv(flush_info_file, header=None, names=[
    "num_entries", "raw_key_size", "raw_value_size", "source_file"
])

log_data_file = "extracted_data/log_data.csv"
df_log2 = pd.read_csv(log_data_file, header=0)

entry_size_pattern = re.compile(r'entry_(\d+)b')

def normalize_source(source_file):
    return os.path.normpath(os.path.dirname(source_file))

def get_top_dir(source_file):
    norm_path = os.path.normpath(source_file)
    parts = norm_path.split(os.sep)
    if 'new_metadata_overhead' in parts:
        idx = parts.index('new_metadata_overhead')
        if idx + 1 < len(parts):
            return parts[idx + 1]
    return "unknown_top_dir"

def get_experiment_type(source_file):
    norm_path = os.path.normpath(source_file)
    parts = norm_path.split(os.sep)
    if 'new_metadata_overhead' in parts:
        idx = parts.index('new_metadata_overhead')
        # next part is top_dir
        if idx+2 < len(parts):
            # the next part after top_dir is the experiment type folder
            return parts[idx+2]
    return "unknown_experiment"

seen_source_dirs = set()
entry_exp_map = {}

with open(debug_filename, "w") as debug_file:
    debug_file.write("=== Metadata Overhead Debug Log ===\n\n")
    debug_file.write("Parsing flush_info.csv for entry_size and experiment_type...\n")

    for idx, row in df_flush2.iterrows():
        source_file = row['source_file']
        if source_file.strip().lower() == "source_file":
            continue
        norm_source = normalize_source(source_file)
        if norm_source in seen_source_dirs:
            continue
        seen_source_dirs.add(norm_source)
        match_entry = entry_size_pattern.search(source_file)
        if match_entry:
            entry_size = int(match_entry.group(1))
        else:
            entry_size = 0
        experiment_type = get_experiment_type(source_file)
        top_dir = get_top_dir(source_file)
        debug_file.write(
            f"flush_info Row {idx}:\n"
            f"  source_file={source_file}\n"
            f"  norm_source={norm_source}\n"
            f"  top_dir={top_dir}\n"
            f"  experiment_type={experiment_type}\n"
            f"  entry_size={entry_size}\n\n"
        )
        entry_exp_map[norm_source] = (entry_size, experiment_type, top_dir)

combined_data = []
with open(debug_filename, "a") as debug_file:
    debug_file.write("Merging log_data.csv with flush_info mapping...\n")
    for idx, row in df_log2.iterrows():
        source_file = row['source_file']
        total_data_size = int(row['total_data_size'])
        norm_source = normalize_source(source_file)
        if norm_source not in entry_exp_map:
            debug_file.write(f"Skipping log_data row {idx}: no flush_info mapping for {norm_source}\n")
            continue
        entry_size, experiment_type, top_dir = entry_exp_map[norm_source]
        metadata_overhead = buffer_size_mb * 1024 * 1024 - total_data_size
        debug_file.write(
            f"log_data Row {idx}:\n"
            f"  source_file={source_file}\n"
            f"  norm_source={norm_source}\n"
            f"  top_dir={top_dir}\n"
            f"  total_data_size={total_data_size}\n"
            f"  entry_size={entry_size}\n"
            f"  experiment_type={experiment_type}\n"
            f"  metadata_overhead={metadata_overhead}\n\n"
        )
        combined_data.append({
            "source_file": source_file,
            "top_dir": top_dir,
            "experiment_type": experiment_type,
            "entry_size": entry_size,
            "total_data_size": total_data_size,
            "metadata_overhead": metadata_overhead
        })

df_combined = pd.DataFrame(combined_data, columns=[
    "source_file", "top_dir", "experiment_type", "entry_size",
    "total_data_size", "metadata_overhead"
])

df_combined = df_combined[df_combined["entry_size"].isin(valid_entry_sizes)]
if excluded_experiments:
    df_combined = df_combined[~df_combined["experiment_type"].isin(excluded_experiments)]

df_combined["entry_size_index"] = df_combined["entry_size"].map(
    {size: idx for idx, size in enumerate(valid_entry_sizes)}
)

with open(debug_filename, "a") as debug_file:
    debug_file.write("\n=== Final DataFrame (after merging & filtering) ===\n")
    df_combined_str = df_combined[[
        "top_dir", "experiment_type", "entry_size",
        "entry_size_index", "total_data_size", "metadata_overhead", "source_file"
    ]].sort_values(["top_dir", "experiment_type", "entry_size"]).to_string(index=False)
    debug_file.write(df_combined_str + "\n")

unique_top_dirs = df_combined["top_dir"].unique()
for tdir in unique_top_dirs:
    sub_df = df_combined[df_combined["top_dir"] == tdir]
    plt.figure(figsize=(10, 6))
    grouped = sub_df.groupby("experiment_type")
    for exp_type, group in grouped:
        group_sorted = group.sort_values("entry_size_index")
        plt.plot(group_sorted["entry_size_index"],
                 group_sorted["metadata_overhead"],
                 marker='o', label=exp_type)
    plt.xlabel("Entry Size (bytes)")
    plt.ylabel("Metadata Overhead (bytes)")
    plt.title(f"Metadata Overhead vs Entry Size\n(top_dir = {tdir})")
    plt.legend(title="Experiment Type")
    plt.grid(False)
    plt.xticks(range(len(valid_entry_sizes)),
               [str(s) for s in valid_entry_sizes])
    plt.ylim(bottom=0)
    plt.tight_layout()
    plt.savefig(f"metadata_overhead_plot_{tdir}.png")
    plt.show()

print("Data extracted and plots generated. Check extracted_data/ for CSVs and .png files.")
print(f"Debug info in '{debug_filename}'.")
