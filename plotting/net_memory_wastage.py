import os
import re
import pandas as pd
import matplotlib.pyplot as plt



extracted_data_dir = "./extracted_data"
buffer_size_mb = 8
debug_filename = "memory_wastage_debug.log"

#  ["skiplist", "vector_8kb_page", "hash_skip_list_bucket_100", ...]
excluded_experiments = [
    # "vector"
    # "skiplist", 
    # "vector_8kb_page",
    # "hash_skip_list_bucket_100",
]



flush_info_file = os.path.join(extracted_data_dir, "flush_info.csv")
df_flush = pd.read_csv(flush_info_file)

entry_size_pattern = re.compile(r'entry_(\d+)b')
prefix = os.path.join("..", ".result", "overhead_table") + os.sep

def get_experiment_type(source_file_path):

    if source_file_path.startswith(prefix):
        remainder = source_file_path[len(prefix):]
    else:
        remainder = source_file_path

    parts = remainder.split(os.sep)
    if not parts:
        return "unknown"

    experiment_folder = parts[0]
    if experiment_folder.endswith("_8mb_buffer"):
        experiment_folder = experiment_folder.replace("_8mb_buffer", "")

    return experiment_folder



seen_source_files = set()
data_list = []

with open(debug_filename, "w") as debug_file:
    debug_file.write("=== Memory Wastage Debug Log ===\n\n")
    
    for idx, row in df_flush.iterrows():
        source_file = row['source_file']

  
        if source_file in seen_source_files:
            continue
        seen_source_files.add(source_file)

        num_entries = row['num_entries']
        

        match_entry = entry_size_pattern.search(source_file)
        if match_entry:
            entry_size = int(match_entry.group(1))
        else:
            entry_size = 0
        
  
        experiment_type = get_experiment_type(source_file)

        overhead_per_entry = entry_size + 8 + 10
        memory_wastage = buffer_size_mb * 1024 * 1024 - overhead_per_entry * num_entries

        debug_file.write(
            f"Row {idx}: source_file={source_file}\n"
            f"  experiment_type={experiment_type}\n"
            f"  entry_size={entry_size}\n"
            f"  num_entries={num_entries}\n"
            f"  memory_wastage={memory_wastage}\n\n"
        )

        data_list.append({
            "experiment_type": experiment_type,
            "entry_size": entry_size,
            "num_entries": num_entries,
            "memory_wastage": memory_wastage,
            "source_file": source_file
        })

df_plot = pd.DataFrame(data_list)


valid_entry_sizes = [8, 16, 32, 64, 128, 256, 512, 1024]
df_plot = df_plot[df_plot["entry_size"].isin(valid_entry_sizes)]


if excluded_experiments:
    df_plot = df_plot[~df_plot["experiment_type"].isin(excluded_experiments)]


index_map = {size: idx for idx, size in enumerate(valid_entry_sizes)}
df_plot["entry_size_index"] = df_plot["entry_size"].map(index_map)



with open(debug_filename, "a") as debug_file:
    debug_file.write("\n=== Final DataFrame (after filtering) ===\n")
    df_plot_str = (
        df_plot[["experiment_type", "entry_size", "entry_size_index", "num_entries", "memory_wastage", "source_file"]]
        .sort_values(["experiment_type", "entry_size"])
        .to_string(index=False)
    )
    debug_file.write(df_plot_str + "\n")


grouped = df_plot.groupby("experiment_type")

plt.figure(figsize=(10, 6))

for exp_type, group in grouped:
    group_sorted = group.sort_values("entry_size_index")
    plt.plot(
        group_sorted["entry_size_index"],
        group_sorted["memory_wastage"],
        marker='o',
        label=exp_type
    )

plt.xlabel("Entry Size (bytes)")
plt.ylabel("Net Memory Wastage (bytes)")
plt.title("Net Memory Wastage vs Entry Size")
plt.legend(title="Experiment Type")
plt.grid(True)


plt.xticks(
    range(len(valid_entry_sizes)),
    [str(size) for size in valid_entry_sizes]
)

plt.tight_layout()
plt.savefig("memory_wastage_plot.png")
plt.show()

print(f"Debug info has been written to '{debug_filename}'.")
