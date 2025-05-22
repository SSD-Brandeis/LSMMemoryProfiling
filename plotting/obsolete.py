import os
import re
import pandas as pd
import matplotlib.pyplot as plt


extracted_data_dir = "./extracted_data"


buffer_size_mb = 8


debug_filename = "metadata_overhead_debug.log"

excluded_experiments = [
    # "skiplist",
    # "vector",
    # "vector_8kb_page",
]


valid_entry_sizes = [8, 16, 32, 64, 128, 256, 512, 1024]


flush_info_file = os.path.join(extracted_data_dir, "flush_info.csv")
df_flush = pd.read_csv(flush_info_file, header=None, names=[
    "num_entries", "raw_key_size", "raw_value_size", "source_file"
])


log_data_file = os.path.join(extracted_data_dir, "log_data.csv")
df_log = pd.read_csv(log_data_file, header=0)


entry_size_pattern = re.compile(r'entry_(\d+)b')


prefix = os.path.join("..", ".result", "new_metadata_overhead", "lambda_0.5_bucket_100_4kb_page") + os.sep

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

def normalize_source(source_file):

    return os.path.normpath(os.path.dirname(source_file))


seen_source_dirs = set()
entry_exp_map = {}  

with open(debug_filename, "w") as debug_file:
    debug_file.write("=== Metadata Overhead Debug Log ===\n\n")
    debug_file.write("Parsing flush_info.csv for entry_size and experiment_type...\n")

    for idx, row in df_flush.iterrows():
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

        debug_file.write(
            f"flush_info Row {idx}:\n"
            f"  source_file={source_file}\n"
            f"  normalized_source={norm_source}\n"
            f"  experiment_type={experiment_type}\n"
            f"  entry_size={entry_size}\n\n"
        )

        entry_exp_map[norm_source] = (entry_size, experiment_type)


combined_data = []

with open(debug_filename, "a") as debug_file:
    debug_file.write("Merging log_data.csv with flush_info mapping...\n")

    for idx, row in df_log.iterrows():
        source_file = row['source_file']
        total_data_size = int(row['total_data_size'])  

     
        norm_source = normalize_source(source_file)

 
        if norm_source not in entry_exp_map:
            debug_file.write(f"Skipping log_data row {idx}: no flush_info mapping for normalized source {norm_source}\n")
            continue

        entry_size, experiment_type = entry_exp_map[norm_source]

       
        metadata_overhead = buffer_size_mb * 1024 * 1024 - total_data_size

        debug_file.write(
            f"log_data Row {idx}:\n"
            f"  source_file={source_file}\n"
            f"  normalized_source={norm_source}\n"
            f"  total_data_size={total_data_size}\n"
            f"  entry_size={entry_size}\n"
            f"  experiment_type={experiment_type}\n"
            f"  metadata_overhead={metadata_overhead}\n\n"
        )

        combined_data.append({
            "source_file": source_file,
            "experiment_type": experiment_type,
            "entry_size": entry_size,
            "total_data_size": total_data_size,
            "metadata_overhead": metadata_overhead
        })


df_combined = pd.DataFrame(combined_data, columns=["source_file", "experiment_type", "entry_size", "total_data_size", "metadata_overhead"])


df_combined = df_combined[df_combined["entry_size"].isin(valid_entry_sizes)]


if excluded_experiments:
    df_combined = df_combined[~df_combined["experiment_type"].isin(excluded_experiments)]


index_map = {size: idx for idx, size in enumerate(valid_entry_sizes)}
df_combined["entry_size_index"] = df_combined["entry_size"].map(index_map)

with open(debug_filename, "a") as debug_file:
    debug_file.write("\n=== Final DataFrame (after merging & filtering) ===\n")
    df_combined_str = (
        df_combined[["experiment_type", "entry_size", "entry_size_index", "total_data_size", "metadata_overhead", "source_file"]]
        .sort_values(["experiment_type", "entry_size"])
        .to_string(index=False)
    )
    debug_file.write(df_combined_str + "\n")



plt.figure(figsize=(10, 6))


grouped = df_combined.groupby("experiment_type")

for exp_type, group in grouped:
    group_sorted = group.sort_values("entry_size_index")
    plt.plot(
        group_sorted["entry_size_index"],
        group_sorted["metadata_overhead"],
        marker='o',
        label=exp_type
    )

plt.xlabel("Entry Size (bytes)")
plt.ylabel("Metadata Overhead (bytes)")
plt.title("Metadata Overhead vs Entry Size")
plt.legend(title="Experiment Type")
plt.grid(False)
plt.xticks(
    range(len(valid_entry_sizes)),
    [str(size) for size in valid_entry_sizes]
)
plt.ylim(bottom=0)
plt.tight_layout()
plt.savefig("metadata_overhead_plot.png")
plt.show()

print(f"Debug info has been written to '{debug_filename}'.")
