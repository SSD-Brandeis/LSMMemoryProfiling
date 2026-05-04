#!/usr/bin/env python3
import os
import json
import matplotlib.pyplot as plt
import re

# Base directory for experiments
base_dir = ".vstats/experiments-concurrency-I1000000-PQ0-RQ0"
# Automatically detect thread directories
thread_dirs = []
for entry in os.listdir(base_dir):
    match = re.match(r'threads_(\d+)$', entry)
    if match and os.path.isdir(os.path.join(base_dir, entry)):
        thread_dirs.append((int(match.group(1)), entry))
if not thread_dirs:
    raise RuntimeError(f"No threads_* directories found under {base_dir}")
thread_dirs.sort()
threads = [num for num, _ in thread_dirs]

# Memtable types
memtables = ["skiplist", "vector-preallocated", "unsortedvector-preallocated", "sortedvector-preallocated"]

# Dictionary to hold throughput data: memtable -> list of throughputs for each thread count
throughput_data = {mt: [] for mt in memtables}

# Function to extract workload execution time from workload.log
def extract_workload_time_ns(log_path):
    try:
        with open(log_path, 'r') as f:
            content = f.read()
            match = re.search(r'Workload Execution Time:\s*(\d+)', content)
            if match:
                return int(match.group(1))
    except FileNotFoundError:
        pass
    return None

def load_workload_op_count(spec_path):
    try:
        with open(spec_path, 'r') as f:
            spec = json.load(f)
    except FileNotFoundError:
        return None

    op_count = 0
    for section in spec.get('sections', []):
        for group in section.get('groups', []):
            for op_name, op_data in group.items():
                if isinstance(op_data, dict) and 'op_count' in op_data:
                    op_count += float(op_data['op_count'])
    return int(op_count) if op_count else None

base_spec_path = os.path.join(base_dir, 'workload.specs.json')
workload_op_count = load_workload_op_count(base_spec_path)
if workload_op_count is None:
    raise RuntimeError(f'Unable to load op_count from {base_spec_path}')

# Collect data
import math

for _, thread_dir in thread_dirs:
    for mt in memtables:
        log_path = os.path.join(base_dir, thread_dir, mt, "workload.log")
        workload_time_ns = extract_workload_time_ns(log_path)
        if workload_time_ns is not None and workload_time_ns > 0:
            throughput = workload_op_count / (workload_time_ns / 1e9)
            throughput_data[mt].append(throughput)
        else:
            print(f"Warning: Could not compute throughput for {mt} in {thread_dir}")
            throughput_data[mt].append(math.nan)

# Plot using categorical thread labels so spacing is equal
x_positions = list(range(len(threads)))
plt.figure(figsize=(10, 6))
for mt in memtables:
    if all(math.isnan(v) for v in throughput_data[mt]):
        print(f"Skipping {mt}: no valid throughput data")
        continue
    plt.plot(x_positions, throughput_data[mt], marker='o', label=mt)

plt.xlabel('Number of Threads')
plt.ylabel('Throughput (ops/sec)')
plt.title('Concurrency Experiment: Throughput vs Threads')
plt.legend()
plt.grid(True)
plt.xticks(x_positions, threads)
plt.tight_layout()

# Save the plot
plt.savefig('concurrency_throughput_plot.png')
print("Plot saved to concurrency_throughput_plot.png")
