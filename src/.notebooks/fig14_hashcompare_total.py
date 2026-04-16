import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import pandas as pd
from matplotlib.ticker import LogFormatterMathtext
from matplotlib import font_manager
from pathlib import Path
import re
import numpy as np
import sys
import os

# --- Configuration ---
SCRIPT_NAME = Path(__file__).stem
try:
    CURR_DIR = Path(__file__).resolve().parent
except NameError:
    CURR_DIR = Path.cwd()

OUTPUT_DIR = CURR_DIR / SCRIPT_NAME
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

class Logger(object):
    def __init__(self, filename):
        self.terminal = sys.stdout
        self.log = open(OUTPUT_DIR / filename, "w")

    def write(self, message):
        self.terminal.write(message)
        self.log.write(message)

    def flush(self):
        self.terminal.flush()
        self.log.flush()

ROOT = CURR_DIR.parent

# --- Style & plot.py Integration ---
bar_styles = {}
try:
    from plot import *
    print("✅ Successfully imported styles from plot.py")
except ImportError:
    print("Warning: plot.py not found. Using default styles.")

# --- Data Paths ---
DATA_ROOT = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_Mar17_fig14")
TARGET_FOLDERS = ["hashhybrid_newsetting_fig14_316-I740000-U0-PQ500-RQ0-S0-lowpri_true-I740000-P32768"]

WRITE_RE = re.compile(r"rocksdb\.db\.write\.micros.*?Sum\s*[:\s]\s*(\d+)", re.IGNORECASE)
GET_RE = re.compile(r"rocksdb\.db\.get\.micros.*?Sum\s*[:\s]\s*(\d+)", re.IGNORECASE)

def parse_log_file(text):
    insert_micros, get_micros = 0, 0
    m_write = WRITE_RE.search(text)
    if m_write: insert_micros = int(m_write.group(1))
    m_get = GET_RE.search(text)
    if m_get: get_micros = int(m_get.group(1))
    return insert_micros, get_micros

def load_data():
    records = []
    for folder_name in TARGET_FOLDERS:
        target_dir = DATA_ROOT / folder_name
        if not target_dir.exists(): continue
        
        for log_path in target_dir.rglob("workload.log"):
            dir_name = log_path.parent.name
            if "skiplist" in dir_name:
                base_name = "skiplist"
                h_val = "N/A"
            else:
                base_match = re.search(r"(hash_linked_list|hash_skip_list|hash_vector)", dir_name)
                h_match = re.search(r"-H(\d+)", dir_name)
                if base_match and h_match:
                    base_name = base_match.group(1)
                    h_val = h_match.group(1)
                else:
                    continue

            ins_micros, pq_micros = parse_log_file(log_path.read_text())
            records.append({
                "base_buffer": base_name,
                "h_val": h_val,
                "total_ins_s": ins_micros * 1e-6,
                "total_pq_s": pq_micros * 1e-6,
                "total_workload_s": (ins_micros + pq_micros) * 1e-6
            })
    if not records: return pd.DataFrame()
    return pd.DataFrame(records)

def generate_metrics_table():
    """Aggregates data and prints a table with 5 points (H1, H2, H5, H10, H100k) horizontally."""
    df = load_data()
    if df.empty:
        print("No data found. Check workload.log files.")
        return

    # Normalize H values for sorting
    h_order = ["1", "2", "5", "10", "100000"]
    h_display = ["H1", "H2", "H5", "H10", "H100k"]
    
    # Rows in specified order
    buffer_order = ["skiplist", "hash_linked_list", "hash_skip_list", "hash_vector"]
    metrics = ["total_ins_s", "total_pq_s", "total_workload_s"]
    metric_names = ["Insert Latency (s)", "PQ Latency (s)", "Total Time (s)"]

    print("\n" + "="*120)
    print("HASH COMPARISON METRICS TABLE (FOR OVERLEAF)")
    print("="*120)
    
    header = f"{'Buffer':<20}"
    for name in metric_names:
        header += f" | {name} ({', '.join(h_display)})"
    print(header)
    print("-" * len(header))

    for base in buffer_order:
        row_df = df[df["base_buffer"] == base]
        if row_df.empty: continue
        
        row_str = f"{base:<20}"
        
        for m in metrics:
            vals = []
            if base == "skiplist":
                # For skiplist, repeat the same value 5 times as it has no H variant
                val = row_df[m].mean()
                vals = [f"{val:.3f}"] * 5
            else:
                for h in h_order:
                    h_data = row_df[row_df["h_val"] == h]
                    if not h_data.empty:
                        vals.append(f"{h_data[m].mean():.3f}")
                    else:
                        vals.append("N/A")
            
            row_str += f" | {'  '.join(vals)}"
        print(row_str)

    print("-" * len(header))
    print("="*120 + "\n")

def main():
    log_filename = f"{SCRIPT_NAME}.log"
    sys.stdout = Logger(log_filename)
    
    generate_metrics_table()
    
    full_log_path = (OUTPUT_DIR / log_filename).resolve()
    sys.stdout.terminal.write(f"\n[TABLE GENERATED] Full log path: file://{full_log_path}\n")

if __name__ == "__main__":
    main()