
from pathlib import Path
import re
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import math
import os

try:
    from plot import *
except ImportError:
    pass

DATA_ROOT = Path("/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_compare_vec_variants_get_internal_debug")


EXP_FOLDER = "fixed_pq_vec_variants-lowpri_true-I100000-U0-Q100-S0-Y0-T10-P524288-B512-E8"
FULL_DATA_PATH = DATA_ROOT / EXP_FOLDER


VARIANTS = [
    "Vector-dynamic",
    "AlwayssortedVector-dynamic",
    "UnsortedVector-dynamic"
]


# Map the variant 
VARIANT_LEGEND_MAP = {
    "Vector-dynamic": "Vector",
    "AlwayssortedVector-dynamic": "Always Sorted Vector",
    "UnsortedVector-dynamic": "Unsorted Vector", 
}

.
METRICS_TO_PLOT = [
    "Sorted_Copy",
    # "Sorted_Search",
    # "MemTable::GetFromTable",
    # "MemTable::Get",
    # "MemTable::GetImplSpent",
    # "DBImpl::Get",
    # "DB::Get",
]



# Format: { VariantName: { Abstract_Metric_ID: Actual_Log_Key } }
VARIANT_METRIC_KEY_MAP = {
    "Vector-dynamic": {
        "Sorted_Copy": "Sorted_Copy",
        "Sorted_Search": "Sorted_Search",
    },
    "AlwayssortedVector-dynamic": {
        "Sorted_Copy": "Sorted_Copy",
        "Sorted_Search": "Sorted_Search",
    },
    "UnsortedVector-dynamic": {
        "Sorted_Copy": "Unsorted_Copy",   # Adjusted naming for Unsorted
        "Sorted_Search": "Unsorted_Search", # Adjusted naming for Unsorted
    }
}


METRIC_LEGEND_MAP = {
    "Sorted_Copy": "Snapshot",
    "Sorted_Search": "Search", 
    "MemTable::GetFromTable": "GetFromTable",
    "MemTable::Get": "Get",
    "MemTable::GetImplSpent": "Impl Spent",
    "DBImpl::Get": "DBImpl Get",
    "DB::Get": "DB Get",
}


COLORS = [
    '#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', 
    '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', 
    '#bcbd22', '#17becf'
] 


LINE_STYLES = [
    '-',                # Solid
    '--',               # Dashed
    '-.',               # Dash-dot
    ':',                # Dotted
    (0, (3, 1, 1, 1)),  # Densely dash-dotted
    (0, (5, 1)),        # Densely dashed
    (0, (1, 1)),        # Densely dotted
    (0, (3, 5, 1, 5)),  # Dash dot (loose)
]

FIGSIZE = (5, 3.6)


CURR_DIR = Path(__file__).resolve().parent
PLOTS_DIR = CURR_DIR / "paper_plot" / "vec_get_debug_fixed"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)


def get_log_key(variant, metric_id):
    """Resolves the actual log file key for a given variant and metric ID."""
    if variant in VARIANT_METRIC_KEY_MAP and metric_id in VARIANT_METRIC_KEY_MAP[variant]:
        return VARIANT_METRIC_KEY_MAP[variant][metric_id]
    return metric_id

def parse_log_file(filepath: Path, metrics_ids, variant_name):
    """
    Parses the log file and returns a dictionary of lists for the requested metrics.
    """
    data = {m_id: [] for m_id in metrics_ids}
    

    log_key_to_id = {}
    for m_id in metrics_ids:
        actual_key = get_log_key(variant_name, m_id)
        log_key_to_id[actual_key] = m_id
    
    if not filepath.exists():
        return None

    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            
            clean_line = line.rstrip('/')
            parts = clean_line.split(',')
            

            current_row_values = {}
            for part in parts:
                if ':' in part:
                    key, val = part.split(':', 1)
                    key = key.strip()
                    try:
                        current_row_values[key] = float(val.strip())
                    except ValueError:
                        continue
            

            if not any(actual_key in current_row_values for actual_key in log_key_to_id):
                continue

  
            for actual_key, m_id in log_key_to_id.items():
                if actual_key in current_row_values:
                    data[m_id].append(current_row_values[actual_key])
                else:
                    data[m_id].append(0)
                    
    return data

def save_individual_legend(handle, label, base_output_path):

    legend_fig = plt.figure(figsize=(4, 0.2))
    legend_fig.legend(
        [handle],
        [label],
        loc="center",
        frameon=False,
        borderaxespad=0,
        labelspacing=0,
        borderpad=0,
        columnspacing=0.4,
        handletextpad=0.2,
        handlelength=1,
        handleheight=1,
    )
    plt.axis("off")

    legend_output_path = base_output_path.with_suffix(".pdf")
    legend_fig.savefig(legend_output_path, bbox_inches="tight", pad_inches=0)
    plt.close(legend_fig)

def save_plot_caption(caption_text, base_output_path):
 
    if not caption_text:
        return
    title_fig = plt.figure(figsize=(FIGSIZE[0], 0.5))
    title_fig.text(
        0.5,
        0.5,
        caption_text,
        ha="center",
        va="center",
        fontsize=plt.rcParams.get("font.size", 10),
    )
    plt.axis("off")

    caption_output_path = base_output_path.with_name(
        f"{base_output_path.name}_caption.pdf"
    )
    title_fig.savefig(caption_output_path, bbox_inches="tight", pad_inches=0.02)
    plt.close(title_fig)

def main():
    fig, ax = plt.subplots(figsize=FIGSIZE)
    # Adjust margins to match style convention
    fig.subplots_adjust(left=0.18, right=0.98, top=0.98, bottom=0.15)
    
    all_max_y = []
    
    if not FULL_DATA_PATH.exists():
         print(f"Error: Experiment folder not found: {FULL_DATA_PATH}")
         return

   
    for v_idx, variant in enumerate(VARIANTS):
        filepath = FULL_DATA_PATH / variant / "run1.log"
        print(f"Processing {variant} from: {filepath}")
        

        variant_data = parse_log_file(filepath, METRICS_TO_PLOT, variant)
        
        if not variant_data:
            print(f"Warning: File not found at {filepath}")
            continue

        for m_idx, metric in enumerate(METRICS_TO_PLOT):
            y_values = variant_data[metric]
            if not y_values:
                continue
            
      
            if all(v == 0 for v in y_values) and len(y_values) > 0:
                print(f"  -> Metric '{metric}' (mapped to '{get_log_key(variant, metric)}') found but all zeros.")

            x_values = np.arange(1, len(y_values) + 1)
            
  
            all_max_y.append(np.max(y_values))
            
   
            color = COLORS[v_idx % len(COLORS)]
            linestyle = LINE_STYLES[m_idx % len(LINE_STYLES)]
            
    
            display_variant = VARIANT_LEGEND_MAP.get(variant, variant.replace("-dynamic", ""))
            display_metric = METRIC_LEGEND_MAP.get(metric, metric)
            
            label = f"{display_variant} - {display_metric}"
            
            (line,) = ax.plot(x_values, y_values, label=label, color=color, linestyle=linestyle, alpha=0.8)

       
            safe_label = re.sub(r"[\s\(\)/:\.]+", "_", label).strip("_")
            legend_name = f"legend_{safe_label}"
            legend_out_path = PLOTS_DIR / legend_name
            save_individual_legend(line, label, legend_out_path)

  
    ax.set_xlabel('number of point queries')
    ax.set_ylabel('latency', labelpad=0)
    

    if all_max_y:
        effective_max = np.max(all_max_y)
    else:
        effective_max = 1

    if effective_max > 999:
        y_power = math.floor(math.log10(effective_max)) if effective_max > 0 else 0
        ax.yaxis.set_major_formatter(
            ticker.FuncFormatter(lambda y, p: f"{y / (10**y_power):.1f}")
        )
        ax.text(
            0.05,
            0.98,
            r"$\times 10^{{{}}}$".format(y_power),
            transform=ax.transAxes,
            fontsize=plt.rcParams.get("font.size", 10),
            ha="left",
            va="top",
        )


    base_out = PLOTS_DIR / "internal_latency_comparison"
    caption_text = "Internal Function Latency per Query"

    if all_max_y:
        output_path = base_out.with_suffix(".pdf")
        fig.savefig(output_path, bbox_inches="tight", pad_inches=0.02)
        print(f"[saved] {output_path}")
        save_plot_caption(caption_text, base_out)
    else:
        print("No data plotted.")

    plt.close(fig)

if __name__ == "__main__":
    main()