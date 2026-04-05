from pathlib import Path
import sys, re
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager

# Set 'histogram' or 'line' to choose the plot type for all datasets
PLOT_TYPE = 'histogram'
# PLOT_TYPE = 'line'

CURR_DIR = Path(__file__).resolve().parent
ROOT = CURR_DIR.parent if CURR_DIR.parent.name else CURR_DIR # Handle case where script is in root
sys.path.insert(0, str(ROOT))

DATA_ROOT = ROOT / "data"
# Base directory for the new dataset structure
DATA_BASE_DIR = DATA_ROOT / "filter_result_diff_keysize_commonprefix"

# --- Main Configuration for Datasets ---
# Add paths and plot-specific axis limits for each dataset.
# Set a limit to a tuple like (min, max) for manual control,
# or leave it as None for automatic scaling.
DATASET_CONFIGS = [
    {
        "stats_dir": DATA_BASE_DIR / "common_prefix_keysize_128_value_896-insertPQRS_X8H1M_varC-lowpri_false-I480000-U0-Q0-S100-Y0.001-T10-P131072-B4-E1024",
        "plots_dir": DATA_ROOT / "plots" / "plots_keysize_128",
        # Example of manual override for this dataset's histogram
        "hist_keys_xlim": None, # (0, 40)
        "hist_keys_ylim": None, # (0, 700)
        "hist_selectivity_xlim": None,
        "hist_selectivity_ylim": None,
        "line_xlim": None,
        "line_keys_ylim": None,
        "line_selectivity_ylim": (0, 1),
    },
    {
        "stats_dir": DATA_BASE_DIR / "common_prefix_keysize_8_value_1016-insertPQRS_X8H1M_varC-lowpri_false-I480000-U0-Q0-S100-Y0.001-T10-P131072-B4-E1024",
        "plots_dir": DATA_ROOT / "plots" / "plots_keysize_8_val_1016",
        # This dataset will use automatic scaling (None for all limits)
        "hist_keys_xlim": None,
        "hist_keys_ylim": None,
        "hist_selectivity_xlim": None,
        "hist_selectivity_ylim": None,
        "line_xlim": None,
        "line_keys_ylim": None,
        "line_selectivity_ylim": (0, 1),
    },
    {
        "stats_dir": DATA_BASE_DIR / "common_prefix_keysize_8_value_24-insertPQRS_X8H1M_varC-lowpri_false-I6000000-U0-Q0-S100-Y0.001-T10-P131072-B128-E32",
        "plots_dir": DATA_ROOT / "plots" / "plots_keysize_8_val_24",
        "hist_keys_xlim": None,
        "hist_keys_ylim": None,
        "hist_selectivity_xlim": None,
        "hist_selectivity_ylim": None,
        "line_xlim": None,
        "line_keys_ylim": None,
        "line_selectivity_ylim": (0, 1),
    }
]

# --- Font and Style Configuration ---
FONT_CANDIDATES = [
    CURR_DIR / "LinLibertine_Mah.ttf",
    ROOT / "LinLibertine_Mah.ttf",
    Path.home() / "Desktop/tectonic/LinLibertine_Mah.ttf",
]
for fp in FONT_CANDIDATES:
    if fp.exists():
        prop = font_manager.FontProperties(fname=str(fp))
        plt.rcParams["font.family"] = prop.get_name()
        break
plt.rcParams["text.usetex"] = True
plt.rcParams["font.weight"] = "normal"
plt.rcParams["font.size"] = 22

FIGSIZE = (5, 3.6)

PLOT_STYLES = {
    "keys_returned": {
        "color": "tab:blue",
        "marker": "o",
        "markersize": 4,
        "linestyle": "-",
        "label": r"Keys Returned" # Used for the legend
    },
    "selectivity": {
        "color": "tab:red",
        "marker": "x",
        "markersize": 5,
        "linestyle": "--",
        "label": r"Selectivity" # Used for the legend
    }
}

def parse_log_file(file_path: Path):
    """Parses the log file to extract keys_returned and selectivity data."""
    keys_returned = []
    selectivity = []
    text = file_path.read_text(errors="ignore")
    for line in text.splitlines():
        match = re.search(r'keys_returned: (\d+), selectivity: ([-+]?\d*\.?\d+([eE][-+]?\d+)?)', line)
        if match:
            keys_returned.append(int(match.group(1)))
            selectivity.append(float(match.group(2)))
    return keys_returned, selectivity

def fig_ax():
    """Creates a figure and axes with standard adjustments."""
    fig, ax = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.18, right=0.85, top=0.98, bottom=0.35)
    return fig, ax

def legend_bottom(fig, handles):
    """Adds a legend to the bottom of the figure."""
    fig.legend(
        handles=handles,
        loc="lower center",
        ncol=2,
        bbox_to_anchor=(0.5, -0.02),
        frameon=False,
        labelspacing=0.3,
        handletextpad=0.6,
        columnspacing=1.0,
    )

def create_line_plot(keys_data, selectivity_data, out_path: Path,
                     xlabel, y1label, y2label,
                     xlim=None, y1lim=None, y2lim=None):
    """Creates and saves a dual-axis line plot."""
    fig, ax1 = fig_ax()

    style1 = PLOT_STYLES["keys_returned"]
    line1 = ax1.plot(keys_data, **style1)[0]
    ax1.set_ylabel(y1label, color=style1["color"])
    ax1.tick_params(axis='y', labelcolor=style1["color"])
    ax1.set_xlabel(xlabel)

    ax2 = ax1.twinx()
    style2 = PLOT_STYLES["selectivity"]
    line2 = ax2.plot(selectivity_data, **style2)[0]
    ax2.set_ylabel(y2label, color=style2["color"])
    ax2.tick_params(axis='y', labelcolor=style2["color"])

    if xlim is not None:
        ax1.set_xlim(xlim)
    if y1lim is not None:
        ax1.set_ylim(y1lim)
    if y2lim is not None:
        ax2.set_ylim(y2lim)

    legend_bottom(fig, handles=[line1, line2])

    fig.savefig(out_path, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"[saved] {out_path}")

def create_histogram_plot(keys_data, selectivity_data, out_path: Path,
                          x1label, y1label, x2label,
                          x1lim=None, y1lim=None, x2lim=None, y2lim=None):
    """Creates and saves a two-panel histogram plot with improved readability."""
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4.2))

    # --- Plot for Keys Returned ---
    if keys_data:
        max_val = np.max(keys_data)
        # If the max value is small, create precise integer-centered bins for clarity
        if max_val < 40:
            bins = np.arange(max_val + 2) - 0.5
            ax1.hist(keys_data, bins=bins, color="tab:blue", edgecolor='black')
            ax1.set_xticks(np.arange(max_val + 1))
        # If the max value is large, let matplotlib decide the bins automatically to prevent freezing
        else:
            ax1.hist(keys_data, bins='auto', color="tab:blue", edgecolor='black')
    ax1.set_xlabel(x1label)
    ax1.set_ylabel(y1label)
    ax1.set_title("Distribution of Keys Returned")

    # --- Plot for Selectivity ---
    if selectivity_data:
        ax2.hist(selectivity_data, bins=50, color="tab:red", edgecolor='black')
    ax2.set_xlabel(x2label)
    ax2.set_title("Distribution of Selectivity")
    # Improve readability of scientific notation on the x-axis
    ax2.ticklabel_format(style='sci', axis='x', scilimits=(-3,3))

    # --- Apply Axis Limits ---
    if x1lim is not None:
        ax1.set_xlim(x1lim)
    else:
        # Ensure the plot starts neatly at 0 for the integer histogram if using integer bins
        if 'bins' in locals() and isinstance(bins, np.ndarray):
             ax1.set_xlim(left=-0.5)
    
    if y1lim is not None:
        ax1.set_ylim(y1lim)

    if x2lim is not None:
        ax2.set_xlim(x2lim)
        
    if y2lim is not None:
        ax2.set_ylim(y2lim)


    fig.tight_layout(pad=1.0)
    fig.savefig(out_path, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"[saved] {out_path}")

def main():
    """Main function to iterate through datasets and generate plots."""

    # --- Global Plot Labels ---
    hist_keys_xlabel = "Keys Returned"
    hist_keys_ylabel = "Number of RQ"
    hist_selectivity_xlabel = "Selectivity"

    line_xlabel = "Number of RQ"
    line_keys_ylabel = "Keys Returned"
    line_selectivity_ylabel = "Selectivity"

    # --- Loop through all configured datasets ---
    for config in DATASET_CONFIGS:
        stats_dir = config["stats_dir"]
        plots_dir = config["plots_dir"]

        if not stats_dir.exists():
            print(f"Warning: Data directory not found, skipping: {stats_dir}")
            continue

        # Create the output directory for the current dataset
        plots_dir.mkdir(parents=True, exist_ok=True)
        print(f"\nProcessing data from: {stats_dir.name}")
        print(f"Saving plots to: {plots_dir}")

        for exp_dir in sorted(stats_dir.iterdir()):
            if not (exp_dir.is_dir() and "-C" in exp_dir.name):
                continue

            log_file = exp_dir / "selectivity.log"
            if not log_file.exists():
                continue

            keys_returned, selectivity = parse_log_file(log_file)
            if not keys_returned:
                continue

            if PLOT_TYPE == 'line':
                out_path = plots_dir / f"selectivity_line_{exp_dir.name}.pdf"
                create_line_plot(
                    keys_returned, selectivity, out_path,
                    xlabel=line_xlabel,
                    y1label=line_keys_ylabel,
                    y2label=line_selectivity_ylabel,
                    xlim=config.get("line_xlim"),
                    y1lim=config.get("line_keys_ylim"),
                    y2lim=config.get("line_selectivity_ylim")
                )
            elif PLOT_TYPE == 'histogram':
                out_path = plots_dir / f"selectivity_hist_{exp_dir.name}.pdf"
                create_histogram_plot(
                    keys_returned, selectivity, out_path,
                    x1label=hist_keys_xlabel,
                    y1label=hist_keys_ylabel,
                    x2label=hist_selectivity_xlabel,
                    x1lim=config.get("hist_keys_xlim"),
                    y1lim=config.get("hist_keys_ylim"),
                    x2lim=config.get("hist_selectivity_xlim"),
                    y2lim=config.get("hist_selectivity_ylim")
                )

if __name__ == "__main__":
    main()

