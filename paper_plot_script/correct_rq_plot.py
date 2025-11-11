from pathlib import Path
import sys, re
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as font_manager
from collections import defaultdict


CURR_DIR = Path(__file__).resolve().parent
ROOT = CURR_DIR.parent
sys.path.insert(0, str(ROOT))
DATA_ROOT = ROOT / "data"

STATS_DIR = DATA_ROOT / "filter_result_fixed_rq_selectivity" / "fixed-rq-selectivity-insertPQRS_X8H1M_varC-lowpri_false-I480000-U0-Q0-S1000-Y0.001-T10-P131072-B4-E1024"
PLOTS_DIR = DATA_ROOT / "plots" / "common_prefix_plots"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)



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

FIGSIZE = (6, 4)


PLOT_STYLES = {
    "keys_returned": {
        "color": "tab:blue",
        "marker": "o",
        "markersize": 5,
        "linestyle": "-",
        "label": r"Avg. Entries Returned" # Changed
    },
    "selectivity": {
        "color": "tab:red",
        "marker": "x",
        "markersize": 6,
        "linestyle": "--",
        "label": r"Avg. Selectivity" # Consistent
    }
}



def parse_log_file(file_path: Path):
    """Parses the selectivity log file to extract keys returned and selectivity."""
    keys_returned = []
    selectivity = []
    text = file_path.read_text(errors="ignore")
    for line in text.splitlines():
        match = re.search(r'keys_returned: (\d+), selectivity: ([-+]?\d*\.?\d+([eE][-+]?\d+)?)', line)
        if match:
            keys_returned.append(int(match.group(1)))
            selectivity.append(float(match.group(2)))
    return keys_returned, selectivity

def create_common_prefix_plot(x_data, y1_data, y2_data, out_path: Path,
                              xlabel, y1label, y2label, y2lim=None):

    fig, ax1 = plt.subplots(figsize=FIGSIZE)
    fig.subplots_adjust(left=0.15, right=0.85, top=0.95, bottom=0.35)

    style1 = PLOT_STYLES["keys_returned"]
    line1 = ax1.plot(x_data, y1_data, **style1)[0]
    ax1.set_ylabel(y1label, color=style1["color"])
    ax1.tick_params(axis='y', labelcolor=style1["color"])
    ax1.set_xlabel(xlabel)
    ax1.set_xticks(x_data)
    ax1.set_yscale('log')
    ax2 = ax1.twinx()
    style2 = PLOT_STYLES["selectivity"]
    line2 = ax2.plot(x_data, y2_data, **style2)[0]
    ax2.set_ylabel(y2label, color=style2["color"])
    ax2.tick_params(axis='y', labelcolor=style2["color"])
    
    if y2lim is not None:
        ax2.set_ylim(y2lim)

    fig.legend(
        handles=[line1, line2],
        loc="lower center",
        ncol=2,
        bbox_to_anchor=(0.5, 0),
        frameon=False,
        labelspacing=0.3,
        handletextpad=0.5,
        columnspacing=1.5,
    )
    
    fig.savefig(out_path, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"✅ Plot saved to {out_path.name}")



def main():
    if not STATS_DIR.exists():
        print(f"Data directory not found at '{STATS_DIR}'")
        return

    data_by_structure = defaultdict(list)

    for exp_dir in STATS_DIR.iterdir():
        match = re.search(r'^(.*)-C(\d+)$', exp_dir.name)
        if not (exp_dir.is_dir() and match):
            continue

        structure_name = match.group(1)
        common_prefix = int(match.group(2))
        log_file = exp_dir / "selectivity.log"

        if not log_file.exists():
            continue

        keys_returned, selectivity = parse_log_file(log_file)
        if not keys_returned:
            continue
        

        avg_keys = np.mean(keys_returned)
        avg_selectivity = np.mean(selectivity)
        data_by_structure[structure_name].append((common_prefix, avg_keys, avg_selectivity))

    if not data_by_structure:
        print(" No data found to plot. Check directory names and log files.")
        return

    for structure_name, data_points in data_by_structure.items():
        if not data_points:
            continue

        data_points.sort()
        c_values, avg_keys_list, avg_selectivity_list = zip(*data_points)
        out_path = PLOTS_DIR / f"{structure_name}_vs_common_prefix.pdf"
        

        create_common_prefix_plot(
            x_data=c_values,
            y1_data=avg_keys_list,
            y2_data=avg_selectivity_list,
            out_path=out_path,
            xlabel="Common Prefix Length",
            y1label="Avg. Entries Returned", 
            y2label="Avg. Selectivity",      
            y2lim=(0, 1)
        )

if __name__ == "__main__":
    main()