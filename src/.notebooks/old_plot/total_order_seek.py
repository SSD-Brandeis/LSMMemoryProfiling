import re
import sys
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.ticker import LogFormatterMathtext
from pathlib import Path
from math import ceil

try:
    CURR_DIR = Path(__file__).resolve().parent
except NameError:
    CURR_DIR = Path.cwd()

NOTEBOOKS_DIR = CURR_DIR
if NOTEBOOKS_DIR.name != 'notebooks':
    NOTEBOOKS_DIR = Path('/Users/cba/Desktop/LSMMemoryBuffer/notebooks')
if str(NOTEBOOKS_DIR) not in sys.path:
    sys.path.append(str(NOTEBOOKS_DIR))

FONT_SIZE = 20
FIG_SIZE = (5, 3.6)

BASELINE_STYLE = {
    'label': 'baseline',
    'color': 'gray',
    'linestyle': '--',
    'marker': None,
    'markersize': 0,
    'markerfacecolor': 'none',
    'linewidth': 2,
}

try:
    from plot.style import line_styles
    print("✅  'plot.style' found. Using consistent plot styles.")
    plt.rcParams.update({
        'font.size': FONT_SIZE,
        'axes.grid': False,
        'axes.titlesize': 'large',
        'axes.labelsize': 'medium'
    })
except ImportError:
    print("⚠️  'plot.style' not found. Using default matplotlib settings.")
    FONT_SIZE = 20
    FIG_SIZE = (5, 3.6)
    plt.rcParams.update({
        'font.size': FONT_SIZE,
        'axes.grid': False,
        'axes.titlesize': 'large',
        'axes.labelsize': 'medium'
    })
    line_styles = {
        'hash_linked_list': {
            'label': 'hash linked list',
            'color': '#b22222',
            'linestyle': '-.',
            'marker': 'D',
            'markersize': 12,
            'markerfacecolor': 'none',
            'linewidth': 1,
        },
        'hash_skip_list': {
            'label': 'hash skip list',
            'color': '#1f78b4',
            'linestyle': '--',
            'marker': '^',
            'markersize': 12,
            'markerfacecolor': 'none',
            'linewidth': 1,
        }
    }

MAIN_DATA_PATH = Path('/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_totalorderflag/total_order_seek_flag_rawop-lowpri_true-I450000-U0-Q0-S1000-Y0.1-T10-P131072-B4-E1024')
BASELINE_DATA_PATH = Path('/Users/cba/Desktop/LSMMemoryBuffer/data/filter_result_totalorderflag/total_order_seek_flag_rawop_baseline-lowpri_true-I450000-U0-Q1000-S1000-Y0.1-T10-P131072-B4-E1024')
OUTPUT_DIR = Path('/Users/cba/Desktop/LSMMemoryBuffer/notebooks/paper_plot/total_order_seek')
LOG_FILE_TO_PARSE = 'stats.log'

DIRS_TO_PLOT = {
    'Hash Linked List': 'hash_linked_list-X6-H100000',
    'Hash Skip List': 'hash_skip_list-X6-H100000'
}
STYLE_KEY_MAP = {
    'Hash Linked List': 'hash_linked_list',
    'Hash Skip List': 'hash_skip_list'
}

LATENCY_REGEX = re.compile(r"ScanTime:\s*([\d\.]+)")
TOKEN_PAT = {
    "insert": re.compile(r"\bI(\d+)\b"),
    "point": re.compile(r"\bQ(\d+)\b"),
    "scan": re.compile(r"\bS(\d+)\b"),
    "yield": re.compile(r"\bY([\d\.]+)\b"),
}

USE_LOG_SCALE = False
YLIM_LATENCY = (0,0.5)
ROLLING_AVERAGE_WINDOW = 1

def line_style(pretty_buf_name):
    global line_styles
    if "line_styles" not in globals():
        line_styles = {}
    style_key = STYLE_KEY_MAP.get(pretty_buf_name)
    if not style_key:
        print(f"Warning: No style key mapping found for '{pretty_buf_name}'.")
        default_style = {"color": "black", "marker": ".", "linestyle": ":", "label": pretty_buf_name}
        return default_style.copy()
    default_style = {"color": "black", "marker": ".", "linestyle": ":", "label": style_key}
    return line_styles.get(style_key, default_style).copy()

def parse_log_for_latencies(file_path):
    latencies = []
    try:
        with open(file_path, 'r') as f:
            for line in f:
                match = LATENCY_REGEX.search(line)
                if match:
                    try:
                        latencies.append(float(match.group(1)))
                    except ValueError:
                        print(f"Warning: Could not convert value '{match.group(1)}' to float in {file_path}")
    except FileNotFoundError:
        print(f"Error: Log file not found at {file_path}")
    except Exception as e:
        print(f"Error reading {file_path}: {e}")
    return latencies

def collect_records(base_path, dirs_to_plot):
    print(f"--- Collecting records from: {base_path} ---")
    all_data = {}
    for pretty_name, dir_name in dirs_to_plot.items():
        sub_dir_path = base_path / dir_name
        if not sub_dir_path.exists():
            print(f"Warning: Directory not found, skipping: {sub_dir_path}")
            continue
        log_path = sub_dir_path / LOG_FILE_TO_PARSE
        print(f"Processing {log_path}...")
        latencies = parse_log_for_latencies(log_path)
        if not latencies:
            print(f"Warning: No 'ScanTime' data found in {log_path}")
            continue
        all_data[pretty_name] = latencies
    return all_data

def format_value(v):
    if v >= 1_000_000:
        return f'{v / 1_000_000:g}M'
    if v >= 1_000:
        return f'{v / 1_000:g}K'
    return f'{v:g}'

def parse_folder_name(folder_name):
    parts = []
    m_ins = TOKEN_PAT["insert"].search(folder_name)
    m_pq = TOKEN_PAT["point"].search(folder_name)
    m_scan = TOKEN_PAT["scan"].search(folder_name)
    m_yield = TOKEN_PAT["yield"].search(folder_name)
    if m_ins:
        parts.append(f'I={format_value(int(m_ins.group(1)))}')
    if m_pq:
        pq_val = int(m_pq.group(1))
        if pq_val == 0:
             parts.append('PQ=0')
        else:
             parts.append(f'PQ={format_value(pq_val)}')
    if m_scan:
        parts.append(f'S={format_value(int(m_scan.group(1)))}')
    if m_yield:
        parts.append(f'Y={float(m_yield.group(1)):g}')
    return " ".join(parts)

def apply_axis_style(ax, y_limit_tuple, is_latency_plot, formatter=None):
    if USE_LOG_SCALE:
        ax.set_yscale("log", base=10)
        ax.set_ylim(*y_limit_tuple)
        ax.yaxis.set_major_formatter(LogFormatterMathtext(base=10))
    else:
        if y_limit_tuple and all(y is not None for y in y_limit_tuple):
            ax.set_ylim(*y_limit_tuple)
        if formatter:
            ax.yaxis.set_major_formatter(formatter)
        else:
            sci_formatter = mticker.ScalarFormatter(useMathText=True)
            sci_formatter.set_powerlimits((0, 0))
            ax.yaxis.set_major_formatter(sci_formatter)
    ax.tick_params(axis="y", labelsize=plt.rcParams["font.size"])

DECIMALS_RE = re.compile(r"\.(\d+)")

def save_plot_caption(caption_text, base_output_path):
    if not caption_text:
        return
    has_decimals = DECIMALS_RE.search(caption_text)
    caption_fontsize = plt.rcParams["font.size"]
    if has_decimals:
        caption_fontsize *= 1
    title_fig = plt.figure(figsize=(0.1, 0.1))
    txt = title_fig.text(
        0, 0, caption_text, ha="left", va="bottom", fontsize=caption_fontsize
    )
    plt.axis("off")
    caption_output_path = base_output_path.with_name(
        f"{base_output_path.name}_caption.pdf"
    )
    title_fig.savefig(
        caption_output_path,
        bbox_inches="tight",
        pad_inches=0.01,
        bbox_extra_artists=[txt],
    )
    print(f"[saved] {caption_output_path.name}")
    plt.close(title_fig)

def save_plot_legend(handles, labels, base_output_path):
    if not handles:
        print("Warning: No legend handles found. Skipping legend save.")
        return
    ncol = 1
    legend_fig = plt.figure(figsize=(0.1, 0.1))
    leg = legend_fig.legend(
        handles,
        labels,
        loc="center",
        ncol=ncol,
        frameon=False,
        fontsize=plt.rcParams["font.size"],
        borderpad=0.1,
        columnspacing=0.8,
        handletextpad=0.5,
    )
    plt.axis("off")
    legend_output_path = base_output_path.with_name(
        f"{base_output_path.name}_legend.pdf"
    )
    legend_fig.savefig(
        legend_output_path,
        bbox_inches="tight",
        pad_inches=0.01,
        bbox_extra_artists=[leg],
    )
    print(f"[saved] {legend_output_path.name}")
    plt.close(legend_fig)

def _process_and_plot_line(ax, latencies_ns, style, label_prefix):
    if not latencies_ns:
        print(f"Warning: No data for line: {label_prefix}")
        return None, None
    min_len = len(latencies_ns)
    latencies_s = np.array(latencies_ns) * 1e-9
    window = ROLLING_AVERAGE_WINDOW
    if window > 1:
        if window > min_len:
            print(f"Warning: Window size ({window}) > data ({min_len}) for {label_prefix}. Skipping rolling avg.")
            data_to_plot = latencies_s
            op_counts = np.arange(min_len)
        else:
            data_to_plot = np.convolve(latencies_s, np.ones(window)/window, mode='valid')
            start_x = window - 1
            op_counts = np.arange(start_x, min_len)
            label = f"{label_prefix} ({window}-op avg)"
    else:
        data_to_plot = latencies_s
        op_counts = np.arange(min_len)
        label = label_prefix
    handle = ax.plot(op_counts, data_to_plot, **style)[0]
    return handle, label

def plot_latency_body(pretty_name, plot_data, base_output_path, y_limit):
    print(f"Generating plot body: {base_output_path.name}.pdf ...")
    fig, ax = plt.subplots(figsize=FIG_SIZE)
    all_handles, all_labels = [], []
    main_latencies = plot_data.get('main')
    baseline_latencies = plot_data.get('baseline')
    all_lengths = []
    if main_latencies:
        all_lengths.append(len(main_latencies))
    if baseline_latencies:
        all_lengths.append(len(baseline_latencies))
    if not all_lengths:
        print("Error: No data found to plot.")
        plt.close(fig)
        return [], []
    min_len = min(all_lengths)
    if main_latencies:
        main_latencies = main_latencies[:min_len]
        main_style = line_style(pretty_name)
        main_label = main_style.pop("label")
        main_style['marker'] = None
        main_style['markersize'] = 0
        handle, label = _process_and_plot_line(ax, main_latencies, main_style, main_label)
        if handle:
            all_handles.append(handle)
            all_labels.append(label)
    if baseline_latencies:
        baseline_latencies = baseline_latencies[:min_len]
        baseline_style = BASELINE_STYLE.copy()
        baseline_label = baseline_style.pop("label")
        handle, label = _process_and_plot_line(ax, baseline_latencies, baseline_style, baseline_label)
        if handle:
            all_handles.append(handle)
            all_labels.append(label)
    
    ax.set_xlim(0, 10)
    ax.xaxis.set_major_locator(mticker.MaxNLocator(integer=True))

    ax.set_xlabel('operation count (RQ)')
    ax.set_ylabel('latency (s)')
    apply_axis_style(ax, y_limit, is_latency_plot=True, formatter=None)
    plt.tight_layout()
    output_filename = base_output_path.with_suffix(".pdf")
    fig.savefig(output_filename, bbox_inches="tight", pad_inches=0.1)
    print(f"[saved] {output_filename.name}")
    plt.close(fig)
    return all_handles, all_labels

def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Ensuring output directory exists: {OUTPUT_DIR}")
    main_data_dict = collect_records(MAIN_DATA_PATH, DIRS_TO_PLOT)
    baseline_data_dict = collect_records(BASELINE_DATA_PATH, DIRS_TO_PLOT)
    if not main_data_dict and not baseline_data_dict:
        print("No data collected from any directory. Exiting.")
        return
    caption_text = parse_folder_name(MAIN_DATA_PATH.name)
    for pretty_name in DIRS_TO_PLOT.keys():
        print(f"\n--- Generating plot for: {pretty_name} ---")
        main_latencies = main_data_dict.get(pretty_name)
        baseline_latencies = baseline_data_dict.get(pretty_name)
        if not main_latencies and not baseline_latencies:
            print(f"No data for '{pretty_name}' in main or baseline. Skipping plot.")
            continue
        plot_data = {
            'main': main_latencies,
            'baseline': baseline_latencies
        }
        style_key = STYLE_KEY_MAP.get(pretty_name, 'unknown')
        base_output_path = OUTPUT_DIR / f'total_order_seek_latency_{style_key}'
        handles, labels = plot_latency_body(
            pretty_name,
            plot_data,
            base_output_path,
            y_limit=YLIM_LATENCY
        )
        save_plot_legend(handles, labels, base_output_path)
        save_plot_caption(caption_text, base_output_path)
    print("\nScript finished.")

if __name__ == "__main__":
    main()