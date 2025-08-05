
import re
from pathlib import Path
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

FIRST_X = None
# PLOT_SCALE = "log"
PLOT_SCALE = "linear"
Y_LIM_BOTTOM = 0
Y_LIM_TOP = 5e8

# DATA_DIR = Path("/home/cc/LSMMemoryProfiling/.result/rerunrawop-lowpri_false-I300000-U0-Q10000-S1000-Y0.1-T10-P16384-B4-E1024")

# DATA_DIR = Path("/home/cc/LSMMemoryProfiling/.result/debugsortinterleave-lowpri_false-I250000-U0-Q10000-S1000-Y0.1-T10-P16384-B4-E1024")

# DATA_DIR = Path("/home/cc/LSMMemoryProfiling/.result/debugsortrawop-lowpri_false-I250000-U0-Q10000-S1000-Y0.1-T10-P16384-B4-E1024")

#interleave 450k 
DATA_DIR = Path("/home/cc/LSMMemoryProfiling/.result/83interleave-lowpri_false-I450000-U0-Q10000-S1000-Y0.1-T10-P131072-B4-E1024")

# #rawop 450k
# DATA_DIR = Path("/home/cc/LSMMemoryProfiling/.result/83rawop-lowpri_false-I450000-U0-Q10000-S1000-Y0.1-T10-P131072-B4-E1024")

PLOTS_DIR = DATA_DIR / "gettime plot"
PLOTS_DIR.mkdir(parents=True, exist_ok=True)

OPERATION = "GetTime"
GETTIME_RE = re.compile(rf"{OPERATION}:\s*([\d.]+)")

def get_buffer_dirs(data_dir):
    dirs = []
    for entry in sorted(data_dir.iterdir()):
        if entry.is_dir() and (entry / "stats.log").exists():
            dirs.append(entry)
    return dirs


def read_latencies(stats_path, regex):
    lines = stats_path.read_text().splitlines()
    latencies = []
    for line in lines:
        match = regex.search(line)
        if match:
            latencies.append(float(match.group(1)))
    return latencies


def save_latencies(latencies, dump_path):
    dump_path.write_text("\n".join(str(x) for x in latencies))


def plot_latencies(latencies, title, out_path, first_x):
    data = latencies[:first_x] if first_x else latencies
    x = np.arange(1, len(data) + 1)
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(x, data, linewidth=0.8)
    ax.set_xlabel("data point")
    ax.set_ylabel("Latency (ns)")
    ax.set_title(title)
    ax.grid(True, linestyle="--", alpha=0.3)
    ax.set_yscale(PLOT_SCALE)
    ax.set_ylim(Y_LIM_BOTTOM, Y_LIM_TOP)
    fig.tight_layout()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out_path, bbox_inches="tight", pad_inches=0.05)
    plt.close(fig)


def main():
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    for buf_dir in get_buffer_dirs(DATA_DIR):
        stats_file = buf_dir / "stats.log"
        latencies = read_latencies(stats_file, GETTIME_RE)
        if not latencies:
            continue
        dump_path = buf_dir / f"{OPERATION}.log"
        save_latencies(latencies, dump_path)
        title = buf_dir.name
        if FIRST_X:
            title = f"{title} — first {FIRST_X}"
        title = f"{title} — {OPERATION}"
        out_file = PLOTS_DIR / f"{buf_dir.name}_{OPERATION.lower()}_trace.pdf"
        plot_latencies(latencies, title, out_file, FIRST_X)
        print(f"Saved data to {dump_path}")
        print(f"Saved plot to {out_file}")

if __name__ == "__main__":
    main()
