
import re
import argparse
from pathlib import Path
import matplotlib.pyplot as plt
from collections import defaultdict

DEFAULT_ROOT = Path("/home/cc/LSMMemoryProfiling/.result/6_7_insert_only_rawop_low_pri_true_dynamic_vec_memtable_profile")
DEFAULT_PLOTS_SUBDIR = "individual point plots"


TIME_RE = re.compile(r"^(InsertTime|GetTime|ScanTime):\s*(\d+)$")


def collect_by_buffer(root: Path):

    data = defaultdict(lambda: {"InsertTime": [], "GetTime": [], "ScanTime": []})
    for stats in root.rglob("stats.log"):
        variant = stats.parent.parent.name
        buf = variant.split("-", 1)[0]
        for line in stats.read_text().splitlines():
            m = TIME_RE.match(line.strip())
            if m:
                op, val = m.group(1), int(m.group(2))
                data[buf][op].append(val)
    return data


def slice_times(times, subset: str, count: int = 1000):

    n = len(times)
    if subset == 'all' or n <= count:
        return times
    if subset == 'first':
        return times[:count]
    if subset == 'last':
        return times[-count:]
    # middle
    start = max((n // 2) - (count // 2), 0)
    end = start + count
    return times[start:end]


def plot_all(data, out_dir: Path, subset: str):

    
    out_dir.mkdir(parents=True, exist_ok=True)
    for buf, ops in data.items():
        for op, times in ops.items():
            if not times:
                print(f"[WARN] no {op} for {buf}")
                continue
            seg = slice_times(times, subset)
            if not seg:
                print(f"[WARN] slice for {buf}-{op} is empty")
                continue
            plt.figure()
            plt.plot(range(1, len(seg)+1), seg, marker='o', linestyle='-')
            plt.xlabel("Operation #")
            plt.ylabel("Time (ns)")
            plt.ylim (0, 100000)  # Add some margin to the top
            title = f"{buf} — {op} ({subset}{'' if subset=='all' else f' {len(seg)}'})"
            plt.title(title)
            plt.tight_layout()
            suffix = '' if subset=='all' else f"_{subset}"
            fn = out_dir / f"{buf}_{op}{suffix}.png"
            plt.savefig(fn, dpi=200)
            plt.close()
            print(f"[SAVED] {fn}")


def main():
    p = argparse.ArgumentParser(
        description="Plot Insert/Get/Scan times from stats.log under a buffer tree, with optional slicing."
    )
    p.add_argument(
        '-r', '--root', type=Path,
        default=DEFAULT_ROOT,
        help="Top-level experiment folder to scan."
    )
    p.add_argument(
        '-o', '--out-dir', type=Path,
        help="Output directory for plots. Defaults to <root>/<plots_subdir>."
    )
    p.add_argument(
        '-s', '--subset', choices=['all', 'first', 'middle', 'last'],
        default='all',
        help="Which segment to plot: entire series ('all'), first 100, middle 100, or last 100."
    )
    p.add_argument(
        '--plots-subdir', default=DEFAULT_PLOTS_SUBDIR,
        help="Subdirectory under root for plots."
    )
    args = p.parse_args()

    root = args.root.expanduser().resolve()
    if not root.is_dir():
        p.error(f"Not a directory: {root}")

    out_dir = args.out_dir or (root / args.plots_subdir)

    print(f"Scanning `{root}` for stats.log...")
    data = collect_by_buffer(root)
    print(f"Plotting subset='{args.subset}' into `{out_dir}`...")
    plot_all(data, out_dir, args.subset)

if __name__ == '__main__':
    main()
