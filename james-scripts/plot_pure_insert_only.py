import re
import argparse
from pathlib import Path
import matplotlib.pyplot as plt
from collections import defaultdict


DEFAULT_ROOT = Path("/home/cc/LSMMemoryProfiling/.result/6_29_rawop_low_pri_false_larger_refill")
DEFAULT_PLOTS_SUBDIR = "pure plots"
POINT_COUNT = 1000  


PAIR_RE = re.compile(r"([\w\[\]_/-]+):\s*(\d+)")


def should_plot(buf_name: str, attr: str) -> bool:
    buf = buf_name.lower()
    attr = attr.strip()
    if buf in ("vector", "preallocated vector", "unsortedvector"):
        return attr == "VectorRep"
    if buf == "alwayssortedvector":
        return attr == "AlwaysSortedVectorRep"
    if buf == "hash_skip_list":
        return attr == "HashSkipList_Insert"
    if buf == "skiplist":
        return attr == "SkipList_InsertKey"
    if buf == "hash_linked_list":
        return attr.startswith("HashLinkList_Insert")
    return False


def collect_by_buffer(root: Path):
    data = defaultdict(lambda: defaultdict(list))
    for temp_log in root.rglob("temp.log"):
        buf = temp_log.parent.parent.parent.name.lower()
        for line in temp_log.read_text().splitlines():
            if not line or line.startswith(("Destroying", "kBlockSize", "Clearing")):
                continue
            for raw_label, val_str in PAIR_RE.findall(line):
                raw_label = raw_label.strip()
                val = int(val_str)
                if not should_plot(buf, raw_label):
                    continue
    
                if buf == "hash_linked_list":
                    op_key = "HashLinkList_Insert"
                else:
                    op_key = raw_label
                data[buf][op_key].append(val)
    return data


def slice_times(times, subset: str):
    n = len(times)
    if subset == "all" or n <= POINT_COUNT:
        return times
    if subset == "first":
        return times[:POINT_COUNT]
    if subset == "last":
        return times[-POINT_COUNT:]
    start = max((n // 2) - (POINT_COUNT // 2), 0)
    return times[start : start + POINT_COUNT]


def plot_all(data, out_dir: Path, subset: str):
    out_dir.mkdir(parents=True, exist_ok=True)
    for buf, ops in data.items():
        for op, times in ops.items():
            if not times:
                print(f"[WARN] no data for {buf} – {op}")
                continue
            seg = slice_times(times, subset)
            if not seg:
                print(f"[WARN] empty slice for {buf} – {op}")
                continue

            plt.figure()
            plt.plot(range(1, len(seg) + 1), seg, marker="o", linestyle="-")
            plt.xlabel("Operation #")
            plt.ylabel("Latency (ns)")
            plt.ylim(0, 100000)
            title = f"{buf} — {op} ({subset}{'' if subset=='all' else f' {len(seg)}'})"
            plt.title(title)
            plt.tight_layout()

            suffix = "" if subset == "all" else f"_{subset}"
            filename = f"{buf}_{op.replace(' ', '_')}{suffix}.png"
            path = out_dir / filename
            plt.savefig(path, dpi=200)
            plt.close()
            print(f"[SAVED] {path}")


def main():
    p = argparse.ArgumentParser(
        description="Plot exactly one insert‐latency series per buffer type from temp.log"
    )
    p.add_argument(
        "-r", "--root", type=Path,
        default=DEFAULT_ROOT,
        help="Top‐level experiment folder."
    )
    p.add_argument(
        "-o", "--out-dir", type=Path,
        help="Output directory (default: <root>/<plots_subdir>)."
    )
    p.add_argument(
        "-s", "--subset", choices=["all", "first", "middle", "last"],
        default="all",
        help="Which segment to plot."
    )
    p.add_argument(
        "--plots-subdir", default=DEFAULT_PLOTS_SUBDIR,
        help="Subdirectory name for the output plots."
    )
    args = p.parse_args()

    root = args.root.expanduser().resolve()
    if not root.is_dir():
        p.error(f"Not a directory: {root}")

    out_dir = args.out_dir or (root / args.plots_subdir)

    print(f"Scanning `{root}` for temp.log…")
    data = collect_by_buffer(root)
    print(f"Plotting subset='{args.subset}' (POINT_COUNT={POINT_COUNT}) into `{out_dir}`…")
    plot_all(data, out_dir, args.subset)

if __name__ == "__main__":
    main()
