#!/usr/bin/env python3
import re
import argparse
from pathlib import Path
from collections import defaultdict
import matplotlib.pyplot as plt



DEFAULT_ROOT = Path("/home/cc/LSMMemoryProfiling/.result/6_29_rawop_low_pri_true_larger_refill")
DEFAULT_PLOTS_SUBDIR = "bump plots"



TARGET_ENTRY_SIZE = None    


_PREAMBLE_PREFIXES = (
    "Destroying database",
    "kBlockSize",
    "Clearing system cache",
)

PAIR_RE = re.compile(r"([\w]+):\s*([0-9]+)")

# ---------- helpers ----------

def _clean_component(token: str) -> str:
    t = token.lower()
    if t.startswith("hashlinklist"):
        return "HashLinkList"
    if "_" in token:
        return token.split("_", 1)[1]
    return token

def _size_tag():
    if TARGET_ENTRY_SIZE:
        m = re.match(r"(\d+)", TARGET_ENTRY_SIZE)
        return f"entry_{m.group(1)}b" if m else None
    return None

def collect_by_buffer(root: Path):
    size_tag = _size_tag()
    data: dict[str, dict[str, list[int]]] = defaultdict(lambda: defaultdict(list))
    matched = 0

    for log_path in root.rglob("temp.log"):
        variant = log_path.parent.parent.name
        if size_tag and size_tag not in variant:
            continue
        matched += 1
        buf = variant.split("-", 1)[0]
        for line in log_path.read_text().splitlines():
            ln = line.strip()
            if not ln or ln.startswith(_PREAMBLE_PREFIXES):
                continue
            for tok, val in PAIR_RE.findall(ln):
                comp = _clean_component(tok)
                data[buf][comp].append(int(val))

    if size_tag and matched == 0:
        print(f"[WARN] No temp.log matched entry size '{TARGET_ENTRY_SIZE}B'.")
    return data

def plot_all(data, out_dir: Path):
    # gather global max for y-limit
    all_vals = []
    for comp_dict in data.values():
        for lst in comp_dict.values():
            all_vals.extend(lst)
    if not all_vals:
        print("[ERROR] No data points found—aborting plots.")
        return
    y_max = max(all_vals)

    out_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"_{TARGET_ENTRY_SIZE}B" if TARGET_ENTRY_SIZE else ""

    for buf, comp_dict in data.items():
        if not comp_dict:
            print(f"[WARN] No data for `{buf}`—skipping.")
            continue

        fig, ax = plt.subplots(figsize=(8, 5))
        order = ["HashLinkList", "InsertKey", "MemTable",
                 "PutCFImpl", "WriteBatchInternal", "DBImpl"]
        comps = sorted(comp_dict, key=lambda c: order.index(c) if c in order else len(order))



        for comp in comps:
            y = comp_dict[comp]
            if not y:
                continue
            x = range(1, len(y) + 1)

            # if this is the HashLinkList curve, rename it to "Insert"
            legend_label = "Insert" if comp == "HashLinkList" else comp

            ax.plot(x, y, label=legend_label, linewidth=1)


        ax.set_yscale("log")
        ax.set_ylim(10, y_max)        
        ax.set_xlabel("Data-point #")
        ax.set_ylabel("Latency (ns)")

        title = f"{buf}"
        if TARGET_ENTRY_SIZE:
            title += f" (entry {TARGET_ENTRY_SIZE} B)"
        ax.set_title(title)

        # horizontal legend below the plot, no frame, wrap into multiple rows
        cols = 3  
        ax.legend(
            ncol=cols,
            loc="upper center",
            bbox_to_anchor=(0.5, -0.18),
            frameon=False
        )

        # make room for the two‐row legend
        fig.subplots_adjust(bottom=0.30)


        fn = out_dir / f"{buf}_{suffix}.png"
        fig.savefig(fn, dpi=250)
        plt.close(fig)
        print(f"[SAVED] {fn}")

# ---------- CLI ----------
def main():
    p = argparse.ArgumentParser(
        description="Plot component latencies from temp.log files."
    )
    p.add_argument("-r", "--root", type=Path, default=DEFAULT_ROOT,
                   help="Top-level experiment folder.")
    p.add_argument("-o", "--out-dir", type=Path,
                   help=f"Directory for output plots (default <root>/{DEFAULT_PLOTS_SUBDIR}).")
    args = p.parse_args()

    root = args.root.expanduser().resolve()
    if not root.is_dir():
        p.error(f"Not a directory: {root}")
    out_dir = args.out_dir or (root / DEFAULT_PLOTS_SUBDIR)

    print(f"Scanning `{root}` for temp.log …")
    data = collect_by_buffer(root)

    print(f"Creating plots in `{out_dir}` …")
    plot_all(data, out_dir)

if __name__ == "__main__":
    main()
