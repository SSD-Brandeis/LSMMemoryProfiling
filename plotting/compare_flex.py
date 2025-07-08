from pathlib import Path
from typing import Optional, Dict, List
from collections import defaultdict
import re, itertools
import matplotlib.pyplot as plt

# EXPERIMENT_ROOTS = [
#     Path("/home/cc/LSMMemoryProfiling/.result/"
#          "7_2_rawop_low_pri_false_default_refill"),
#     Path("/home/cc/LSMMemoryProfiling/.result/"
#          "7_2_rawop_low_pri_true_default_refill"),
#     Path("/home/cc/LSMMemoryProfiling/.result/"
#          "7_2_rawop_low_pri_true_larger_refill"),
# ]

EXPERIMENT_ROOTS = [
    Path("/home/cc/LSMMemoryProfiling/.result/dddebug_7_5_rawop_low_pri_true_default_refill"),
    Path("/home/cc/LSMMemoryProfiling/.result/7_2_rawop_low_pri_true_default_refill"),
    Path("/home/cc/LSMMemoryProfiling/.result/7_2_rawop_low_pri_false_default_refill"),

]

OUT_SUBDIR        = "comparative flex plots"
TARGET_ENTRY_SIZE = None            

LOG_SCALE = False


COMPONENTS = [
    "Insert",
    "Lock",
    "MemTableRep",
    "MemTable",
    "PutCFImpl",
    "WriteBatchInternal",
    "DBImpl",

]


Y_LIM_LINEAR = (0, 100000)
Y_LIM_LOG    = (1e2, 1e6)


INSERT_MAP: Dict[str, str] = {
    "vector": "VectorRep",
    "preallocated vector": "VectorRep",
    "unsortedvector": "VectorRep",
    "alwayssortedvector": "AlwaysSortedVectorRep",
    "hash_skip_list": "HashSkipList_Insert",
    "skiplist": "SkipList_InsertKey",
    "hash_linked_list": "HashLinkList_Insert",
}

_PREAMBLE = ("Destroying database", "kBlockSize", "Clearing system cache")
PAIR_RE   = re.compile(r"([\w\[\]_/-]+):\s*([0-9]+)")



def _size_tag() -> Optional[str]:
    if TARGET_ENTRY_SIZE is None:
        return None
    m = re.match(r"(\d+)", str(TARGET_ENTRY_SIZE))
    return f"entry_{m.group(1)}b" if m else None


def wanted_labels_for_buffer(buf_lower: str) -> Dict[str, str]:
    mapping = {}
    for logical in COMPONENTS:
        if logical.lower() == "insert":
            mapping[INSERT_MAP.get(buf_lower, "Insert")] = "Insert"
        else:
            mapping[logical] = logical
    return mapping


def collect(roots) -> Dict[str, Dict[str, Dict[str, List[int]]]]:
    tag = _size_tag()
    data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))

    for root in roots:
        exp_label = root.name
        for log in root.rglob("run1.log"):
            if tag and tag not in log.parent.parent.name:
                continue

            buf_dir   = log.parent.parent.parent.name
            buf_lower = buf_dir.lower()
            label_map = wanted_labels_for_buffer(buf_lower)

            if not label_map:
                continue

            with log.open() as f:
                for line in f:
                    if not line or line.startswith(_PREAMBLE):
                        continue
                    for raw_tok, val_str in PAIR_RE.findall(line):
                        logical = label_map.get(raw_tok.strip())
                        if logical:
                            data[buf_dir][exp_label][logical].append(int(val_str))
    return data


def plot(data, exp_labels, out_dir: Path):
    out_dir.mkdir(parents=True, exist_ok=True)
    suffix = f"_{TARGET_ENTRY_SIZE}B" if TARGET_ENTRY_SIZE else ""

    base_colors = plt.rcParams["axes.prop_cycle"].by_key()["color"]
    color_map = {comp: base_colors[i % len(base_colors)]
                 for i, comp in enumerate(COMPONENTS)}

    for buf, per_exp in data.items():
        fig, axes = plt.subplots(
            len(exp_labels), 1,
            figsize=(8, 2.7 * len(exp_labels)),
            sharex=True, sharey=False
        )
        axes = axes.flatten()

        handles_for_legend = {}

        for ax, exp_label in zip(axes, exp_labels):
            comp_dict = per_exp.get(exp_label, {})
            if not comp_dict:
                ax.set_visible(False)
                continue

            for comp in COMPONENTS:
                y = comp_dict.get(comp)
                if not y:
                    continue
                x = range(1, len(y) + 1)
                line, = ax.plot(
                    x, y, lw=1,
                    label=comp,
                    color=color_map[comp]
                )
                handles_for_legend.setdefault(comp, line)

       
            if LOG_SCALE:
                ax.set_yscale("log")
                ax.set_ylim(Y_LIM_LOG)
            else:
                ax.set_ylim(Y_LIM_LINEAR)

            ax.set_title(exp_label, fontsize=8, pad=3)
            ax.tick_params(axis='y', labelsize=8)
            ax.tick_params(axis='x', labelsize=8)

        axes[-1].set_xlabel("Data-point #")
        fig.text(
            0.02, 0.5, "Latency (ns)",
            rotation=90, va="center", ha="center", fontsize=10
        )

        if handles_for_legend:
            legend_handles = [handles_for_legend[c] for c in COMPONENTS if c in handles_for_legend]
            legend_labels  = [c for c in COMPONENTS if c in handles_for_legend]
            fig.legend(
                legend_handles, legend_labels,
                loc="lower center", ncol=min(len(legend_labels), 3),
                frameon=False, fontsize=7
            )

        fig.subplots_adjust(left=0.14, top=0.90, bottom=0.28, hspace=0.35)

        comps_tag = "+".join(COMPONENTS)
        out_path  = out_dir / f"{buf}_{comps_tag}{suffix}.png"
        fig.savefig(out_path, dpi=250)
        plt.close(fig)
        print(f"[SAVED] {out_path}")


def main():
    if not COMPONENTS:
        raise SystemExit("No components selected – un-comment at least one.")

    for p in EXPERIMENT_ROOTS:
        if not p.is_dir():
            raise SystemExit(f"Missing experiment dir: {p}")

    out_dir = EXPERIMENT_ROOTS[0] / OUT_SUBDIR
    data = collect(EXPERIMENT_ROOTS)
    plot(data, [p.name for p in EXPERIMENT_ROOTS], out_dir)
    print("[DONE]")


if __name__ == "__main__":
    main()
