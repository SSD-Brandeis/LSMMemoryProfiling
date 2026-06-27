import os
from pathlib import Path

import numpy as np
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt

from plot import *
from plot.style import line_styles, hatch_map
from plot.rocksdb_stats import parse_rocksdb_log

TAG = "lowpri-wal-exp"
os.makedirs(TAG, exist_ok=True)

CURR_DIR = Path.cwd()
PROJECT_ROOT = CURR_DIR.parent.parent
EXP_DIR = PROJECT_ROOT / ".vstats" / TAG

BUCKET_COUNT   = 100000
PREFIX_LENGTH  = 6

# 4 configs: (low_pri, wal)
CONFIGS = [
    ("LP0-WAL0", "lp=0\nwal=0"),
    ("LP0-WAL1", "lp=0\nwal=1"),
    ("LP1-WAL0", "lp=1\nwal=0"),
    ("LP1-WAL1", "lp=1\nwal=1"),
]

# (style_key, subdir_name)
IMPLS = [
    ("vector",               "vector-preallocated"),
    ("unsortedvector",       "unsortedvector-preallocated"),
    ("alwayssortedvector",   "sortedvector-preallocated"),
    ("skiplist",             "skiplist"),
    ("simpleskiplist",       "simpleskiplist"),
    ("hashlinkedlist",       f"hashlinkedlist-H{BUCKET_COUNT}-X{PREFIX_LENGTH}"),
    ("hashskiplist",         f"hashskiplist-H{BUCKET_COUNT}-X{PREFIX_LENGTH}"),
    ("hashvector",           f"hashvector-H{BUCKET_COUNT}-X{PREFIX_LENGTH}"),
]

# Op counts from workload spec (hardcoded from bash script)
INSERT_COUNT        = 100_000_000   # 80M + 10M + 10M (all phases)
INSERT_COUNT_PHASE1 = 80_000_000    # phase 0: bulk inserts only
PQ_COUNT     = 10_000
RQ_COUNT     = 1_000


TOTAL_OPS = INSERT_COUNT + PQ_COUNT + RQ_COUNT


def read_phase1_insert_throughput(workload_log: Path) -> float:
    phases = parse_rocksdb_log(str(workload_log))
    if not phases:
        return 0.0
    insert_time_ns = phases[0]["meta"].get("insert_time", 0)
    return INSERT_COUNT_PHASE1 / (insert_time_ns / 1e9) / 1e6 if insert_time_ns > 0 else 0.0


def read_throughputs(workload_log: Path) -> tuple[float, float, float, float]:
    phases = parse_rocksdb_log(str(workload_log))
    insert_time_ns  = sum(p["meta"].get("insert_time",       0) for p in phases)
    pq_time_ns      = sum(p["meta"].get("point_query_time",  0) for p in phases)
    rq_time_ns      = sum(p["meta"].get("range_query_time",  0) for p in phases)
    workload_time_ns = sum(p["meta"].get("workload_time",    0) for p in phases)
    ins     = INSERT_COUNT / (insert_time_ns   / 1e9) / 1e6 if insert_time_ns   > 0 else 0.0
    pq      = PQ_COUNT     / (pq_time_ns       / 1e9) / 1e3 if pq_time_ns       > 0 else 0.0
    rq      = RQ_COUNT     / (rq_time_ns       / 1e9) / 1e3 if rq_time_ns       > 0 else 0.0
    overall = TOTAL_OPS    / (workload_time_ns  / 1e9) / 1e6 if workload_time_ns > 0 else 0.0
    return ins, pq, rq, overall


def load_data() -> dict[str, dict[str, list[float]]]:
    data = {key: {"insert": [], "PQ": [], "RQ": [], "overall": []} for key, _ in IMPLS}
    for cfg_dir, _ in CONFIGS:
        for style_key, subdir in IMPLS:
            wlog = EXP_DIR / cfg_dir / subdir / "workload.log"
            if wlog.exists():
                ins, pq, rq, overall = read_throughputs(wlog)
            else:
                print(f"Missing: {wlog}")
                ins, pq, rq, overall = 0.0, 0.0, 0.0, 0.0
            data[style_key]["insert"].append(ins)
            data[style_key]["PQ"].append(pq)
            data[style_key]["RQ"].append(rq)
            data[style_key]["overall"].append(overall)
    return data


def _plot_op(data, op_key, ylabel, output_name, fig_label, yticks=None, yticklabels=None):
    n_configs = len(CONFIGS)
    n_impls   = len(IMPLS)
    width     = 0.8 / n_impls
    x         = np.arange(n_configs)

    fig, ax = plt.subplots(figsize=(3.4, 1.8))

    for i, (style_key, _) in enumerate(IMPLS):
        s      = line_styles[style_key]
        offset = (i - n_impls / 2 + 0.5) * width
        ax.bar(x + offset, data[style_key][op_key], width,
               color="none" if style_key != "vector" else s["color"], edgecolor=s["color"], label=s["label"], hatch=hatch_map[style_key])

    cfg_labels = [lbl for _, lbl in CONFIGS]
    ax.set_xticks(x)
    # if op_key not in ["RQ"]:
    #     ax.set_xticklabels([])
    # else:
    ax.set_xticklabels(cfg_labels, fontsize=22, rotation=90)
    ax.set_ylabel(ylabel if op_key == "insert" else "", loc="top")
    ax.set_ylim(bottom=0)
    if yticks is not None:
        ax.set_yticks(yticks)
        if yticklabels is not None:
            ax.set_yticklabels(yticklabels)
    ax.text(0.4, 0.97, f"{op_key}", transform=ax.transAxes,
            va="top", ha="left", fontsize=20)
    ax.text(0.01, 0.96, r"$\times{}10^{6}$" if op_key == "insert" else r"$\times{}10^{3}$", transform=ax.transAxes,
            va="top", ha="left", fontsize=20)
    ax.text(0.99, 0.98, fig_label, transform=ax.transAxes,
            fontsize=20, va="top", ha="right")

    output_file = DROPBOX_PATH / output_name
    fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(fig)
    print(f"Saved: {output_file}")


def _read_levels(workload_log: Path) -> dict[int, int]:
    phases = parse_rocksdb_log(str(workload_log))
    if not phases:
        return {}
    return {lvl: d["size_bytes"]
            for lvl, d in phases[-1]["meta"].get("levels", {}).items()}



def compaction_debt_bytes(levels: dict[int, int]) -> float:
    """Sum of excess bytes above ideal for every level."""
    return sum(max(0, sz - _ideal_bytes(lvl)) for lvl, sz in levels.items())


def load_compaction_debt() -> dict[str, list[float]]:
    debt = {key: [] for key, _ in IMPLS}
    for cfg_dir, _ in CONFIGS:
        for style_key, subdir in IMPLS:
            wlog = EXP_DIR / cfg_dir / subdir / "workload.log"
            if wlog.exists():
                levels = _read_levels(wlog)
                debt[style_key].append(compaction_debt_bytes(levels) / 1e9)
            else:
                debt[style_key].append(0.0)
    return debt


def plot_compaction_debt():
    debt = load_compaction_debt()
    n_impls = len(IMPLS)
    x       = np.arange(n_impls)

    for cfg_idx, (cfg_dir, _) in enumerate(CONFIGS):
        fig, ax = plt.subplots(figsize=(0.8, 1.2))

        for i, (style_key, _) in enumerate(IMPLS):
            s = line_styles[style_key]
            ax.bar(i, debt[style_key][cfg_idx], 1.0,
                   color=s["color"] if style_key == "vector" else "none",
                   edgecolor=s["color"],
                   hatch=hatch_map[style_key], label=s["label"])

        ax.set_xticks(x)
        ax.set_xticklabels([])
        ax.tick_params(axis="x", length=0)
        # ax.set_ylabel("debt (GB)", labelpad=-0.5, loc="top")
        ax.set_ylim(bottom=0)
        ax.yaxis.set_major_formatter(
            plt.FuncFormatter(lambda v, _: "0" if v == 0 else f"{v:g}")
        )

        cfg_tag = cfg_dir.lower()
        output_file = DROPBOX_PATH / f"lowpri-wal-compaction-debt-{cfg_tag}.pdf"
        fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
        plt.close(fig)
        print(f"Saved: {output_file}")


def load_phase1_data() -> dict[str, list[float]]:
    data = {key: [] for key, _ in IMPLS}
    for cfg_dir, _ in CONFIGS:
        for style_key, subdir in IMPLS:
            wlog = EXP_DIR / cfg_dir / subdir / "workload.log"
            if wlog.exists():
                tp = read_phase1_insert_throughput(wlog)
            else:
                print(f"Missing: {wlog}")
                tp = 0.0
            data[style_key].append(tp)
    return data


def plot_phase1_insert():
    raw = load_phase1_data()
    data = {key: {"insert": vals} for key, vals in raw.items()}
    _plot_op(data, "insert", "insert throughput", "lowpri-wal-insert-phase1.pdf", "(A)")


def plot_all():
    data = load_data()
    _plot_op(data, "insert",  "throughput",  "lowpri-wal-insert.pdf",  "(A)", yticks=[0, 0.1, 0.2], yticklabels=["0", "0.1", "0.2"])
    _plot_op(data, "PQ",      "throughput",  "lowpri-wal-pq.pdf",      "(B)", yticks=[0, 1, 2, 3], yticklabels=["0", "1", "2", "3"])
    _plot_op(data, "RQ",      "throughput",  "lowpri-wal-rq.pdf",      "(C)", yticks=[0, 0.25, 0.5], yticklabels=["0", "0.2", "0.5"])
    _plot_op(data, "overall", "throughput",  "lowpri-wal-overall.pdf", "(D)", yticks=None)


def plot_legend():
    handles = []
    labels  = []
    for style_key, _ in IMPLS:
        s = line_styles[style_key]
        patch = mpatches.Patch(
            facecolor=s["color"] if style_key == "vector" else "none",
            edgecolor=s["color"],
            hatch=hatch_map[style_key],
            label=s["label"],
        )
        handles.append(patch)
        labels.append(s["label"])

    leg_fig, leg_ax = plt.subplots(figsize=(2, 0.2))
    leg_ax.axis("off")
    leg_ax.legend(handles, labels, frameon=False, ncol=4,
                  loc="center", labelspacing=0.1, handlelength=1.2,
                  columnspacing=2, handletextpad=0.3, fontsize=22)
    output_file = DROPBOX_PATH / "lowpri-wal-legend.pdf"
    leg_fig.savefig(output_file, bbox_inches="tight", pad_inches=0.01)
    plt.close(leg_fig)
    print(f"Saved: {output_file}")


BUFFER_SIZE_MB = 1
SIZE_RATIO     = 6

IMPL_ABBREV = {
    "vector":             line_styles["vector"]["label"],
    "unsortedvector":     line_styles["unsortedvector"]["label"],
    "alwayssortedvector": line_styles["alwayssortedvector"]["label"],
    "skiplist":           line_styles["skiplist"]["label"],
    "simpleskiplist":     line_styles["simpleskiplist"]["label"],
    "hashlinkedlist":     line_styles["hashlinkedlist"]["label"],
    "hashskiplist":       line_styles["hashskiplist"]["label"],
    "hashvector":         line_styles["hashvector"]["label"],
}

# Table-local ordering: simpleskiplist before skiplist
TABLE_IMPL_ORDER = [
    "vector", "unsortedvector", "alwayssortedvector",
    "simpleskiplist", "skiplist",
    "hashlinkedlist", "hashskiplist", "hashvector",
]



def _ideal_bytes(level: int) -> int:
    base = BUFFER_SIZE_MB * (1 << 20) * SIZE_RATIO  # L0 = 6MB
    return base * (SIZE_RATIO ** level)


def dump_level_excess():
    """Print average excess data (actual - ideal) in GB per level, per config."""
    # Collect per-config: level -> list of sizes across all impls
    for cfg_dir, cfg_label in CONFIGS:
        level_samples: dict[int, list[float]] = {}
        for _, subdir in IMPLS:
            wlog = EXP_DIR / cfg_dir / subdir / "workload.log"
            if not wlog.exists():
                continue
            for lvl, sz in _read_levels(wlog).items():
                level_samples.setdefault(lvl, []).append(sz)

        print(f"\n{cfg_dir} ({cfg_label.replace(chr(10), ' ')}):")
        print(f"  {'level':>6}  {'ideal (GB)':>10}  {'avg actual (GB)':>15}  {'avg excess (GB)':>15}")
        for lvl in sorted(level_samples):
            ideal_gb  = _ideal_bytes(lvl) / 1e9
            avg_gb    = float(np.mean(level_samples[lvl])) / 1e9
            excess_gb = max(0.0, avg_gb - ideal_gb)
            print(f"  {lvl:>6}  {ideal_gb:>10.3f}  {avg_gb:>15.3f}  {excess_gb:>15.3f}")


def level_table_latex():
    level_data: dict[str, dict[str, dict[int, int]]] = {
        key: {} for key, _ in IMPLS
    }
    for cfg_dir, _ in CONFIGS:
        for style_key, subdir in IMPLS:
            wlog = EXP_DIR / cfg_dir / subdir / "workload.log"
            if wlog.exists():
                phases = parse_rocksdb_log(str(wlog))
                levels = phases[-1]["meta"].get("levels", {}) if phases else {}
                level_data[style_key][cfg_dir] = {
                    lvl: d["size_bytes"] for lvl, d in levels.items()
                }
            else:
                level_data[style_key][cfg_dir] = {}

    impl_keys   = TABLE_IMPL_ORDER
    abbrevs     = [IMPL_ABBREV[k] for k in impl_keys]
    n_impl_cols = len(impl_keys)
    gap      = r"@{\hskip 4pt}"
    col_spec = "@{}l" + gap + "l" + (gap + "r") * n_impl_cols + "@{}"

    lines = []
    lines.append(r"\scriptsize")
    lines.append(r"\centering")
    lines.append(r"\setlength{\tabcolsep}{0.01pt}")
    lines.append(
        r"\captionof{table}{{\color{red}$\blacktriangle$}: excess above ideal; "
        r"{\color{green!60!black}$\blacktriangledown$}: remaining capacity.}"
    )
    # lines.append(r"\vspace{-2em}")
    lines.append(r"\begin{tabular}{" + col_spec + "}")
    lines.append(r"\toprule")

    header_impls = " & ".join(r"\textbf{\texttt{" + a + r"}}" for a in abbrevs)
    lines.append(r" & \textbf{level} & " + header_impls + r" \\")
    lines.append(r"\midrule")

    for cfg_idx, (cfg_dir, cfg_label) in enumerate(CONFIGS):
        cfg_tex = cfg_label.replace("\n", ", ")
        nonempty_levels = sorted({
            lvl for key in impl_keys
            for lvl, sz in level_data[key].get(cfg_dir, {}).items() if sz > 0
        })
        n_rows = len(nonempty_levels) if nonempty_levels else 1

        for row_i, lvl in enumerate(nonempty_levels):
            ideal = _ideal_bytes(lvl)
            cells = []
            for key in impl_keys:
                sz = level_data[key].get(cfg_dir, {}).get(lvl, 0)
                if sz == 0:
                    cells.append(r"\textemdash")
                elif sz > ideal * 1.10:
                    cells.append(rf"{{\color{{red}}$\blacktriangle$}} {sz / ideal:.2f}")
                elif sz < ideal:
                    cells.append(rf"{{\color{{green!60!black}}$\blacktriangledown$}} {sz / ideal:.2f}")
                else:
                    cells.append(rf"{sz / ideal:.2f}")

            cfg_cell = (
                r"\multirow{" + str(n_rows) + r"}{*}{\rotatebox{90}{\texttt{" +
                cfg_tex + r"}}}"
                if row_i == 0 else ""
            )
            lines.append(cfg_cell + " & L" + str(lvl) + " & " + " & ".join(cells) + r" \\")

        if cfg_idx < len(CONFIGS) - 1:
            lines.append(r"\midrule")

    lines.append(r"\bottomrule")
    lines.append(r"\end{tabular}")
    lines.append(r"\label{tab:level-sizes}")

    table_body = "\n".join(lines)

    tex_doc = r"""\documentclass{standalone}
\usepackage{booktabs}
\usepackage{multirow}
\usepackage{rotating}
\usepackage{xcolor}
\usepackage{amssymb}
\usepackage{caption}
\begin{document}
""" + table_body + r"""
\end{document}
"""

    import subprocess, tempfile, shutil
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        src = tmp / "table.tex"
        src.write_text(tex_doc)
        result = subprocess.run(
            ["pdflatex", "-interaction=nonstopmode", "table.tex"],
            cwd=tmp, capture_output=True, text=True
        )
        pdf_tmp = tmp / "table.pdf"
        if pdf_tmp.exists():
            output_pdf = DROPBOX_PATH / "lowpri-wal-level-table.pdf"
            shutil.copy(pdf_tmp, output_pdf)
            print(f"Saved: {output_pdf}")
        else:
            print("pdflatex failed:")
            print(result.stdout[-2000:])

    tex_file = DROPBOX_PATH / "lowpri-wal-level-table.tex"
    tex_file.write_text(table_body)
    print(f"Saved: {tex_file}")


def dump_csv():
    import csv

    # --- level sizes ---
    level_rows = []
    for cfg_dir, _ in CONFIGS:
        for style_key, subdir in IMPLS:
            wlog = EXP_DIR / cfg_dir / subdir / "workload.log"
            if not wlog.exists():
                continue
            phases = parse_rocksdb_log(str(wlog))
            levels = phases[-1]["meta"].get("levels", {}) if phases else {}
            for lvl, d in levels.items():
                sz = d["size_bytes"]
                ideal = _ideal_bytes(lvl)
                level_rows.append({
                    "config":     cfg_dir,
                    "impl":       style_key,
                    "level":      lvl,
                    "size_bytes": sz,
                    "size_gb":    f"{sz / 1e9:.4f}",
                    "ideal_gb":   f"{ideal / 1e9:.4f}",
                    "ratio":      f"{sz / ideal:.4f}" if ideal > 0 else "",
                })

    out = CURR_DIR / TAG / "lowpri-wal-level-sizes.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["config", "impl", "level",
                                          "size_bytes", "size_gb", "ideal_gb", "ratio"])
        w.writeheader()
        w.writerows(level_rows)
    print(f"Saved: {out}")

    # --- throughputs ---
    data = load_data()
    tp_rows = []
    for cfg_idx, (cfg_dir, _) in enumerate(CONFIGS):
        for style_key, _ in IMPLS:
            tp_rows.append({
                "config":        cfg_dir,
                "impl":          style_key,
                "insert_mops":   f"{data[style_key]['insert'][cfg_idx]:.6f}",
                "pq_kops":       f"{data[style_key]['PQ'][cfg_idx]:.6f}",
                "rq_kops":       f"{data[style_key]['RQ'][cfg_idx]:.6f}",
            })

    out = CURR_DIR / TAG / "lowpri-wal-throughput.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["config", "impl",
                                          "insert_mops", "pq_kops", "rq_kops"])
        w.writeheader()
        w.writerows(tp_rows)
    print(f"Saved: {out}")

    # --- compaction debt ---
    debt = load_compaction_debt()
    debt_rows = []
    for cfg_idx, (cfg_dir, _) in enumerate(CONFIGS):
        for style_key, _ in IMPLS:
            debt_rows.append({
                "config":    cfg_dir,
                "impl":      style_key,
                "debt_gb":   f"{debt[style_key][cfg_idx]:.4f}",
            })

    out = CURR_DIR / TAG / "lowpri-wal-compaction-debt.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["config", "impl", "debt_gb"])
        w.writeheader()
        w.writerows(debt_rows)
    print(f"Saved: {out}")


def plot_lsm_tree_states():
    COLOR_OVER  = "#c4a0a0"
    COLOR_UNDER = "#a0c4a4"
    CLIP_GB     = 2.6
    LEVEL_SPACING = 0.26
    BAR_H         = 0.15

    # Pre-load all configs so every figure uses the same fixed level set
    all_level_data: dict[str, dict[int, list[int]]] = {}
    all_levels: set[int] = set()
    for cfg_dir, _ in CONFIGS:
        level_sizes: dict[int, list[int]] = {}
        for _, subdir in IMPLS:
            wlog = EXP_DIR / cfg_dir / subdir / "workload.log"
            if not wlog.exists():
                continue
            for lvl, sz in _read_levels(wlog).items():
                if sz > 0:
                    level_sizes.setdefault(lvl, []).append(sz)
                    all_levels.add(lvl)
        all_level_data[cfg_dir] = level_sizes

    levels   = sorted(all_levels)
    n_levels = len(levels)
    fig_h    = n_levels * LEVEL_SPACING + 0.005

    for cfg_idx, (cfg_dir, _) in enumerate(CONFIGS):
        fig, ax = plt.subplots(figsize=(3, fig_h))
        level_sizes = all_level_data[cfg_dir]

        for i, lvl in enumerate(levels):
            y        = (n_levels - 1 - i) * LEVEL_SPACING
            ideal    = _ideal_bytes(lvl)
            ideal_gb = ideal / 1e9

            if lvl in level_sizes and level_sizes[lvl]:
                avg     = float(np.mean(level_sizes[lvl]))
                avg_gb  = avg / 1e9
                clipped = lvl == 4 and avg_gb > CLIP_GB

                within_gb = min(avg_gb, ideal_gb)
                over_gb   = max(0.0, avg_gb - ideal_gb)
                if clipped:
                    within_gb = min(within_gb, CLIP_GB)
                    over_gb   = max(0.0, min(CLIP_GB - within_gb, over_gb))

                ax.barh(y, within_gb, height=BAR_H, color=COLOR_UNDER, edgecolor="none")
                if over_gb > 0:
                    ax.barh(y, over_gb, left=within_gb, height=BAR_H,
                            color=COLOR_OVER, edgecolor="none")
                if clipped:
                    ax.text(within_gb + over_gb + 0.05, y, f"{avg_gb:.1f}GB",
                            va="center", ha="left", fontsize=7, color="black")

                ideal_draw_gb = min(ideal_gb, CLIP_GB) if clipped else ideal_gb
            else:
                ideal_draw_gb = min(ideal_gb, CLIP_GB) if lvl == 4 else ideal_gb

            ax.barh(y, ideal_draw_gb, height=BAR_H,
                    color="none", edgecolor="black", linewidth=0.5, linestyle="dashed")

        y_positions = [(n_levels - 1 - i) * LEVEL_SPACING for i in range(n_levels)]
        ax.set_yticks(y_positions)
        ax.set_yticklabels([f"" for lvl in levels], fontsize=12)
        ax.set_ylim(-BAR_H, (n_levels - 1) * LEVEL_SPACING + BAR_H)
        ax.set_xlim(left=0, right=CLIP_GB + 0.8)
        if cfg_idx < len(CONFIGS) - 1:
            ax.tick_params(axis="x", length=0, labelbottom=False)
        else:
            ax.tick_params(axis="x", labelsize=18)
            ax.set_xlabel("size (GB)", labelpad=1)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)

        cfg_tag = cfg_dir.lower()
        output_file = DROPBOX_PATH / f"lowpri-wal-tree-state-{cfg_tag}.pdf"
        fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
        plt.close(fig)
        print(f"Saved: {output_file}")


def plot_tree_legend():
    COLOR_OVER  = "#c4a0a0"
    handles = [
        mpatches.Patch(facecolor="none", edgecolor="black", linestyle="dashed",
                       linewidth=0.8, label="capacity"),
        mpatches.Patch(facecolor=COLOR_OVER, edgecolor="none", label="debt"),
    ]
    fig, ax = plt.subplots(figsize=(0.1, 0.1))
    ax.axis("off")
    leg_fig, leg_ax = plt.subplots(figsize=(2.5, 0.3))
    leg_ax.axis("off")
    leg_ax.legend(handles=handles, frameon=False, ncol=2,
                  loc="center", handlelength=1.2, handletextpad=0.4,
                  columnspacing=1.0, fontsize=18)
    plt.close(fig)
    output_file = DROPBOX_PATH / "lowpri-wal-tree-legend.pdf"
    leg_fig.savefig(output_file, bbox_inches="tight", pad_inches=0.02)
    plt.close(leg_fig)
    print(f"Saved: {output_file}")


if __name__ == "__main__":
    # plot_all()
    # plot_compaction_debt()
    # plot_lsm_tree_states()
    # dump_level_excess()
    # plot_tree_legend()
    level_table_latex()
    # plot_legend()
    # dump_csv()
