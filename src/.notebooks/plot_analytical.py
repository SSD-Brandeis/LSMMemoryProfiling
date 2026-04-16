import copy
import os
import numpy as np
import matplotlib.pyplot as plt

from plot import *
from plot.style import line_styles

TAG = "analytical"
os.makedirs(TAG, exist_ok=True)

# -------------------------
# Parameters
# -------------------------
ptr_size = 8
E = 1024             # average entry size in bytes

# Hash-hybrid parameters
H = 256              # number of hash buckets
X = 4                # prefix length
D = 128              # unique chars in key domain
Ke_denom = min(H, D ** X)

# Inline skip-list splice distance as fraction of K
D_FRACTION = 0.05

# Buffer size axis: K (entries that fit in the buffer)
K = np.linspace(500, 100000, 500)
K_e = np.maximum(K / Ke_denom, 1)

# Distance for inline skip-list splice
D_splice = np.maximum(K * D_FRACTION, 2)

# -------------------------
# Per-operation cost models (from Table 2)
# Each function returns the cost of ONE operation given K entries in buffer.
#
# Random memory access penalty:
#   Pointer-chasing structures (linked-list, skip-list) incur cache misses
#   on every hop. Contiguous-memory structures (vector) are cache-friendly.
#   We model this with a multiplier: c_rnd for random access, c_seq for sequential.
# -------------------------
c_seq = 1    # sequential/contiguous memory access (array ops, cache-friendly)
c_rnd = 10   # random pointer-chasing access (cache misses per hop)

costs = {
    "vector": {
        "put":       lambda K: c_seq * np.ones_like(K),
        "get":       lambda K: c_seq * K * np.log2(K),         # snapshot + sort + bsearch
        "shortscan": lambda K: c_seq * K * np.log2(K),         # same as get: sort snapshot, bsearch, then seq scan
    },
    "unsortedvector": {
        "put":       lambda K: c_seq * np.ones_like(K),
        "get":       lambda K: c_seq * K,                       # reverse linear scan
        "shortscan": lambda K: c_seq * K * np.log2(K),         # must sort for ordered scan
    },
    "alwayssortedvector": {
        "put":       lambda K: c_seq * K,                       # bsearch + shift
        "get":       lambda K: c_seq * K,                       # snapshot + bsearch
        "shortscan": lambda K: c_seq * K,                       # snapshot + bsearch + seq read
    },
    "linkedlist": {
        "put":       lambda K: c_rnd * K,                       # linear traversal, pointer-chasing
        "get":       lambda K: c_rnd * K,
        "shortscan": lambda K: c_rnd * K,                       # traverse to start + follow next ptrs
    },
    "simpleskiplist": {
        "put":       lambda K: c_rnd * np.log2(K),
        "get":       lambda K: c_rnd * np.log2(K),
        "shortscan": lambda K: c_rnd * np.log2(K),             # search to start + follow level-0
    },
    "skiplist": {
        "put":       lambda K: c_rnd * np.log2(np.maximum(K * D_FRACTION, 2)),
        "get":       lambda K: c_rnd * np.log2(np.maximum(K * D_FRACTION, 2)),
        "shortscan": lambda K: c_rnd * np.log2(np.maximum(K * D_FRACTION, 2)),
    },
    "hashskiplist": {
        "put":       lambda K: c_rnd * np.log2(np.maximum(K / Ke_denom, 1)),
        "get":       lambda K: c_rnd * np.log2(np.maximum(K / Ke_denom, 1)),
        "shortscan": lambda K: c_rnd * np.log2(np.maximum(K / Ke_denom, 1)),  # within-bucket scan
    },
    "hashlinkedlist": {
        "put":       lambda K: c_rnd * np.maximum(K / Ke_denom, 1),
        "get":       lambda K: c_rnd * np.maximum(K / Ke_denom, 1),
        "shortscan": lambda K: c_rnd * np.maximum(K / Ke_denom, 1),            # linear scan within linked-list bucket
    },
    "hashvector": {
        "put":       lambda K: c_seq * np.maximum(K / Ke_denom, 1),
        "get":       lambda K: c_seq * np.log2(np.maximum(K / Ke_denom, 1)),
        "shortscan": lambda K: c_seq * np.log2(np.maximum(K / Ke_denom, 1)),  # bsearch in contiguous bucket
    },
}

# -------------------------
# Phased workload model
#
# A workload consists of multiple phases, each with a fixed number of
# put and get operations. The total execution time is:
# Workload parameterization:
#   N = total number of operations
#   r = read ratio (fraction that are reads vs writes)
#   q = scan ratio (fraction of reads that are short-scans vs point gets)
#   s = average selectivity per scan (qualifying keys returned)
#
#   puts  = N * (1 - r)
#   gets  = N * r * (1 - q)
#   scans = N * r * q
#
# Total cost per phase:
#   N*(1-r)*put(K) + N*r*(1-q)*get(K) + N*r*q*(scan(K) + c_seq*s)
# -------------------------

N_ops = 1_000_000_000  # total operations per phase (tunable)
s_default = 50         # default scan selectivity

workloads = {
    "write-heavy": [
        {"N": N_ops, "r": 0.00,    "q": 0.0, "s": 0},
        {"N": N_ops, "r": 0.00001, "q": 0.0, "s": 0},
    ],
    "balanced": [
        {"N": N_ops, "r": 0.20, "q": 0.10, "s": s_default},
        {"N": N_ops, "r": 0.50, "q": 0.10, "s": s_default},
    ],
    "read-heavy": [
        {"N": N_ops, "r": 0.80, "q": 0.20, "s": s_default},
        {"N": N_ops, "r": 0.95, "q": 0.20, "s": s_default},
    ],
}

def total_workload_cost(K, buf_name, phases):
    put_fn = costs[buf_name]["put"]
    get_fn = costs[buf_name]["get"]
    scan_fn = costs[buf_name]["shortscan"]
    total = np.zeros_like(K)
    for phase in phases:
        n, r, q, s = phase["N"], phase["r"], phase["q"], phase["s"]
        total += (n * (1 - r) * put_fn(K)
                + n * r * (1 - q) * get_fn(K)
                + n * r * q * (scan_fn(K) + c_seq * s))
    return total

# -------------------------
# Plot: one subplot per workload type
# -------------------------
fig, axes = plt.subplots(1, 3, figsize=(15, 4), sharey=False)

buf_order = ["vector", "unsortedvector", "alwayssortedvector", "linkedlist",
             "simpleskiplist", "skiplist", "hashskiplist", "hashlinkedlist", "hashvector"]

for ax, (wl_name, phases) in zip(axes, workloads.items()):
    for buf_name in buf_order:
        total = total_workload_cost(K, buf_name, phases)
        ax.plot(K, total, **line_styles[buf_name])

    # Build subtitle showing phase ratios
    ratios = ", ".join(f"r={p['r']}" + (f"/q={p['q']}" if p['q'] > 0 else "") for p in phases)
    ax.set_xlabel("K (buffer entries)")
    ax.set_ylabel("total workload cost")
    ax.set_title(f"{wl_name} ({ratios})")
    ax.set_yscale("log")
    ax.set_ylim(1e0)

fig.tight_layout()
plt.savefig(f"{TAG}/workload_cost.pdf", bbox_inches="tight", pad_inches=0.06)

# -------------------------
# Legend (all 9 buffers)
# -------------------------
_, legend_fig = plt.subplots(figsize=(2, 0.7))

legend_lines = {name: line_styles[name] for name in buf_order}
for name, style in legend_lines.items():
    legend_fig.plot([], [], **style)
legend_fig.legend(loc="center", ncol=5, frameon=False)
legend_fig.axis("off")
plt.savefig(f"{TAG}/workload_cost_legend.pdf", bbox_inches="tight", pad_inches=0.06)

# -------------------------
# LaTeX table: total workload cost at a fixed K for 4 workloads
# -------------------------
K_fixed = 50000  # representative buffer size
K_arr = np.array([K_fixed], dtype=float)
K_e_fixed = max(K_fixed / Ke_denom, 1)
D_splice_fixed = max(K_fixed * D_FRACTION, 2)

table_workloads = {
    "write-only\n($r{=}0$)":                       [{"N": N_ops, "r": 0.00, "q": 0.0,  "s": 0}],
    "write-heavy\n($r{=}0.05,~q{=}0.1$)":          [{"N": N_ops, "r": 0.05, "q": 0.10, "s": s_default}],
    "balanced\n($r{=}0.50,~q{=}0.1$)":              [{"N": N_ops, "r": 0.50, "q": 0.10, "s": s_default}],
    "read-heavy\n($r{=}0.95,~q{=}0.2$)":            [{"N": N_ops, "r": 0.95, "q": 0.20, "s": s_default}],
}

buf_labels = {
    "vector":             "Standard vector",
    "unsortedvector":     "Unsorted vector",
    "alwayssortedvector": "Sorted vector",
    "linkedlist":         "Linked-list",
    "simpleskiplist":     "Simple skip-list",
    "skiplist":           "Inline skip-list",
    "hashskiplist":       "Hash skip-list",
    "hashlinkedlist":     "Hash linked-list",
    "hashvector":         "Hash vector",
}

def fmt(val):
    """Format large numbers in scientific notation for LaTeX."""
    if val < 1e3:
        return f"{val:.0f}"
    exp = int(np.floor(np.log10(val)))
    mantissa = val / 10**exp
    return f"${mantissa:.1f} \\times 10^{{{exp}}}$"

# Compute table
print("\n% --- LaTeX table: workload cost at K = {:,} ---".format(K_fixed))
print(f"% c_seq = {c_seq}, c_rnd = {c_rnd}, N = {N_ops:.0e}, H = {H}, X = {X}")
print(r"\begin{table}[t]")
print(r"\small")
print(r"\centering")
print(r"\caption{Total workload cost per buffer at $K{=}" + f"{K_fixed:,}" + r"$ ($c_{{seq}}{=}" + str(c_seq) + r"$, $c_{{rnd}}{=}" + str(c_rnd) + r"$)}")
print(r"\vspace{-1em}")
ncols = len(table_workloads)
print(r"\begin{tabular}{@{}l" + "r" * ncols + r"@{}}")
print(r"\toprule")

# Header
header = r"\textbf{Buffer}"
for wl_name in table_workloads:
    clean = wl_name.replace("\n", " ")
    header += f" & \\textbf{{{clean}}}"
header += r" \\"
print(header)
print(r"\midrule")

# Rows
for buf_name in buf_order:
    row = buf_labels[buf_name]
    for wl_name, phases in table_workloads.items():
        put_fn = costs[buf_name]["put"]
        get_fn = costs[buf_name]["get"]
        scan_fn = costs[buf_name]["shortscan"]
        total = 0.0
        for phase in phases:
            n, r, q, s = phase["N"], phase["r"], phase["q"], phase["s"]
            put_cost = float(put_fn(K_arr)[0])
            get_cost = float(get_fn(K_arr)[0])
            scan_cost = float(scan_fn(K_arr)[0])
            total += (n * (1 - r) * put_cost
                    + n * r * (1 - q) * get_cost
                    + n * r * q * (scan_cost + c_seq * s))
        row += f" & {fmt(total)}"
    row += r" \\"
    print(row)

print(r"\bottomrule")
print(r"\end{tabular}")
print(r"\end{table}")
