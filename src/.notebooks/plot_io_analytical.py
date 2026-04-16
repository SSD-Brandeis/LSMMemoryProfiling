import copy
import os
import numpy as np
import matplotlib.pyplot as plt

from plot import *
from plot.style import line_styles

TAG = "analytical_io"
os.makedirs(TAG, exist_ok=True)

# =========================
# Configurable parameters
# =========================
ptr_size = 8          # pointer size in bytes
E_raw = 1024          # average key-value pair size in bytes
E_internal = 12       # RocksDB internal overhead per entry (8B sequence number + 4B type/flags)
E = E_raw + E_internal  # total entry size = 1036 bytes
page_size = 4096      # disk page size in bytes
T = 10                # LSM-tree size ratio
N_total = 10_000_000  # total entries in workload

# --- Skip-list knobs (configurable) ---
l = 4                 # max skip-list height (capped)
p = 0.5               # promotion probability

# --- Hash-hybrid knobs ---
H = 256               # number of hash buckets
X = 4                 # prefix length
D_char = 128          # unique characters in key domain
Ke_denom = min(H, D_char ** X)

# =========================
# Expected forward pointers per node
#
# A node at level 0 always has 1 forward pointer.
# It is promoted to level i with probability p^i.
# Each level adds one forward pointer.
#
# E[fwd ptrs] = sum_{i=0}^{l-1} p^i = (1 - p^l) / (1 - p)
#
# For simple skip-list, a node also stores a key pointer (to the entry on heap).
# For inline skip-list, the key is stored inline (no key pointer).
# =========================
def expected_fwd_pointers(l, p):
    """Expected number of forward pointers per node for a skip-list capped at l levels."""
    return (1 - p**l) / (1 - p)

E_fwd = expected_fwd_pointers(l, p)
print(f"Skip-list parameters: l={l}, p={p}")
print(f"  E[forward pointers per node] = {E_fwd:.4f}")
print(f"  Simple skip-list per-node overhead = (1 key_ptr + {E_fwd:.3f} fwd_ptrs) * {ptr_size}B = {(1 + E_fwd) * ptr_size:.1f} B")
print(f"  Inline skip-list per-node overhead = {E_fwd:.3f} fwd_ptrs * {ptr_size}B = {E_fwd * ptr_size:.1f} B")
print()

# =========================
# Splice overhead (inline skip-list only)
#
# Splice is a SHARED structure (one per buffer, not per entry).
# It caches prev + next pointers for each level = 2*l pointers,
# plus 4 bytes of height metadata per level.
# =========================
splice_fixed = 2 * l * ptr_size + 4 * l
print(f"  Splice fixed overhead = {splice_fixed} bytes (2*{l}*{ptr_size} + 4*{l})")
print()

# =========================
# Buffer size axis: M in bytes
# =========================
M = np.linspace(1 * 1024 * 1024, 4 * 1024 * 1024, 500)  # 1 MB to 4 MB

# =========================
# Capacity models: K = entries that fit in buffer of M bytes
#
# General form:
#   K = (M - fixed_overhead) / (E + per_entry_overhead)
#
# Per-entry overhead = pointers/metadata stored WITH each entry.
# Fixed overhead = structures independent of entry count (splice, bucket array).
# =========================

# Per-entry pointer overhead for each buffer type
per_entry_overhead = {
    # Vector: 1 pointer on stack per entry
    "vector":           ptr_size,
    "unsortedvector":   ptr_size,
    "alwayssortedvector": ptr_size,

    # Linked-list: prev + next pointers per node
    "linkedlist":       2 * ptr_size,

    # Simple skip-list: 1 key pointer + E[fwd] forward pointers per node
    "simpleskiplist":   ptr_size * (1 + E_fwd),

    # Inline skip-list: NO key pointer (inline) + E[fwd] forward pointers per node
    "skiplist":         ptr_size * E_fwd,

    # Hash skip-list: within-bucket node = 1 key ptr + E[fwd] fwd ptrs
    "hashskiplist":     ptr_size * (1 + E_fwd),

    # Hash linked-list: within-bucket node = prev + next ptrs
    "hashlinkedlist":   2 * ptr_size,

    # Hash vector: within-bucket = 1 pointer per entry
    "hashvector":       ptr_size,
}

# Fixed overhead (independent of entry count)
hash_fixed = H * ptr_size  # bucket pointer array

fixed_overhead = {
    "vector":           0,
    "unsortedvector":   0,
    "alwayssortedvector": 0,
    "linkedlist":       0,
    "simpleskiplist":   0,
    "skiplist":         splice_fixed,
    "hashskiplist":     hash_fixed,
    "hashlinkedlist":   hash_fixed,
    "hashvector":       hash_fixed,
}

# Compute capacities
capacities = {}
for name in per_entry_overhead:
    usable = np.maximum(M - fixed_overhead[name], 0)
    capacities[name] = np.maximum(usable / (E + per_entry_overhead[name]), 1)

# =========================
# Derived I/O metrics
# =========================
entries_per_page = page_size / E

def pages_per_flush(cap):
    """Pages written per flush = ceil(K * E / page_size)."""
    return np.ceil(cap * E / page_size)

def num_flushes(cap):
    """Number of flushes to ingest N_total entries."""
    return np.ceil(N_total / cap)

def total_flush_ios(cap):
    """Total page I/Os from all flushes."""
    return num_flushes(cap) * pages_per_flush(cap)

def num_levels(cap):
    """Number of LSM-tree levels: L = ceil(log_T(N/K))."""
    return np.maximum(np.ceil(np.log(N_total / cap) / np.log(T)), 1)

def total_compaction_ios(cap):
    """Total compaction I/Os (leveled compaction).
    Each entry is merged ~T times per level, reading + writing one page per B entries.
    """
    L = num_levels(cap)
    return N_total * L * T * 2 / entries_per_page

def total_ios(cap):
    return total_flush_ios(cap) # + total_compaction_ios(cap)


# =========================
# Plot configuration
# =========================
plot_order = ["vector", "linkedlist", "simpleskiplist", "skiplist",
              "hashvector", "hashlinkedlist", "hashskiplist"]

all_order = ["vector", "unsortedvector", "alwayssortedvector", "linkedlist",
             "simpleskiplist", "skiplist", "hashskiplist", "hashlinkedlist", "hashvector"]

M_MB = M / (1024 * 1024)

# # =========================
# # Plot 1: Effective buffer capacity vs M
# # =========================
# fig, ax = plt.subplots(figsize=(5, 3.5))
# for name in plot_order:
#     ax.plot(M_MB, capacities[name], **line_styles[name])
# ax.set_xlabel("M (buffer size in MB)")
# ax.set_ylabel("effective capacity (entries)", loc="top")
# ax.ticklabel_format(axis='y', style='scientific', scilimits=(0, 0))
# plt.savefig(f"{TAG}/effective_capacity.pdf", bbox_inches="tight", pad_inches=0.06)

# # =========================
# # Plot 2: Number of flushes vs M
# # =========================
# fig, ax = plt.subplots(figsize=(5, 3.5))
# for name in plot_order:
#     ax.plot(M_MB, num_flushes(capacities[name]), **line_styles[name])
# ax.set_xlabel("M (buffer size in MB)")
# ax.set_ylabel("flush count", loc="top")
# ax.set_yscale("log")
# plt.savefig(f"{TAG}/io_flushes.pdf", bbox_inches="tight", pad_inches=0.06)

# # =========================
# # Plot 3: Pages per flush vs M
# # =========================
# fig, ax = plt.subplots(figsize=(5, 3.5))
# for name in plot_order:
#     ax.plot(M_MB, pages_per_flush(capacities[name]), **line_styles[name])
# ax.set_xlabel("M (buffer size in MB)")
# ax.set_ylabel("pages per flush", loc="top")
# plt.savefig(f"{TAG}/io_pages_per_flush.pdf", bbox_inches="tight", pad_inches=0.06)

# # =========================
# # Plot 4: Total flush I/Os vs M
# # =========================
# fig, ax = plt.subplots(figsize=(5, 3.5))
# for name in plot_order:
#     ax.plot(M_MB, total_flush_ios(capacities[name]), **line_styles[name])
# ax.set_xlabel("M (buffer size in MB)")
# ax.set_ylabel("total flush I/Os", loc="top")
# ax.set_yscale("log")
# plt.savefig(f"{TAG}/io_total_flush.pdf", bbox_inches="tight", pad_inches=0.06)

# # =========================
# # Plot 5: Total I/Os (flush + compaction) vs M
# # =========================
# fig, ax = plt.subplots(figsize=(5, 3.5))
# for name in plot_order:
#     ax.plot(M_MB, total_ios(capacities[name]), **line_styles[name])
# ax.set_xlabel("M (buffer size in MB)")
# ax.set_ylabel("total I/Os (flush + compaction)", loc="top")
# ax.set_yscale("log")
# plt.savefig(f"{TAG}/io_total.pdf", bbox_inches="tight", pad_inches=0.06)

# =========================
# Legend
# =========================
_, legend_fig = plt.subplots(figsize=(2, 0.7))
edited_lines = copy.deepcopy(line_styles)
del edited_lines["unsortedvector"]
del edited_lines["alwayssortedvector"]
order = ["vector", "linkedlist", "skiplist", "simpleskiplist",
         "hashlinkedlist", "hashskiplist", "hashvector"]
edited_lines = {name: edited_lines[name] for name in order}
for name, style in edited_lines.items():
    legend_fig.plot([], [], **style)
legend_fig.legend(loc="center", ncol=7, frameon=False)
legend_fig.axis("off")
plt.savefig(f"{TAG}/io_legend.pdf", bbox_inches="tight", pad_inches=0.06)

# =========================
# Reusable I/O computation for arbitrary (M, E_total, N) combinations
# =========================
def compute_ios_per_buffer(M_val, E_total, N, plot_names=None):
    """Compute total I/Os for each buffer at given M, E, and N.

    Returns dict: buffer_name -> total_ios (scalar or array matching N shape).
    """
    if plot_names is None:
        plot_names = plot_order

    ef = expected_fwd_pointers(l, p)
    epp = page_size / E_total  # entries per page

    overheads = {
        "vector":           ptr_size,
        "unsortedvector":   ptr_size,
        "alwayssortedvector": ptr_size,
        "linkedlist":       2 * ptr_size,
        "simpleskiplist":   ptr_size * (1 + ef),
        "skiplist":         ptr_size * ef,
        "hashskiplist":     ptr_size * (1 + ef),
        "hashlinkedlist":   2 * ptr_size,
        "hashvector":       ptr_size,
    }
    splice_f = 2 * l * ptr_size + 4 * l
    hash_f = H * ptr_size
    fixed = {
        "vector": 0, "unsortedvector": 0, "alwayssortedvector": 0,
        "linkedlist": 0, "simpleskiplist": 0,
        "skiplist": splice_f,
        "hashskiplist": hash_f, "hashlinkedlist": hash_f, "hashvector": hash_f,
    }

    result = {}
    for name in plot_names:
        cap = max((M_val - fixed[name]) / (E_total + overheads[name]), 1)
        fl = np.ceil(N / cap)                          # flushes
        ppf = np.ceil(cap * E_total / page_size)       # pages per flush
        flush_io = fl * ppf
        # L = np.maximum(np.ceil(np.log(np.maximum(N / cap, 1)) / np.log(T)), 1)
        # compact_io = N * L * T * 2 / epp
        result[name] = flush_io # + compact_io
    return result


# =========================
# Reusable: compute flush-only I/Os (where buffer choice matters)
# Compaction I/Os are nearly identical across buffers (same N, same levels),
# so we plot flush I/Os to highlight buffer-specific differences.
#
# We use continuous (non-ceil) versions to avoid staircase artifacts.
# =========================
def compute_flush_ios_per_buffer(M_val, E_total, N, plot_names=None):
    """Compute flush I/Os for each buffer. Uses continuous math (no ceil)."""
    if plot_names is None:
        plot_names = plot_order

    ef = expected_fwd_pointers(l, p)
    overheads = {
        "vector": ptr_size, "unsortedvector": ptr_size, "alwayssortedvector": ptr_size,
        "linkedlist": 2 * ptr_size,
        "simpleskiplist": ptr_size * (1 + ef), "skiplist": ptr_size * ef,
        "hashskiplist": ptr_size * (1 + ef), "hashlinkedlist": 2 * ptr_size, "hashvector": ptr_size,
    }
    splice_f = 2 * l * ptr_size + 4 * l
    hash_f = H * ptr_size
    fixed = {
        "vector": 0, "unsortedvector": 0, "alwayssortedvector": 0,
        "linkedlist": 0, "simpleskiplist": 0, "skiplist": splice_f,
        "hashskiplist": hash_f, "hashlinkedlist": hash_f, "hashvector": hash_f,
    }

    result = {}
    for name in plot_names:
        cap = np.maximum((M_val - fixed[name]) / (E_total + overheads[name]), 1)
        # Continuous: flushes = N / cap, pages_per_flush = cap * E / page_size
        # flush_ios = (N / cap) * (cap * E / page_size) = N * E / page_size
        # That cancels out! So we need the discrete version for flush I/Os.
        # Instead, just track flushes (the metric that differs across buffers).
        flushes = N / cap
        pages_per_flush = cap * E_total / page_size
        result[name] = {"flushes": flushes, "pages_per_flush": pages_per_flush,
                        "flush_ios": flushes * pages_per_flush}
    return result


# =========================
# Plot 6: Flush overhead ratio vs N (workload size), small entries
#
# Shows flushes(buffer) / flushes(vector) — the multiplicative overhead
# each buffer pays relative to the best-case (vector = minimum metadata).
# =========================
E_small_total = 16 + E_internal  # 28 bytes
M_plot = 5 * 1024 # * 1024         # 4 MB buffer

N_axis = np.logspace(6, 8, 500)  # 1K to 100M entries

fig, ax = plt.subplots(figsize=(3, 2.5))

data_n = compute_flush_ios_per_buffer(M_plot, E_small_total, N_axis)
# baseline = data_n["vector"]["flushes"]
for name in plot_order:
    ratio = data_n[name]["flushes"] # / baseline
    ax.plot(N_axis, ratio, **line_styles[name])

ax.set_xlabel("entries count")
ax.set_ylabel("flush I/Os")
ax.set_xscale("log")
# ax.set_yscale("log")
ax.set_ylim(0)
ax.set_yticks([])
ax.set_yticklabels([])
ax.set_xticks([])
ax.set_xticklabels([])

# --- Equation annotations for flush I/Os ---
# flush_ios = flushes * pages_per_flush = (N/K) * (K*E/P) = N*E/P
# But K differs per buffer, so discrete ceil versions diverge.

# # General formula at top
# ax.text(0.03, 0.97,
#         r"$\mathrm{flush\_IOs} = \left\lceil \frac{N}{K} \right\rceil"
#         r"\cdot \left\lceil \frac{K \cdot E}{P} \right\rceil$",
#         transform=ax.transAxes, fontsize=8, va="top", ha="left",
#         bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="gray", alpha=0.8))

# # Capacity formula
# ax.text(0.03, 0.72,
#         r"$K = \frac{M - F}{E + O}$",
#         transform=ax.transAxes, fontsize=8, va="top", ha="left",
#         bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="gray", alpha=0.8))

# Parameter box
# param_text = (f"$E = {E_small_total - E_internal}+{E_internal} = {E_small_total}$B, "
#               f"$P = {page_size}$B\n"
#               f"$M = {M_plot}$B, "
#               f"$p = {ptr_size}$B, $l = {l}$")
# ax.text(0.97, 0.97, param_text, transform=ax.transAxes,
#         fontsize=6.5, va="top", ha="right",
#         bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="gray", alpha=0.8))

plt.savefig(f"{TAG}/flush_ratio_vs_N.pdf", bbox_inches="tight", pad_inches=0.06)


# # =========================
# # Plot 7: Flush overhead ratio vs E (entry size), fixed M and N
# #
# # Log x-axis to expand the small-E region where per-entry overhead
# # is a significant fraction of entry size and differences are largest.
# # =========================
# N_plot = 10_000_000  # 10M entries

# E_axis = np.logspace(np.log10(4), np.log10(1024), 500)  # 4 B to 2048 B
# E_total_axis = E_axis + E_internal

# fig, ax = plt.subplots(figsize=(5, 3.5))

# # Compute all buffers first, then normalize to vector
# all_flushes = {}
# for name in plot_order:
#     flushes_arr = np.zeros_like(E_axis)
#     for i, et in enumerate(E_total_axis):
#         d = compute_flush_ios_per_buffer(M_plot, et, N_plot, [name])
#         flushes_arr[i] = d[name]["flushes"]
#     all_flushes[name] = flushes_arr

# baseline_e = all_flushes["vector"]
# for name in plot_order:
#     ratio = all_flushes[name] / baseline_e
#     ax.plot(E_axis, ratio, **line_styles[name])

# ax.set_xlabel("entry size")
# ax.set_ylabel("flush I/Os")
# ax.set_xscale("log")
# # ax.axhline(y=1.0, color='grey', linewidth=0.5, linestyle=':')

# plt.savefig(f"{TAG}/flush_ratio_vs_E.pdf", bbox_inches="tight", pad_inches=0.06)


# =========================
# Plot 7: Metadata overhead vs E (entry size)
#
# Per-entry overhead including amortized fixed costs (hash bucket array, splice).
# effective_overhead = per_entry_overhead + fixed_overhead / K
# where K = (M - fixed_overhead) / (E_total + per_entry_overhead)
# =========================
E_axis = np.logspace(np.log10(4), np.log10(1024), 500)
M_plot = 4 * 1024 * 1024  # 4 MB buffer

ef = expected_fwd_pointers(l, p)

per_entry_ov = {
    "vector":         ptr_size,
    "linkedlist":     2 * ptr_size,
    "simpleskiplist": ptr_size * (1 + ef),
    "skiplist":       ptr_size * ef,
    "hashskiplist":   ptr_size * (1 + ef),
    "hashlinkedlist": 2 * ptr_size,
    "hashvector":     ptr_size,
}

fixed_ov = {
    "vector":         0,
    "linkedlist":     0,
    "simpleskiplist": 0,
    "skiplist":       splice_fixed,              # 2*l*ptr_size + 4*l
    "hashskiplist":   100000 * ptr_size,              # bucket pointer array
    "hashlinkedlist": 100000 * ptr_size,
    "hashvector":     100000 * ptr_size,
}

fig, ax = plt.subplots(figsize=(3, 2.5))

for name in plot_order:
    ov = per_entry_ov[name]
    fix = fixed_ov[name]

    E_total_axis = E_axis + E_internal
    # Effective capacity at each entry size
    K = np.maximum((M_plot - fix) / (E_total_axis + ov), 1)
    # Amortized fixed overhead per entry + structural per-entry overhead
    effective_ov = ov + fix / K
    # As percentage of total entry size
    overhead_pct = effective_ov / E_total_axis * 100
    ax.plot(E_axis, overhead_pct, **line_styles[name])

ax.set_xlabel("entry size")
ax.set_ylabel("overhead (\\%)", loc="top")
ax.set_xscale("log")
ax.set_yscale("log")
ax.set_yticks([])
ax.set_yticklabels([])
ax.set_xticks([])
ax.set_xticklabels([])

# --- Equation annotations for metadata overhead vs E ---
# overhead% = (O + F/K) / E_total * 100
# where K = (M - F) / (E_total + O)

# # Main formula
# ax.text(0.97, 0.97,
#         r"$\mathrm{overhead} = \frac{O + F/K}{E} \times 100\%$",
#         transform=ax.transAxes, fontsize=8, va="top", ha="right",
#         bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="gray", alpha=0.8))

# # Annotate per-entry overhead O for key structures (right side, near curves at small E)
# # Use small-E end where curves are most spread out
# E_ann = E_axis[20]  # near left edge
# E_tot_ann = E_ann + E_internal

# # Vector: O = ptr_size
# K_v = max((M_plot - 0) / (E_tot_ann + ptr_size), 1)
# ov_v = ptr_size / E_tot_ann * 100
# ax.annotate(r"$O{=}p$", xy=(E_ann, ov_v),
#             fontsize=7, color="#006d2c", ha="right", va="top",
#             xytext=(-5, -2), textcoords="offset points")

# # Linked-list: O = 2p
# K_ll = max((M_plot - 0) / (E_tot_ann + 2*ptr_size), 1)
# ov_ll = 2 * ptr_size / E_tot_ann * 100
# ax.annotate(r"$O{=}2p$", xy=(E_ann, ov_ll),
#             fontsize=7, color="#222d8b", ha="right", va="bottom",
#             xytext=(-5, 2), textcoords="offset points")

# # Simple skip-list: O = (1+E[fwd])*p
# ov_ssl = ptr_size * (1 + ef) / E_tot_ann * 100
# ax.annotate(r"$O{=}(1{+}E[fwd]) \cdot p$", xy=(E_ann, ov_ssl),
#             fontsize=7, color="#cf17a7", ha="right", va="bottom",
#             xytext=(-5, 2), textcoords="offset points")

# # Inline skip-list: O = E[fwd]*p
# ov_isl = ptr_size * ef / E_tot_ann * 100
# ax.annotate(r"$O{=}E[fwd] \cdot p$", xy=(E_ann, ov_isl),
#             fontsize=7, color="#6a3d9a", ha="right", va="top",
#             xytext=(-5, -2), textcoords="offset points")

# # Hash hybrids: F = H*p  (note in a box)
# ax.text(0.97, 0.72,
#         r"Hash: $F = H \cdot p$" + f"\n$H = {H}$, " + r"$E[fwd] = \frac{1-p^l}{1-p}$",
#         transform=ax.transAxes, fontsize=7, va="top", ha="right",
#         bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="gray", alpha=0.8))

# # Parameter box
# param_text = (f"$M = {M_plot // (1024*1024)}$MB, "
#               f"$p = {ptr_size}$B\n"
#               f"$l = {l}$, prob $= {p}$, $H = {H}$")
# ax.text(0.03, 0.03, param_text, transform=ax.transAxes,
#         fontsize=6.5, va="bottom", ha="left",
#         bbox=dict(boxstyle="round,pad=0.3", facecolor="white", edgecolor="gray", alpha=0.8))

plt.savefig(f"{TAG}/metadata_overhead_vs_E.pdf", bbox_inches="tight", pad_inches=0.06)

# =========================
# Summary table at fixed M
# =========================
M_fixed = 4 * 1024 * 1024  # 4 MB

def print_summary(M_val, E_val, E_internal_val, label=""):
    """Print summary table for a given M and E configuration."""
    E_total = E_val + E_internal_val
    epp = page_size / E_total  # entries per page

    if label:
        print(f"\n{'='*90}")
        print(f"{label}")
    print(f"{'='*90}")
    print(f"M = {M_val // (1024*1024)} MB, E_raw = {E_val} B, E_internal = {E_internal_val} B, "
          f"E_total = {E_total} B, page = {page_size} B, N = {N_total:,}, l = {l}, p = {p}")
    print(f"{'='*90}")
    print(f"{'Buffer':<22} {'Overhead/entry':>14} {'Capacity':>10} {'Flushes':>10} "
          f"{'Pages/flush':>12} {'Flush I/Os':>12} {'Levels':>8} {'Total I/Os':>14}")
    print(f"{'-'*22} {'-'*14} {'-'*10} {'-'*10} {'-'*12} {'-'*12} {'-'*8} {'-'*14}")

    # Recompute per-entry overhead with this E_total
    ef = expected_fwd_pointers(l, p)
    overheads = {
        "vector":           ptr_size,
        "unsortedvector":   ptr_size,
        "alwayssortedvector": ptr_size,
        "linkedlist":       2 * ptr_size,
        "simpleskiplist":   ptr_size * (1 + ef),
        "skiplist":         ptr_size * ef,
        "hashskiplist":     ptr_size * (1 + ef),
        "hashlinkedlist":   2 * ptr_size,
        "hashvector":       ptr_size,
    }
    fixed = {
        "vector": 0, "unsortedvector": 0, "alwayssortedvector": 0,
        "linkedlist": 0, "simpleskiplist": 0,
        "skiplist": splice_fixed,
        "hashskiplist": hash_fixed, "hashlinkedlist": hash_fixed, "hashvector": hash_fixed,
    }

    for name in all_order:
        ov = overheads[name]
        fix = fixed[name]
        cap = max((M_val - fix) / (E_total + ov), 1)
        fl = int(np.ceil(N_total / cap))
        ppf = int(np.ceil(cap * E_total / page_size))
        fio = fl * ppf
        L = max(int(np.ceil(np.log(N_total / cap) / np.log(T))), 1)
        cio = int(N_total * L * T * 2 / epp)
        tio = fio + cio

        print(f"{name:<22} {ov:>12.1f} B {int(cap):>10,} {fl:>10,} "
              f"{ppf:>12,} {fio:>12,} {L:>8} {tio:>14,}")

# --- Large entries (E = 1024 + 12) ---
print_summary(M_fixed, 1024, 12, "LARGE ENTRIES")

# --- Medium entries (E = 128 + 12) ---
print_summary(M_fixed, 128, 12, "MEDIUM ENTRIES")

# --- Small entries (E = 16 + 12) ---
print_summary(M_fixed, 16, 12, "SMALL ENTRIES")

# =========================
# Show how l affects capacity for skip-list buffers (small entries)
# =========================
E_small = 16 + 12  # 28 bytes
print(f"\n{'='*70}")
print(f"Sensitivity of skip-list capacity to l (max height), p = {p}")
print(f"E = {E_small} B, M = {M_fixed // (1024*1024)} MB")
print(f"{'='*70}")
print(f"{'l':>4} {'E[fwd]':>8} {'overhead':>10} {'simple skip':>14} {'inline skip':>14} {'hash skip':>14}")
print(f"{'':>4} {'':>8} {'(bytes)':>10} {'(entries)':>14} {'(entries)':>14} {'(entries)':>14}")
print(f"{'-'*4} {'-'*8} {'-'*10} {'-'*14} {'-'*14} {'-'*14}")

for l_test in [1, 2, 3, 4, 5, 6, 8]:
    ef = expected_fwd_pointers(l_test, p)
    splice_test = 2 * l_test * ptr_size + 4 * l_test
    ov_simple = ptr_size * (1 + ef)

    cap_simple = M_fixed / (E_small + ptr_size * (1 + ef))
    cap_inline = (M_fixed - splice_test) / (E_small + ptr_size * ef)
    cap_hskip = max(M_fixed - hash_fixed, 0) / (E_small + ptr_size * (1 + ef))

    print(f"{l_test:>4} {ef:>8.3f} {ov_simple:>10.1f} {int(cap_simple):>14,} {int(cap_inline):>14,} {int(cap_hskip):>14,}")
