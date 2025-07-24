# For bar plots: Only parameters that ax.bar() accepts
bar_styles = {
    "YCSB": {
        "label": "YCSB",
        "color": "None",        # Transparent fill for bar interiors
        "edgecolor": "grey",  # Academic-friendly blue
        "hatch": "///",            # No hatch pattern
        # "color": "grey",
    },
    "KVBench": {
        "label": "KVBench",
        "color": "None",
        "edgecolor": "tab:blue",  # Muted purple
        "hatch": "//",
    },
    "Tectonic": {
        "label": "Tectonic",
        "color": "tab:red",
        "edgecolor": "tab:red",  # Warm, muted orange
        "hatch": "",
    }
}

# ── LSMMemoryBuffer
bar_styles.update({
    "hash_linked_list": {
        "label": "Hash linked list",
        "color": "None",
        "edgecolor": "black",
        "hatch": "///",
    },
    "hash_skip_list": {
        "label": "Hash skip list",
        "color": "None",
        "edgecolor": "tab:red",
        "hatch": "//",
    },
    "skiplist": {
        "label": "Skip-list",
        "color": "None",
        "edgecolor": "tab:blue",
        "hatch": "",
    },
    "AlwayssortedVector": {
        "label": "Always-sorted vector",
        "color": "None",
        "edgecolor": "tab:orange",
        "hatch": "..",
    },
    "UnsortedVector": {
        "label": "Unsorted vector",
        "color": "None",
        "edgecolor": "tab:brown",
        "hatch": "\\\\",
    },
    "Vector": {
        "label": "Vector",
        "color": "None",
        "edgecolor": "tab:green",
        "hatch": "++",
    },
})


# For line plots: Only parameters that ax.plot() accepts
line_styles = {
    "YCSB": {
        "label": "YCSB",
        "color": "black",
        "linestyle": "-",      # Solid line
        "marker": "^",         # Circle marker
        "markersize": 12,
        "markerfacecolor": "none",
    },
    "KVBench": {
        "label": "KVBench",
        "color": "tab:blue",
        "linestyle": "--",     # Dashed line (or any preferred style)
        "marker": "v",         # Triangle marker (downward)
        "markersize": 12,
        "markerfacecolor": "none",
    },
    "Tectonic": {
        "label": "Tectonic",
        "color": "tab:red",
        "linestyle": "-.",     # Dashed line
        "marker": "s",         # Square marker
        "markersize": 12,
        "markerfacecolor": "none",
    }
}

# ── LSMmemoryBuffer 
line_styles.update({
    "hash_linked_list": {
        "label": "Hash linked list",
        "color": "black",
        "linestyle": ":",
        "marker": "D",
        "markersize": 8,
        "markerfacecolor": "none",
    },
    "hash_skip_list": {
        "label": "Hash skip list",
        "color": "tab:red",
        "linestyle": "--",
        "marker": "^",
        "markersize": 8,
        "markerfacecolor": "none",
    },
    "skiplist": {
        "label": "Skip-list",
        "color": "tab:blue",
        "linestyle": "-",
        "marker": "o",
        "markersize": 8,
        "markerfacecolor": "none",
    },
    "AlwayssortedVector": {
        "label": "Always-sorted vector",
        "color": "tab:orange",
        "linestyle": "-",
        "marker": "o",
        "markersize": 8,
        "markerfacecolor": "none",
    },
    "UnsortedVector": {
        "label": "Unsorted vector",
        "color": "tab:brown",
        "linestyle": "--",
        "marker": "s",
        "markersize": 8,
        "markerfacecolor": "none",
    },
    "Vector": {
        "label": "Vector",
        "color": "tab:green",
        "linestyle": "-.",
        "marker": "D",
        "markersize": 8,
        "markerfacecolor": "none",
    },
})
# ─────────────────────────────────────────────────────────────────────────────
