#
# style.py
#

# --- ⬇️ START: CORRECTED bar_styles ⬇️ ---
bar_styles = {
    "hash_linked_list": {
        "label": "hash linked list",
        "facecolor": "white",
        "edgecolor": "#b22222",
        "hatch": "//",
        "linewidth": 1,
    },
    "hash_linked_list_optimized": {
        "label": "hash linked list optimized",
        "facecolor": "white",
        "edgecolor": "#d62728",
        "hatch": "//",
        "linewidth": 1,
    },
    "hash_skip_list": {
        "label": "hash skip list",
        "facecolor": "white",
        "edgecolor": "#1f78b4",
        "hatch": "\\\\",
        "linewidth": 1,
    },
    "skiplist": {
        "label": "skip list",
        "facecolor": "white",
        "edgecolor": "#6a3d9a",
        "hatch": "--",
        "linewidth": 1,
    },
    
    # --- Solid Fill Styles (as seen in your fallback) ---
    "AlwaysSortedVector": {
        "label": "always-sorted vector",
        "facecolor": "gray",    # <-- Solid fill
        # "edgecolor": "#8b4513", # <-- Brown edge
        "hatch": None,          # <-- No hatch
        "linewidth": 1,
    },
    "AlwaysSortedVector-dynamic": {
        "label": "always-sorted vector dynamic",
        "facecolor": "gray",    # <-- Solid fill
        # "edgecolor": "#8b4513", # <-- Brown edge
        "hatch": None,          # <-- No hatch
        "linewidth": 1,
    },
    "UnsortedVector": {
        "label": "unsorted vector",
        "facecolor": "black",   # <-- Solid fill
        # "edgecolor": "black",   # <-- Black edge
        "hatch": None,          # <-- No hatch
        "linewidth": 1,
    },
    "UnsortedVector-dynamic": {
        "label": "unsorted vector dynamic",
        "facecolor": "black",   # <-- Solid fill
        # "edgecolor": "black",   # <-- Black edge
        "hatch": None,          # <-- No hatch
        "linewidth": 1,
    },
    
    # --- ⬇️ MODIFIED: Solid Green Fill ---
    "Vector": {
        "label": "vector",
        "facecolor": "#006d2c",  # <-- Solid Green Fill
        # "edgecolor": "#006d2c",  # <-- Green Edge
        "hatch": None,           # <-- No Hatch
        "linewidth": 1,
    },
    "Vector-dynamic": {
        "label": "vector dynamic",
        "facecolor": "#006d2c",  # <-- Solid Green Fill
        # "edgecolor": "#006d2c",  # <-- Green Edge
        "hatch": None,           # <-- No Hatch
        "linewidth": 1,
    },
}
# --- ⬆️ END: CORRECTED bar_styles ⬆️ ---


# --- line_styles as provided by you ---
line_styles = {
    "Vector": {
        "label": "vector",
        "color": "#006d2c",  # Dark green (distinct and rich)
        "linestyle": (0, (1, 1)),  # Dotted
        "marker": "x",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 1,
    },
    "skiplist": {
        "label": "skip list",
        "color": "#6a3d9a",  # Deep purple
        "linestyle": "-",
        "marker": "o",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 1,
    },
    "hash_skip_list": {
        "label": "hash skip list",
        "color": "#1f78b4",  # Darker blue (colorblind-friendly)
        "linestyle": "--",
        "marker": "^",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 1,
    },
    "hash_linked_list": {
        "label": "hash linked list",
        "color": "#b22222",  # Firebrick (dark red)
        "linestyle": "-.",
        "marker": "D",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 1,
    },
    "hash_linked_list_optimized": {
        "label": "hash linked list optimized",
        "color": "#d62728",
        "linestyle": "-",
        "marker": "s",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 1,
    },
    "AlwaysSortedVector": {
        "label": "always sorted vector",
        "color": "#8b4513",  # SaddleBrown (dark reddish-brown)
        "linestyle": (0, (3, 1, 1, 1)),  # Dash-dot-dot
        "marker": "s",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 1,
    },
    "UnsortedVector": {
        "label": "unsorted vector",
        "color": "#4d4d4d",  # Dark gray (visible and clean)
        "linestyle": (0, (5, 2)),  # Long dashes
        "marker": "v",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 1,
    },
    # writestall
    "vectorrep": {
        "label": "vectorrep",  # Label for the legend
        "color": "#006d2c",
        "linestyle": "-", 
        "marker": "", 
        "markersize": 0,
        "markerfacecolor": "none",
        "linewidth": 1,
    },
    # Add new styles for the other write stall components
    "Vector-dynamic": {
        "label": "vector dynamic",
        "color": "#006d2c", 
        "linestyle": (0, (1, 1)),
        "marker": "x",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 1,
    },
    "Vector-preallocated": {
        "label": "vector preallocated",
        "color": "#ff7f00", # Example: Orange
        "linestyle": "-",
        "marker": "+",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 1,
    },
}