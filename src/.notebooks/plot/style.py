bar_styles = {
    "hash_linked_list":               {"label": "Hash linked list",               "color": "None", "edgecolor": "black", "hatch": "////"},
    "hash_linked_list-X6-H100000":    {"label": "Hash linked list X6 H100k",      "color": "None", "edgecolor": "black", "hatch": "////"},
    "hash_skip_list":                 {"label": "Hash skip list",                 "color": "None", "edgecolor": "black", "hatch": "\\\\"},
    "hash_skip_list-X6-H100000":      {"label": "Hash skip list X6 H100k",        "color": "None", "edgecolor": "black", "hatch": "\\\\"},
    "skiplist":                       {"label": "inline Skip-list",                      "color": "None", "edgecolor": "black", "hatch": "++++"},
    "AlwaysSortedVector":             {"label": "Always-sorted vector",           "color": "None", "edgecolor": "black", "hatch": "...."},
    "AlwaysSortedVector-dynamic":     {"label": "Always-sorted vector dynamic",       "color": "None", "edgecolor": "black", "hatch": "...."},
    "UnsortedVector":                 {"label": "Unsorted vector",                "color": "None", "edgecolor": "black", "hatch": "xxxx"},
    "UnsortedVector-dynamic":         {"label": "Unsorted vector dynamic",            "color": "None", "edgecolor": "black", "hatch": "xxxx"},
    "Vector":                         {"label": "Vector",                         "color": "None", "edgecolor": "black", "hatch": "----"},
    "Vector-dynamic":                 {"label": "Vector dynamic",                     "color": "None", "edgecolor": "black", "hatch": "----"},
}


line_styles = {
    "vector": {
        "label": "vector",
        "color": "#006d2c",  # Dark green (distinct and rich)
        "linestyle": (0, (1, 1)),  # Dotted
        "marker": "x",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "unsortedvector": {
        "label": "unsorted vector",
        "color": "#4d4d4d",  # Dark gray (visible and clean)
        "linestyle": (0, (5, 2)),  # Long dashes
        "marker": "v",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "alwayssortedvector": {
        "label": "sorted vector",
        "color": "#8b4513",  # SaddleBrown (dark reddish-brown)
        "linestyle": (0, (3, 1, 1, 1)),  # Dash-dot-dot
        "marker": "s",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "skiplist": {
        "label": "inline skip-list",
        "color": "#6a3d9a",  # Deep purple
        "linestyle": "-",
        "marker": "o",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "simpleskiplist": {
        "label": "skip-list",
        "color": "#cf17a7",  # Teal (distinct from purple & blue)
        "linestyle": ":",    # Dotted (different from skiplist solid)
        "marker": "P",       # Plus-filled marker (distinct)
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "hashlinkedlist": {
        "label": "hash linked-list",
        "color": "#b22222",  # Firebrick (dark red)
        "linestyle": "-.",
        "marker": "D",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "hashskiplist": {
        "label": "hash skip-list",
        "color": "#1f78b4",  # Darker blue (colorblind-friendly)
        "linestyle": "--",
        "marker": "^",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "hashvector": {
        "label": "hash vector",
        "color": "#ff7f0e",  # Orange (distinct from others)
        "linestyle": "-",
        "marker": "H",       # Hexagon marker (distinct)
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
}


hatch_map = {
    "vector": "",
    "unsortedvector": "\\",
    "alwayssortedvector": "/",
    "skiplist": "-",
    "simpleskiplist": "---",
    "hashlinkedlist": "///",
    "hashskiplist": "\\\\\\",
    "hashvector": "....",
}