bar_styles = {
    # --- Vector (Solid) ---
    "Vector":                         {"label": "vector",                         "color": "#006d2c", "edgecolor": "#006d2c", "hatch": ""},
    "vector-preallocated":            {"label": "vector",                         "color": "#006d2c", "edgecolor": "#006d2c", "hatch": ""},
    "Vector-dynamic":                 {"label": "vector dynamic",                 "color": "#006d2c", "edgecolor": "#006d2c", "hatch": ""},

    # --- Unsorted Vector ---
    "UnsortedVector":                 {"label": "unsorted vector",                "color": "None", "edgecolor": "#4d4d4d", "hatch": "\\\\"},
    "unsortedvector-preallocated":    {"label": "unsorted vector",                "color": "None", "edgecolor": "#4d4d4d", "hatch": "\\\\"},
    "UnsortedVector-dynamic":         {"label": "unsorted vector dynamic",        "color": "None", "edgecolor": "#4d4d4d", "hatch": "\\\\"},

    # --- Sorted Vector ---
    "AlwaysSortedVector":             {"label": "sorted vector",                  "color": "None", "edgecolor": "#8b4513", "hatch": "/"},
    "sortedvector-preallocated":      {"label": "sorted vector",                  "color": "None", "edgecolor": "#8b4513", "hatch": "/"},
    "AlwaysSortedVector-dynamic":     {"label": "sorted vector dynamic",          "color": "None", "edgecolor": "#8b4513", "hatch": "/"},

    # --- Skip Lists ---
    "skiplist":                       {"label": "inline skip-list",               "color": "None", "edgecolor": "#6a3d9a", "hatch": "-"},
    "simpleskiplist":                 {"label": "skip-list",                      "color": "None", "edgecolor": "#cf17a7", "hatch": "---"},
    "simple_skiplist":                {"label": "skip-list",                      "color": "None", "edgecolor": "#cf17a7", "hatch": "---"},

    # --- Linked List ---
    "linkedlist":                     {"label": "linked-list",                    "color": "None", "edgecolor": "#222d8b", "hatch": "\\\\."},

    # --- Hash Linked List ---
    "hash_linked_list":               {"label": "hash linked-list",               "color": "None", "edgecolor": "#b22222", "hatch": "////"},
    "hash_linked_list-X6-H100000":    {"label": "hash linked-list",               "color": "None", "edgecolor": "#b22222", "hatch": "////"},
    "hashlinkedlist-H100000-X6":      {"label": "hash linked-list",               "color": "None", "edgecolor": "#b22222", "hatch": "////"},

    # --- Hash Skip List ---
    "hash_skip_list":                 {"label": "hash skip-list",                 "color": "None", "edgecolor": "#1f78b4", "hatch": "\\\\\\\\"},
    "hash_skip_list-X6-H100000":      {"label": "hash skip-list",                 "color": "None", "edgecolor": "#1f78b4", "hatch": "\\\\\\\\"},
    "hashskiplist-H100000-X6":        {"label": "hash skip-list",                 "color": "None", "edgecolor": "#1f78b4", "hatch": "\\\\\\\\"},

    # --- Hash Vector ---
    "hashvector":                     {"label": "hash vector",                    "color": "None", "edgecolor": "#ff7f0e", "hatch": "/."},
    "hash_vector-X6-H100000":         {"label": "hash vector",                    "color": "None", "edgecolor": "#ff7f0e", "hatch": "/."},
    "hashvector-H100000-X6":          {"label": "hash vector",                    "color": "None", "edgecolor": "#ff7f0e", "hatch": "/."},
}


line_styles = {
    "vector": {
        "label": "vector",
        "color": "#006d2c",  # Dark green (distinct and rich)
        "linestyle": 'solid', # (0, (1, 1)),  # Dotted
        "marker": "x",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "unsortedvector": {
        "label": "unsorted vector",
        "color": "#4d4d4d",  # Dark gray (visible and clean)
        "linestyle": 'dotted',  # Long dashes
        "marker": "v",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "alwayssortedvector": {
        "label": "sorted vector",
        "color": "#8b4513",  # SaddleBrown (dark reddish-brown)
        "linestyle": 'dashed',  # Dash-dot-dot
        "marker": "s",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "skiplist": {
        "label": "inline skip-list",
        "color": "#6a3d9a",  # Deep purple
        "linestyle": (0, (3, 5, 1, 5, 1, 5)),  # Dotted (long gaps)
        "marker": "o",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "simpleskiplist": {
        "label": "skip-list",
        "color": "#cf17a7",  # Teal (distinct from purple & blue)
        "linestyle": (0, (1, 1)),    # Dotted (different from skiplist solid)
        "marker": "P",       # Plus-filled marker (distinct)
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "linkedlist": {
        "label": "linked-list",
        "color": "#222d8b",  # ForestGreen (distinct and vibrant)
        "linestyle": (5, (10, 3)),
        "marker": "D",       # Diamond marker (distinct)
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
        "linestyle": (0, (3, 1, 1, 1, 1, 1)),
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
    "linkedlist": "\\.",
    "hashlinkedlist": "////",
    "hashskiplist": "\\\\\\\\",
    "hashvector": "/.",
}