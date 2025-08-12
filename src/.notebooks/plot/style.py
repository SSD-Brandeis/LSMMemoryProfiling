bar_styles = {
    "hash_linked_list":               {"label": "Hash linked list",               "color": "None", "edgecolor": "black", "hatch": "////"},
    "hash_linked_list-X6-H100000":    {"label": "Hash linked list X6 H100k",      "color": "None", "edgecolor": "black", "hatch": "////"},
    "hash_skip_list":                 {"label": "Hash skip list",                 "color": "None", "edgecolor": "black", "hatch": "\\\\"},
    "hash_skip_list-X6-H100000":      {"label": "Hash skip list X6 H100k",        "color": "None", "edgecolor": "black", "hatch": "\\\\"},
    "skiplist":                       {"label": "Skip-list",                      "color": "None", "edgecolor": "black", "hatch": "++++"},
    "AlwaysSortedVector":             {"label": "Always-sorted vector",           "color": "None", "edgecolor": "black", "hatch": "...."},
    "AlwaysSortedVector-dynamic":     {"label": "Always-sorted vector dynamic",       "color": "None", "edgecolor": "black", "hatch": "...."},
    # "AlwaysSortedVector-preallocated": {"label": "Always-sorted vector prealloc",  "color": "None", "edgecolor": "black", "hatch": "...."},
    "UnsortedVector":                 {"label": "Unsorted vector",                "color": "None", "edgecolor": "black", "hatch": "xxxx"},
    "UnsortedVector-dynamic":         {"label": "Unsorted vector dynamic",            "color": "None", "edgecolor": "black", "hatch": "xxxx"},
    # "UnsortedVector-preallocated":    {"label": "Unsorted vector prealloc",       "color": "None", "edgecolor": "black", "hatch": "xxxx"},
    "Vector":                         {"label": "Vector",                         "color": "None", "edgecolor": "black", "hatch": "----"},
    "Vector-dynamic":                 {"label": "Vector dynamic",                     "color": "None", "edgecolor": "black", "hatch": "----"},
    # "Vector-preallocated":            {"label": "Vector prealloc",                "color": "None", "edgecolor": "black", "hatch": "----"},
}

# line_styles = {
#     "hash_linked_list":               {"label": "Hash linked list",               "color": "black",  "linestyle": ":",  "marker": "D", "markersize": 8, "markerfacecolor": "none"},
#     "hash_linked_list-X6-H100000":    {"label": "Hash linked list X6 H100k",      "color": "black",  "linestyle": ":",  "marker": "D", "markersize": 8, "markerfacecolor": "none"},
#     "hash_skip_list":                 {"label": "Hash skip list",                 "color": "blue",   "linestyle": "--", "marker": "^", "markersize": 8, "markerfacecolor": "none"},
#     "hash_skip_list-X6-H100000":      {"label": "Hash skip list X6 H100k",        "color": "blue",   "linestyle": "--", "marker": "^", "markersize": 8, "markerfacecolor": "none"},
#     "skiplist":                       {"label": "Skip-list",                      "color": "orange", "linestyle": "-",  "marker": "o", "markersize": 8, "markerfacecolor": "none"},
#     "AlwaysSortedVector":             {"label": "Always-sorted vector",           "color": "black",  "linestyle": "-.", "marker": "s", "markersize": 8, "markerfacecolor": "none"},
#     "AlwaysSortedVector-dynamic":     {"label": "Always-sorted vector dynamic",       "color": "black",  "linestyle": "-.", "marker": "s", "markersize": 8, "markerfacecolor": "none"},
#     # "AlwaysSortedVector-preallocated": {"label": "Always-sorted vector prealloc",  "color": "black",  "linestyle": "-.", "marker": "s", "markersize": 8, "markerfacecolor": "none"},
#     "UnsortedVector":                 {"label": "Unsorted vector",                "color": "black",  "linestyle": "--", "marker": "v", "markersize": 8, "markerfacecolor": "none"},
#     "UnsortedVector-dynamic":         {"label": "Unsorted vector dynamic",            "color": "black",  "linestyle": "--", "marker": "v", "markersize": 8, "markerfacecolor": "none"},
#     # "UnsortedVector-preallocated":    {"label": "Unsorted vector prealloc",       "color": "black",  "linestyle": "--", "marker": "v", "markersize": 8, "markerfacecolor": "none"},
#     "Vector":                         {"label": "Vector",                         "color": "purple", "linestyle": ":",  "marker": "x", "markersize": 8, "markerfacecolor": "none"},
#     "Vector-dynamic":                 {"label": "Vector dynamic",                     "color": "purple", "linestyle": ":",  "marker": "x", "markersize": 8, "markerfacecolor": "none"},
#     # "Vector-preallocated":            {"label": "Vector prealloc",                "color": "purple", "linestyle": ":",  "marker": "x", "markersize": 8, "markerfacecolor": "none"},
# }


line_styles = {
    "Vector": {
        "label": "vector",
        "color": "#006d2c",  # Dark green (distinct and rich)
        "linestyle": (0, (1, 1)),  # Dotted
        "marker": "x",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "skiplist": {
        "label": "skip list",
        "color": "#6a3d9a",  # Deep purple
        "linestyle": "-",
        "marker": "o",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "hash_skip_list": {
        "label": "hash skip list",
        "color": "#1f78b4",  # Darker blue (colorblind-friendly)
        "linestyle": "--",
        "marker": "^",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "hash_linked_list": {
        "label": "hash linked list",
        "color": "#b22222",  # Firebrick (dark red)
        "linestyle": "-.",
        "marker": "D",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "AlwaysSortedVector": {
        "label": "always sorted vector",
        "color": "#8b4513",  # SaddleBrown (dark reddish-brown)
        "linestyle": (0, (3, 1, 1, 1)),  # Dash-dot-dot
        "marker": "s",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
    "UnsortedVector": {
        "label": "unsorted vector",
        "color": "#4d4d4d",  # Dark gray (visible and clean)
        "linestyle": (0, (5, 2)),  # Long dashes
        "marker": "v",
        "markersize": 12,
        "markerfacecolor": "none",
        "linewidth": 2,
    },
}
