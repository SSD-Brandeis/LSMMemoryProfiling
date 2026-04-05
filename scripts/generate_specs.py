import json
import argparse

# ==============================================================================
# PREVIOUS SEQUENTIAL SPEC GENERATION (COMMENTED OUT)
# ==============================================================================
# def build_specs(args):
#     def numexpr(n):
#         return float(n)
#
#     key_size = args.entry_size * args.lmbda
#     val_size = args.entry_size * (1 - args.lmbda)
#
#     group1 = {
#         "inserts": {
#             "op_count": numexpr(args.inserts),
#             "key": {"uniform": {"len": key_size}},
#             "val": {"uniform": {"len": val_size}},
#         },
#     }
#     group2 = {
#         "point_queries": {
#             "op_count": numexpr(args.point_queries),
#         },
#         # "updates": {
#         #     "op_count": numexpr(args.updates),
#         #     "val": {"uniform": {"len": val_size}},
#         #     "selection": {"uniform": {"min": 0, "max": 1}},
#         # },
#         # "point_deletes": {
#         #     "op_count": numexpr(args.point_deletes),
#         # },
#         # "range_deletes": {
#         #     "op_count": numexpr(args.range_deletes),
#         #     "selectivity": numexpr(args.range_delete_selectivity),
#         #     "range_format": "StartEnd",
#         # },
#     }
#     group3 = {
#         "range_queries": {
#             "op_count": numexpr(args.range_queries),
#             "selectivity": numexpr(args.range_selectivity),
#             "range_format": "StartEnd",
#         },
#     }
#
#     group1 = {
#         k: v for k, v in group1.items() if v and args.__dict__[k.replace("-", "_")] > 0
#     }
#     group2 = {
#         k: v for k, v in group2.items() if v and args.__dict__[k.replace("-", "_")] > 0
#     }
#     group3 = {
#         k: v for k, v in group3.items() if v and args.__dict__[k.replace("-", "_")] > 0
#     }
#
#     return {"sections": [{"groups": [group1, group2, group3]}]}


# ==============================================================================
# ACTUAL INTERLEAVED SPEC GENERATION
# ==============================================================================
def build_specs(args):
    """
    To interleave operations in Tectonic, all op types must be in the SAME group.
    """
    def numexpr(n):
        return float(n)

    key_size = args.entry_size * args.lmbda
    val_size = args.entry_size * (1 - args.lmbda)

    # Combine all potential operations into one dictionary
    combined_ops = {
        "inserts": {
            "op_count": numexpr(args.inserts),
            "key": {"uniform": {"len": key_size}},
            "val": {"uniform": {"len": val_size}},
        },
    }
    group2 = {
        "point_queries": {
            "op_count": numexpr(args.point_queries),
        },
        "range_queries": {
            "op_count": numexpr(args.range_queries),
            "selectivity": numexpr(args.range_selectivity),
            "range_format": "StartEnd",
        },
        "updates": {
            "op_count": numexpr(args.updates),
            "val": {"uniform": {"len": val_size}},
            "selection": {"uniform": {"min": 0, "max": 1}},
        },
        "range_deletes": {
            "op_count": numexpr(args.range_deletes),
            "selectivity": numexpr(args.range_delete_selectivity),
            "range_format": "StartEnd",
        },
    }

    # Filter out operations with an op_count of 0
    interleaved_group = {
        k: v for k, v in combined_ops.items() 
        if args.__dict__.get(k.replace("-", "_"), 0) > 0
    }
    group2 = {
        k: v for k, v in group2.items() if v and args.__dict__[k.replace("-", "_")] > 0
    }
    group3 = {
        k: v for k, v in group3.items() if v and args.__dict__[k.replace("-", "_")] > 0
    }

    # Return a single section with a single group to force interleaving
    return {"sections": [{"groups": [interleaved_group]}]}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-I", "--inserts", type=int, default=0)
    parser.add_argument("-U", "--updates", type=int, default=0)
    parser.add_argument("-S", "--range_queries", type=int, default=0)
    parser.add_argument("-Y", "--range_selectivity", type=float, default=0.1)
    parser.add_argument("-Q", "--point_queries", type=int, default=0)
    parser.add_argument("-D", "--point_deletes", type=int, default=0)
    parser.add_argument("-R", "--range_deletes", type=int, default=0)
    parser.add_argument("-y", "--range_delete_selectivity", type=float, default=0.05)
    parser.add_argument("-E", "--entry_size", type=int, default=8)
    parser.add_argument("-L", "--lmbda", type=float, default=0.5)
    parser.add_argument("-o", "--output", type=str, default="workload.specs.json")
    args = parser.parse_args()

    spec = build_specs(args)

    with open(args.output, "w") as f:
        json.dump(spec, f, indent=2)


if __name__ == "__main__":
    main()