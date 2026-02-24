import json
import argparse


def build_specs(args):
    def numexpr(n):
        return float(n)

    key_size = args.entry_size * args.lmbda
    val_size = args.entry_size * (1 - args.lmbda)

    group1 = {
        "inserts": {
            "op_count": numexpr(args.inserts),
            "key": {"uniform": {"len": key_size}},
            "val": {"uniform": {"len": val_size}},
        },
        "point_queries": {
            "op_count": numexpr(args.point_queries),
        },
        # "updates": {
        #     "op_count": numexpr(args.updates),
        #     "val": {"uniform": {"len": val_size}},
        #     "selection": {"uniform": {"min": 0, "max": 1}},
        # },
        "range_queries": {
            "op_count": numexpr(args.range_queries),
            "selectivity": numexpr(args.range_selectivity),
            "range_format": "StartEnd",
        },
        # "point_deletes": {
        #     "op_count": numexpr(args.point_deletes),
        # },
        # "range_deletes": {
        #     "op_count": numexpr(args.range_deletes),
        #     "selectivity": numexpr(args.range_delete_selectivity),
        #     "range_format": "StartEnd",
        # },
    }

    group1 = {
        k: v for k, v in group1.items() if v and args.__dict__[k.replace("-", "_")] > 0
    }

    return {"sections": [{"groups": [group1]}]}


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