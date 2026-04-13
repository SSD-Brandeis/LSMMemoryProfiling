import json
import argparse

def build_specs(args):
    def numexpr(n):
        return float(n)

    key_size = args.entry_size * args.lmbda
    val_size = args.entry_size * (1 - args.lmbda)

    # Single group for the specific phase counts passed from bash
    group = {
        "inserts": {
            "op_count": numexpr(args.inserts),
            "key": {"uniform": {"len": key_size}},
            "val": {"uniform": {"len": val_size}},
        },
        "point_queries": {
            "op_count": numexpr(args.point_queries),
        }
    }

    return {"sections": [{"groups": [group]}]}

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-I", "--inserts", type=int, default=0)
    parser.add_argument("-Q", "--point_queries", type=int, default=0)
    parser.add_argument("-E", "--entry_size", type=int, default=32)
    parser.add_argument("-L", "--lmbda", type=float, default=0.25)
    # Defaulting output to specs.txt as tectonic expects
    parser.add_argument("-o", "--output", type=str, default="specs.txt")
    args = parser.parse_args()

    spec = build_specs(args)
    with open(args.output, "w") as f:
        json.dump(spec, f, indent=2)

if __name__ == "__main__":
    main()