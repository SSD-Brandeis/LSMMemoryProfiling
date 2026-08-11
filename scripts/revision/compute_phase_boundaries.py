#!/usr/bin/env python3
"""Derive --phase_boundaries/--phase_names for run_workload.cc from a
Tectonic workload spec's per-group op_count fields.

Usage: compute_phase_boundaries.py <spec.json>
Prints two lines: comma-separated cumulative boundaries, then
comma-separated phase names (one more entry than boundaries -- the final
phase's end is implicit at EOF, so it needs no boundary of its own).

Only spec["sections"][0]["groups"] is considered -- every ycsb spec in this
repo has exactly one section.
"""
import json
import sys

OP_KEYS = (
    "unique_inserts",
    "inserts",
    "updates",
    "merges",
    "point_deletes",
    "empty_point_deletes",
    "range_deletes",
    "point_queries",
    "range_queries",
)


def main():
    if len(sys.argv) != 2:
        print(f"usage: {sys.argv[0]} <spec.json>", file=sys.stderr)
        sys.exit(1)

    with open(sys.argv[1]) as f:
        spec = json.load(f)

    groups = spec["sections"][0]["groups"]

    counts = []
    names = []
    for i, group in enumerate(groups):
        total = sum(group[k]["op_count"] for k in OP_KEYS if k in group)
        counts.append(total)
        names.append(group.get("name", f"Phase {i + 1}"))

    boundaries = []
    running = 0
    for c in counts[:-1]:
        running += c
        boundaries.append(running)

    print(",".join(str(b) for b in boundaries))
    print(",".join(names))


if __name__ == "__main__":
    main()
