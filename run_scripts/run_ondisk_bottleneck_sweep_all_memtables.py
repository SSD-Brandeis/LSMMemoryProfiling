#!/usr/bin/env python3

"""Does the on-disk write-throughput-vs-threads fix (128 MB buffer) hold
across all seven memtables, not just skiplist? Single fixed parameter combo
-- bg_jobs=8, max_write_buffer_number=16 (the best-scaling combo found in
run_ondisk_bottleneck_sweep_skiplist.py's mwbn_sweep_buf128mib_scaled) --
applied identically (same op count, same buffer) to every memtable so the
comparison across memtables is apples-to-apples. write_100pct only.

Memtables are run fastest-expected-first (sorted_vector's O(n) per-insert
cost makes it dramatically slower than the others at this op count) and
results.csv is rewritten after each memtable finishes, so a slow last
memtable doesn't block seeing the rest.
"""

import argparse
import csv
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from workload_log_metrics import read_metrics_from_workload_log

REPO_ROOT = Path(__file__).resolve().parents[1]
BIN_DIR = REPO_ROOT / "bin"
EXPERIMENT_ROOT = REPO_ROOT / "data" / "ondisk_bottleneck_sweep_all_memtables"
WORKLOAD_SCRATCH = EXPERIMENT_ROOT / "_workload"
RUNNER_SCRIPT = "run_scripts/run_ondisk_bottleneck_sweep_all_memtables.py"
PLOT_SCRIPT = "plot_scripts/plot_ondisk_bottleneck_sweep_all_memtables.py"

THREAD_COUNTS = [1, 2, 4, 8, 16]

# Fastest-expected-first so a pathologically slow memtable (sorted_vector)
# doesn't block seeing results for the rest.
MEMTABLES = {
    "skiplist": 1,
    "simple_skiplist": 8,
    "art": 11,
    "tlx_btree": 12,
    "vector": 2,
    "unsorted_vector": 5,
    "sorted_vector": 6,
}

# 128 MB buffer, same geometry as memtable_scalability_vs_threads_ondisk_
# scaled_l2 and the winning bufsize_sweep point (see AGENTS.md "Buffer
# Geometry"): E = real entry size (128B key + 896B val), B*E = 4096B page,
# P derived so P*B*E is exact. -M omitted so write_buffer_size comes from
# P*B*E alone.
BUFFER_BYTES = 128 * 1024 * 1024
ENTRY_SIZE = 128 + 896
ENTRIES_PER_PAGE = 4
BUFFER_SIZE_IN_PAGES = BUFFER_BYTES // (ENTRIES_PER_PAGE * ENTRY_SIZE)
assert BUFFER_SIZE_IN_PAGES * ENTRIES_PER_PAGE * ENTRY_SIZE == BUFFER_BYTES
BUFFER_GEOMETRY = ["-E", str(ENTRY_SIZE), "-B", str(ENTRIES_PER_PAGE),
                   "-P", str(BUFFER_SIZE_IN_PAGES), "-T", "10"]

# Winning combo from run_ondisk_bottleneck_sweep_skiplist.py's
# mwbn_sweep_buf128mib_scaled: mwb=16 scaled ~2.1x from T=1 to T=16 (vs
# ~1.5x for mwb=8, and flat for mwb=1/2/4), so this is the parameter that
# best restores thread scaling under real compaction.
BG_JOBS_FIXED = 8
MWBN_FIXED = 16

# Same op count for every memtable (identical workload, not memtable-tuned):
# 6,000,000 * 1024B =~ 6.1 GB, ~48 flushes at 128 MB buffer, comfortably
# above the 10-file level0_file_num_compaction_trigger -- real compaction
# happens (L1 reached) without needing L2 depth.
WRITE_OP_COUNT = 6_000_000

UNORDERED_WRITE = True
LOW_PRI = 1

WRITE_SPEC = EXPERIMENT_ROOT / "write_100pct" / "concurrency_write_all_memtables.spec.json"


def write_spec():
    WRITE_SPEC.parent.mkdir(parents=True, exist_ok=True)
    spec = {
        "character_set": "alphanumeric",
        "sections": [{
            "groups": [{
                "inserts": {
                    "op_count": WRITE_OP_COUNT,
                    "key": {"uniform": {"len": 128}},
                    "val": {"uniform": {"len": 896}},
                }
            }]
        }],
    }
    with open(WRITE_SPEC, "w") as f:
        json.dump(spec, f, indent=2)


def combo_flags():
    return ["--bg_jobs", str(BG_JOBS_FIXED),
            "--max_write_buffer_number", str(MWBN_FIXED),
            "--concurrent_memtable_write", "1",
            "--unordered_write", "1" if UNORDERED_WRITE else "0",
            "--lowpri", str(LOW_PRI)]


def run(cmd, cwd=None, log_path=None, check=True):
    proc = subprocess.run(cmd, cwd=cwd, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True)
    if log_path is not None:
        log_path.write_text(proc.stdout)
    if check and proc.returncode != 0:
        raise RuntimeError(
            f"command failed ({proc.returncode}): {' '.join(map(str, cmd))}\n"
            f"--- output ---\n{proc.stdout[-4000:]}")
    return proc


def build_binaries():
    print("Building working_version_mt, tectonic-cli ...")
    run(["cmake", "--build", "build", "--target", "working_version_mt",
        "tectonic-cli", "-j", "24"], cwd=REPO_ROOT)


def generate_thread_shards(spec_path: Path, run_dir: Path, num_threads: int,
                           expected_lines_per_thread: int):
    run_dir.mkdir(parents=True, exist_ok=True)
    scale = 1.0 / num_threads
    for t in range(num_threads):
        shard_path = run_dir / f"shard_{t}.txt"
        run([str(BIN_DIR / "tectonic-cli"), "generate", "-w", str(spec_path),
            "-o", str(shard_path), "-s", str(scale)])
        with open(shard_path) as f:
            n = sum(1 for _ in f)
        assert n == expected_lines_per_thread, (
            f"{shard_path}: expected {expected_lines_per_thread} lines "
            f"(spec scaled by 1/{num_threads}), got {n}")


def harvest_and_cleanup(run_dir: Path):
    db_log = run_dir / "db" / "LOG"
    if db_log.exists():
        shutil.move(str(db_log), str(run_dir / "LOG"))
    db_dir = run_dir / "db"
    if db_dir.exists():
        shutil.rmtree(db_dir)


def run_one(scenario_dir, name, factory_id, T):
    run_dir = scenario_dir / name / f"t{T}"
    assert WRITE_OP_COUNT % T == 0
    generate_thread_shards(WRITE_SPEC, run_dir, T, WRITE_OP_COUNT // T)

    flags = BUFFER_GEOMETRY + combo_flags()
    cmd = [str(BIN_DIR / "working_version_mt"), "--threads", str(T),
          "-m", str(factory_id)] + flags + [
          "--stat", "1", "--perf", "1", "--iostat", "1", "--progress", "0"]
    t0 = time.monotonic()
    run(cmd, cwd=run_dir, log_path=run_dir / "rocksdb_stats.log")
    wall = time.monotonic() - t0

    row = read_metrics_from_workload_log(run_dir)
    row["memtable"] = name
    row["threads"] = T
    row["wall_seconds"] = f"{wall:.3f}"
    print(f"  {name:16s} T={T:<2d} ops/s={float(row['ops_per_sec']):>10.1f} "
          f"(wall {wall:.1f}s)")

    harvest_and_cleanup(run_dir)
    for t in range(T):
        (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)
    return row


def run_write_scenario(memtables, thread_counts):
    scenario_dir = EXPERIMENT_ROOT / "write_100pct"
    scenario_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for name in memtables:
        factory_id = MEMTABLES[name]
        for T in thread_counts:
            results.append(run_one(scenario_dir, name, factory_id, T))
        # Rewritten after each memtable so a slow one (sorted_vector) doesn't
        # block seeing the rest.
        write_results_csv(scenario_dir / "results.csv", results)


def write_results_csv(path: Path, rows: list):
    fields = ["memtable", "threads", "total_ops", "seconds", "ops_per_sec",
             "wall_seconds"]
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in fields})
    print(f"wrote {path}")


def relative_to_repo_or_abs(path: Path) -> str:
    try:
        return str(path.relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def write_manifest(thread_counts, memtables):
    manifest = {
        "memtables": {name: MEMTABLES[name] for name in memtables},
        "thread_counts": thread_counts,
        "max_background_jobs": BG_JOBS_FIXED,
        "max_write_buffer_number": MWBN_FIXED,
        "buffer_geometry": BUFFER_GEOMETRY,
        "buffer_bytes": BUFFER_BYTES,
        "op_count": WRITE_OP_COUNT,
        "unordered_write": UNORDERED_WRITE,
        "concurrent_memtable_write": True,
        "spec": str(WRITE_SPEC.relative_to(REPO_ROOT)),
        "note": "bg_jobs=8/mwb=16/buffer=128MB is the combo that best "
            "restored thread scaling in run_ondisk_bottleneck_sweep_"
            "skiplist.py's mwbn_sweep_buf128mib_scaled (~2.1x T1->T16, vs "
            "~1.5x for mwb=8 and flat for mwb=1/2/4). Same op count "
            "(6,000,000) used identically for every memtable -- not "
            "memtable-tuned -- so the comparison across memtables is "
            "apples-to-apples. Sized so real compaction happens (L1 "
            "reached, level0_file_num_compaction_trigger=10 crossed) "
            "without requiring L2 depth.",
        "harness": {
            "generator": "lib/Tectonic (tectonic-cli)",
            "executor": "working_version_mt (src/run_workload_multithread.cc)",
        },
        "provenance": {
            "runner_script": RUNNER_SCRIPT,
            "plot_script": PLOT_SCRIPT,
            "data_dir": relative_to_repo_or_abs(EXPERIMENT_ROOT),
            "plot_dir": relative_to_repo_or_abs(EXPERIMENT_ROOT / "plots"),
            "artifact_dir": relative_to_repo_or_abs(EXPERIMENT_ROOT),
            "command_setup": {
                "write": "python3 run_scripts/run_ondisk_bottleneck_sweep_all_memtables.py",
                "plot": "python3 plot_scripts/plot_ondisk_bottleneck_sweep_all_memtables.py",
            },
            "metric_definitions": {
                "ops_per_sec": "total_ops / seconds, where total_ops is the "
                    "fixed op count for the run (every thread replays its "
                    "shard exactly once) and seconds is however long that "
                    "took. Parsed directly from each run's own workload.log "
                    "'Threads:'/'Total Ops:'/'Workload Execution Time:'/'Ops "
                    "Per Sec:' lines (see run_scripts/workload_log_metrics.py)",
                "wall_seconds": "end-to-end subprocess wall time for the "
                    "run as measured by the orchestrating Python script "
                    "(includes DB::Open/Close overhead, unlike 'seconds' "
                    "which is the binary's own measurement window)",
            },
        },
    }
    EXPERIMENT_ROOT.mkdir(parents=True, exist_ok=True)
    with open(EXPERIMENT_ROOT / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--only", default=None,
                        help="Comma-separated subset of memtable names "
                             "(default: all 7, fastest-expected-first).")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--thread-counts", default=None)
    args = parser.parse_args()

    memtables = (args.only.split(",") if args.only else list(MEMTABLES.keys()))
    for name in memtables:
        if name not in MEMTABLES:
            sys.exit(f"unknown memtable {name!r}; choices: {list(MEMTABLES)}")

    thread_counts = ([int(x) for x in args.thread_counts.split(",")]
                     if args.thread_counts else THREAD_COUNTS)

    if not args.no_build:
        build_binaries()

    write_spec()
    write_manifest(thread_counts, memtables)
    run_write_scenario(memtables, thread_counts)


if __name__ == "__main__":
    main()
