#!/usr/bin/env python3

"""One-at-a-time bottleneck sweep for the on-disk write-throughput-vs-threads
regression: skiplist only, write_100pct only. Two independent sweeps sharing
the same buffer geometry/op count as memtable_scalability_vs_threads_ondisk_
small_l2:

  bg_sweep:   max_write_buffer_number fixed at 8; for each thread count T,
              max_background_jobs takes every value in BG_JOBS_VALUES that
              is <= T (background jobs must never exceed client threads).
  mwbn_sweep: max_background_jobs = T (exactly, for every thread count T);
              max_write_buffer_number swept over MWBN_VALUES.
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
EXPERIMENT_ROOT = REPO_ROOT / "data" / "ondisk_small_l2_bottleneck_sweep_skiplist"
WORKLOAD_SCRATCH = EXPERIMENT_ROOT / "_workload"
RUNNER_SCRIPT = "run_scripts/run_ondisk_bottleneck_sweep_skiplist.py"
PLOT_SCRIPT = "plot_scripts/plot_ondisk_bottleneck_sweep_skiplist.py"

THREAD_COUNTS = [1, 2, 4, 8, 16]
MEMTABLE_NAME = "skiplist"
MEMTABLE_FACTORY_ID = 1

# P * B * E must equal BUFFER_BYTES (see AGENTS.md "Buffer Geometry"): E is
# the real entry size (128B key + 896B val), same as memtable_scalability_
# vs_threads_ondisk_small_l2. -M is omitted so write_buffer_size comes from
# P*B*E.
BUFFER_BYTES = 8 * 1024 * 1024
ENTRY_SIZE = 128 + 896
ENTRIES_PER_PAGE = 4
BUFFER_SIZE_IN_PAGES = BUFFER_BYTES // (ENTRIES_PER_PAGE * ENTRY_SIZE)
assert BUFFER_SIZE_IN_PAGES * ENTRIES_PER_PAGE * ENTRY_SIZE == BUFFER_BYTES
BUFFER_GEOMETRY = ["-E", str(ENTRY_SIZE), "-B", str(ENTRIES_PER_PAGE),
                   "-P", str(BUFFER_SIZE_IN_PAGES), "-T", "10"]

WRITE_OP_COUNT = 450_000

BG_JOBS_VALUES = [1, 2, 4, 8, 16]
MWBN_FIXED_FOR_BG_SWEEP = 8

MWBN_VALUES = [2, 4, 8, 16, 32]

UNORDERED_WRITE = True
LOW_PRI = 1

WRITE_SPEC = EXPERIMENT_ROOT / "write_100pct" / "concurrency_write_bottleneck_sweep.spec.json"


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


def combo_flags(bg_jobs, mwbn):
    return ["--bg_jobs", str(bg_jobs),
            "--max_write_buffer_number", str(mwbn),
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


def run_one(scenario_dir, tag, T, bg_jobs, mwbn):
    run_dir = scenario_dir / tag / f"t{T}"
    assert WRITE_OP_COUNT % T == 0
    generate_thread_shards(WRITE_SPEC, run_dir, T, WRITE_OP_COUNT // T)

    flags = BUFFER_GEOMETRY + combo_flags(bg_jobs, mwbn)
    cmd = [str(BIN_DIR / "working_version_mt"), "--threads", str(T),
          "-m", str(MEMTABLE_FACTORY_ID)] + flags + [
          "--stat", "1", "--perf", "1", "--iostat", "1", "--progress", "0"]
    t0 = time.monotonic()
    run(cmd, cwd=run_dir, log_path=run_dir / "rocksdb_stats.log")
    wall = time.monotonic() - t0

    row = read_metrics_from_workload_log(run_dir)
    row["threads"] = T
    row["max_background_jobs"] = bg_jobs
    row["max_write_buffer_number"] = mwbn
    row["wall_seconds"] = f"{wall:.3f}"
    print(f"  {tag:12s} T={T:<2d} bg={bg_jobs:<2d} mwbn={mwbn:<2d} "
          f"ops/s={float(row['ops_per_sec']):>10.1f} (wall {wall:.1f}s)")

    harvest_and_cleanup(run_dir)
    for t in range(T):
        (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)
    return row


def run_bg_sweep(thread_counts):
    scenario_dir = EXPERIMENT_ROOT / "write_100pct" / "bg_sweep"
    scenario_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for T in thread_counts:
        for bg_jobs in BG_JOBS_VALUES:
            if bg_jobs > T:
                continue
            tag = f"bg{bg_jobs}"
            row = run_one(scenario_dir, tag, T, bg_jobs, MWBN_FIXED_FOR_BG_SWEEP)
            results.append(row)
    write_results_csv(scenario_dir / "results.csv", results)


def run_mwbn_sweep(thread_counts):
    scenario_dir = EXPERIMENT_ROOT / "write_100pct" / "mwbn_sweep"
    scenario_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for T in thread_counts:
        bg_jobs = T
        for mwbn in MWBN_VALUES:
            tag = f"mwbn{mwbn}"
            row = run_one(scenario_dir, tag, T, bg_jobs, mwbn)
            results.append(row)
    write_results_csv(scenario_dir / "results.csv", results)


def write_results_csv(path: Path, rows: list):
    fields = ["threads", "max_background_jobs", "max_write_buffer_number",
             "total_ops", "seconds", "ops_per_sec", "wall_seconds"]
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


def write_manifest(thread_counts):
    manifest = {
        "memtable": {MEMTABLE_NAME: MEMTABLE_FACTORY_ID},
        "bg_sweep": {
            "thread_counts": thread_counts,
            "bg_jobs_values_tried": BG_JOBS_VALUES,
            "constraint": "bg_jobs <= threads (skipped otherwise)",
            "max_write_buffer_number": MWBN_FIXED_FOR_BG_SWEEP,
        },
        "mwbn_sweep": {
            "thread_counts": thread_counts,
            "max_background_jobs": "= threads (exactly, per run)",
            "max_write_buffer_number_values": MWBN_VALUES,
        },
        "spec": str(WRITE_SPEC.relative_to(REPO_ROOT)),
        "op_count": WRITE_OP_COUNT,
        "buffer_geometry": BUFFER_GEOMETRY,
        "buffer_bytes": BUFFER_BYTES,
        "unordered_write": UNORDERED_WRITE,
        "concurrent_memtable_write": True,
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
                "bg_sweep": "python3 run_scripts/run_ondisk_bottleneck_sweep_skiplist.py --sweep bg",
                "mwbn_sweep": "python3 run_scripts/run_ondisk_bottleneck_sweep_skiplist.py --sweep mwbn",
                "plot": "python3 plot_scripts/plot_ondisk_bottleneck_sweep_skiplist.py",
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
                "max_background_jobs": "bg_sweep: swept over BG_JOBS_VALUES "
                    "capped at <= threads for that run (background jobs must "
                    "not exceed client thread count). mwbn_sweep: fixed = "
                    "threads for that run",
                "max_write_buffer_number": "bg_sweep: fixed at 8 "
                    "(rocksdb::Options::max_write_buffer_number; see "
                    "data/ondisk_write_stall_diagnostic/ for why 2, the "
                    "RocksDB default elsewhere in this codebase, causes "
                    "write stalls under concurrent writers). mwbn_sweep: "
                    "swept over MWBN_VALUES",
                "unordered_write": "fixed true for every run in this script "
                    "(rocksdb::Options::unordered_write, --unordered_write)",
            },
        },
    }
    EXPERIMENT_ROOT.mkdir(parents=True, exist_ok=True)
    with open(EXPERIMENT_ROOT / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sweep", choices=["bg", "mwbn"], required=True)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--thread-counts", default=None)
    args = parser.parse_args()

    thread_counts = ([int(x) for x in args.thread_counts.split(",")]
                     if args.thread_counts else THREAD_COUNTS)

    if not args.no_build:
        build_binaries()

    write_spec()
    write_manifest(thread_counts)

    if args.sweep == "bg":
        run_bg_sweep(thread_counts)
    else:
        run_mwbn_sweep(thread_counts)


if __name__ == "__main__":
    main()
