#!/usr/bin/env python3

"""Does max_subcompactions (splitting ONE compaction job across multiple
threads) close the gap between RocksDB's realized disk write throughput
(~236 MB/s peak, measured in ondisk_skiplist_diskio) and the raw disk's
real sustained capacity (~430 MB/s, measured via direct dd in
disk_raw_benchmark)? Skiplist only, write_100pct only.

Deliberately uses the smaller 8 MB buffer / 450,000-op config (same as
memtable_scalability_vs_threads_ondisk_small_l2) rather than the 128 MB /
6M-op "winning combo" -- smaller and faster to run, while still reliably
triggering real L0->L1 compaction (450,000 * 1024B =~ 461 MB of data /
8 MB buffer =~ 57 flushes, comfortably past the 10-file
level0_file_num_compaction_trigger).
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
EXPERIMENT_ROOT = REPO_ROOT / "data" / "ondisk_subcompactions_sweep_skiplist"
WORKLOAD_SCRATCH = EXPERIMENT_ROOT / "_workload"
RUNNER_SCRIPT = "run_scripts/run_ondisk_subcompactions_sweep_skiplist.py"
PLOT_SCRIPT = "plot_scripts/plot_ondisk_subcompactions_sweep_skiplist.py"

THREAD_COUNTS = [1, 2, 4, 8, 16]
MEMTABLE_NAME = "skiplist"
MEMTABLE_FACTORY_ID = 1

# Same geometry as memtable_scalability_vs_threads_ondisk_small_l2 (see
# AGENTS.md "Buffer Geometry"): E = real entry size, B*E = 4096B page, P
# derived so P*B*E is exact.
BUFFER_BYTES = 8 * 1024 * 1024
ENTRY_SIZE = 128 + 896
ENTRIES_PER_PAGE = 4
BUFFER_SIZE_IN_PAGES = BUFFER_BYTES // (ENTRIES_PER_PAGE * ENTRY_SIZE)
assert BUFFER_SIZE_IN_PAGES * ENTRIES_PER_PAGE * ENTRY_SIZE == BUFFER_BYTES
BUFFER_GEOMETRY = ["-E", str(ENTRY_SIZE), "-B", str(ENTRIES_PER_PAGE),
                   "-P", str(BUFFER_SIZE_IN_PAGES), "-T", "10"]

BG_JOBS_FIXED = 8
MWBN_FIXED = 8
WRITE_OP_COUNT = 450_000
UNORDERED_WRITE = True
LOW_PRI = 1

SUBCOMPACTIONS_VALUES = [1, 2, 4, 8]

WRITE_SPEC = EXPERIMENT_ROOT / "write_100pct" / "concurrency_write_subcompactions_sweep.spec.json"


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


def combo_flags(max_subcompactions):
    return ["--bg_jobs", str(BG_JOBS_FIXED),
            "--max_write_buffer_number", str(MWBN_FIXED),
            "--max_subcompactions", str(max_subcompactions),
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


def run_one(scenario_dir, tag, T, max_subcompactions):
    run_dir = scenario_dir / tag / f"t{T}"
    assert WRITE_OP_COUNT % T == 0
    generate_thread_shards(WRITE_SPEC, run_dir, T, WRITE_OP_COUNT // T)

    flags = BUFFER_GEOMETRY + combo_flags(max_subcompactions)
    cmd = [str(BIN_DIR / "working_version_mt"), "--threads", str(T),
          "-m", str(MEMTABLE_FACTORY_ID)] + flags + [
          "--stat", "1", "--perf", "1", "--iostat", "1", "--progress", "0"]
    t0 = time.monotonic()
    run(cmd, cwd=run_dir, log_path=run_dir / "rocksdb_stats.log")
    wall = time.monotonic() - t0

    row = read_metrics_from_workload_log(run_dir)
    row["threads"] = T
    row["max_subcompactions"] = max_subcompactions
    row["wall_seconds"] = f"{wall:.3f}"
    print(f"  subc={max_subcompactions:<2d} T={T:<2d} "
          f"ops/s={float(row['ops_per_sec']):>10.1f} (wall {wall:.1f}s)")

    harvest_and_cleanup(run_dir)
    for t in range(T):
        (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)
    return row


def run_write_scenario(thread_counts):
    scenario_dir = EXPERIMENT_ROOT / "write_100pct"
    scenario_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for max_subcompactions in SUBCOMPACTIONS_VALUES:
        tag = f"subc{max_subcompactions}"
        for T in thread_counts:
            results.append(run_one(scenario_dir, tag, T, max_subcompactions))
        write_results_csv(scenario_dir / "results.csv", results)


def write_results_csv(path: Path, rows: list):
    fields = ["threads", "max_subcompactions", "total_ops", "seconds",
             "ops_per_sec", "wall_seconds"]
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
        "thread_counts": thread_counts,
        "max_background_jobs": BG_JOBS_FIXED,
        "max_write_buffer_number": MWBN_FIXED,
        "max_subcompactions_values": SUBCOMPACTIONS_VALUES,
        "buffer_geometry": BUFFER_GEOMETRY,
        "buffer_bytes": BUFFER_BYTES,
        "op_count": WRITE_OP_COUNT,
        "unordered_write": UNORDERED_WRITE,
        "concurrent_memtable_write": True,
        "spec": str(WRITE_SPEC.relative_to(REPO_ROOT)),
        "note": "Tests whether splitting a single compaction job across "
            "multiple threads (max_subcompactions) closes the gap found in "
            "ondisk_skiplist_diskio (RocksDB peaked at ~236 MB/s measured "
            "disk write, vs ~430 MB/s sustained raw dd throughput to the "
            "same device in disk_raw_benchmark). Uses the smaller 8 MB / "
            "450,000-op config (not the 128 MB / 6M-op 'winning combo') "
            "for a faster sweep, while still reliably triggering real "
            "L0->L1 compaction (~57 flushes vs the 10-file "
            "level0_file_num_compaction_trigger).",
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
                "write": "python3 run_scripts/run_ondisk_subcompactions_sweep_skiplist.py",
                "plot": "python3 plot_scripts/plot_ondisk_subcompactions_sweep_skiplist.py",
            },
            "metric_definitions": {
                "ops_per_sec": "total_ops / seconds, parsed from "
                    "workload.log (see run_scripts/workload_log_metrics.py)",
                "max_subcompactions": "swept over SUBCOMPACTIONS_VALUES "
                    "(rocksdb::Options::max_subcompactions, "
                    "--max_subcompactions; RocksDB default is 1, i.e. no "
                    "splitting)",
                "wall_seconds": "end-to-end subprocess wall time as "
                    "measured by the orchestrating Python script",
            },
        },
    }
    EXPERIMENT_ROOT.mkdir(parents=True, exist_ok=True)
    with open(EXPERIMENT_ROOT / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--thread-counts", default=None)
    args = parser.parse_args()

    thread_counts = ([int(x) for x in args.thread_counts.split(",")]
                     if args.thread_counts else THREAD_COUNTS)

    if not args.no_build:
        build_binaries()

    write_spec()
    write_manifest(thread_counts)
    run_write_scenario(thread_counts)


if __name__ == "__main__":
    main()
