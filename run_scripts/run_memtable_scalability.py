#!/usr/bin/env python3
"""Memtable scalability-vs-thread-count experiment (reviewer comment R2.W2).

Drives the project's real experiment harness — the same working_version
binary used by runexp.sh — rather than a standalone benchmark. Two scenarios,
run one at a time:

  write : 100% insert. A single Tectonic-generated workload (3,000,000
          unique random inserts) is re-sharded into T contiguous pieces for
          each thread count; T client threads (working_version_mt --threads
          T) each replay one shard concurrently against a FRESH empty DB, so
          every thread count processes the identical total amount of work
          and completion time is directly comparable.

  read  : 100% read. Per memtable: load 1,500,000 keys once (single-threaded
          working_version, ~1.4 GB > the 128 MB write buffer, so data is
          flushed to disk — matches the project's standard on-disk buffer
          geometry, see runexp_correctness.sh), then replay 200,000 point
          queries T-way sharded against that same DB for each thread count
          in turn (working_version_mt --threads T -d 0), reusing the loaded
          DB across all thread counts.

For every run: stdout/stderr -> rocksdb_stats.log, the RocksDB info LOG is
moved out of db/ before db/ is deleted (kept even on failure), and the
one-line throughput.csv the binary emits is folded into a per-scenario
results.csv. Output layout:

  experiment_data/memtable_scalability_vs_threads/
    write_100pct/<memtable>/t<T>/{rocksdb_stats.log,LOG,throughput.csv}
    write_100pct/results.csv
    read_100pct/<memtable>/load/{rocksdb_stats.log,LOG}
    read_100pct/<memtable>/t<T>/{rocksdb_stats.log,LOG,throughput.csv}
    read_100pct/results.csv
    manifest.json
    ANALYSIS.md   (written separately, after inspecting results.csv)

Usage:
  python3 scripts/concurrency/run_scalability_experiment.py --scenario write
  python3 scripts/concurrency/run_scalability_experiment.py --scenario read
  python3 scripts/concurrency/run_scalability_experiment.py --scenario write --only skiplist,art
"""
import argparse
import csv
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
BIN_DIR = REPO_ROOT / "bin"
EXPERIMENT_ROOT = REPO_ROOT / "experiment_data" / "memtable_scalability_vs_threads"
WORKLOAD_SCRATCH = EXPERIMENT_ROOT / "_workload"

THREAD_COUNTS = [1, 2, 4, 8, 16]

# The 7 concurrency-capable memtables (see include/config_options.h switch
# and include/db_env.h memtable_factory doc comment for the full id map).
MEMTABLES = {
    "skiplist": 1,
    "vector": 2,
    "unsorted_vector": 5,
    "sorted_vector": 6,
    "simple_skiplist": 8,
    "art": 11,
    "tlx_btree": 12,
}

# 128 MB write buffer (E * B * P bytes), matching runexp_correctness.sh's
# geometry convention. --bg_jobs raises max_background_jobs from the
# project default of 1: at 1, a single flush/compaction thread cannot keep
# up with several MB/s of concurrent inserts crossing repeated 128 MB
# memtable boundaries, so RocksDB's own write-stall throttle
# (WaitUntilFlushWouldNotStallWrites) throttles ALL client threads
# regardless of the memtable's own insert concurrency -- a flush-pipeline
# bottleneck, not a memtable-scalability result. Confirmed via the LOG
# ("WaitUntilFlushWouldNotStallWrites waiting on stall conditions to
# clear") on a first pass of this experiment before this flag was added.
# Fixed-duration measurement (see run_workload_multithread.cc): each thread
# wraps its shard on EOF and runs until this many wall-clock seconds have
# elapsed, so ops-completed is compared at a common measurement window
# instead of "how long did this fixed op count take" -- a handful of
# seconds' worth of DB::Open / cache-clear / background-thread-warmup fixed
# cost would otherwise dominate a short fixed-op-count run and make
# thread-count comparisons noisy (confirmed on a first pass of this
# experiment before this flag was added: T=4 came in below T=1).
DURATION_SECS = 5
COMMON_FLAGS = ["--bg_jobs", "8", "--duration_secs", str(DURATION_SECS)]
WRITE_BUFFER_FLAGS = ["-E", "128", "-B", "32", "-P", "32768", "-T", "6"] + COMMON_FLAGS
READ_BUFFER_FLAGS = ["-E", "1024", "-B", "4", "-P", "32768", "-T", "6"] + COMMON_FLAGS

WRITE_SPEC = REPO_ROOT / "lib/Tectonic/specs/concurrency_write.spec.json"
READ_SPEC = REPO_ROOT / "lib/Tectonic/specs/concurrency_read.spec.json"
WRITE_OP_COUNT = 3_000_000
READ_LOAD_OP_COUNT = 1_500_000  # + 1 filler insert emitted by section 1
READ_QUERY_OP_COUNT = 200_000


def run(cmd, cwd=None, log_path=None, check=True):
    """Runs a subprocess, optionally teeing stdout+stderr to log_path."""
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
    print("Building working_version, working_version_mt, tectonic-cli ...")
    run(["cmake", "--build", "build", "--target", "working_version",
        "working_version_mt", "tectonic-cli", "-j", "24"], cwd=REPO_ROOT)


def generate_workload(spec_path: Path, out_path: Path):
    if out_path.exists():
        print(f"  workload already generated: {out_path}")
        return
    out_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"  generating {out_path.name} from {spec_path.name} ...")
    run([str(BIN_DIR / "tectonic-cli"), "generate", "-w", str(spec_path),
        "-o", str(out_path)])


def write_shards(lines, run_dir: Path, num_threads: int):
    """Splits `lines` into num_threads contiguous, balanced shard files
    named shard_0.txt .. shard_{T-1}.txt inside run_dir."""
    run_dir.mkdir(parents=True, exist_ok=True)
    n = len(lines)
    base, extra = divmod(n, num_threads)
    start = 0
    for t in range(num_threads):
        count = base + (1 if t < extra else 0)
        shard_path = run_dir / f"shard_{t}.txt"
        with open(shard_path, "w") as f:
            f.writelines(lines[start:start + count])
        start += count


def harvest_and_cleanup(run_dir: Path):
    """Moves db/LOG out to run_dir/LOG, then deletes db/ to save space."""
    db_log = run_dir / "db" / "LOG"
    if db_log.exists():
        shutil.move(str(db_log), str(run_dir / "LOG"))
    db_dir = run_dir / "db"
    if db_dir.exists():
        shutil.rmtree(db_dir)


def read_throughput_csv(run_dir: Path) -> dict:
    path = run_dir / "throughput.csv"
    with open(path) as f:
        row = next(csv.DictReader(f))
    return row


def run_write_scenario(memtables):
    scenario_dir = EXPERIMENT_ROOT / "write_100pct"
    scenario_dir.mkdir(parents=True, exist_ok=True)

    workload_path = WORKLOAD_SCRATCH / "write_workload.txt"
    generate_workload(WRITE_SPEC, workload_path)
    with open(workload_path) as f:
        lines = f.readlines()
    assert len(lines) == WRITE_OP_COUNT, (
        f"expected {WRITE_OP_COUNT} lines, got {len(lines)}")

    results = []
    for name in memtables:
        factory_id = MEMTABLES[name]
        for T in THREAD_COUNTS:
            run_dir = scenario_dir / name / f"t{T}"
            write_shards(lines, run_dir, T)

            cmd = [str(BIN_DIR / "working_version_mt"), "--threads", str(T),
                  "-m", str(factory_id)] + WRITE_BUFFER_FLAGS + [
                  "--stat", "0", "--progress", "0"]
            t0 = time.monotonic()
            run(cmd, cwd=run_dir, log_path=run_dir / "rocksdb_stats.log")
            wall = time.monotonic() - t0

            row = read_throughput_csv(run_dir)
            row["memtable"] = name
            row["wall_seconds"] = f"{wall:.3f}"
            results.append(row)
            print(f"  write {name:16s} T={T:<2d} "
                  f"ops/s={float(row['ops_per_sec']):>10.1f} "
                  f"(wall {wall:.1f}s)")

            harvest_and_cleanup(run_dir)
            for t in range(T):
                (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)

    write_results_csv(scenario_dir / "results.csv", results)


def run_read_scenario(memtables):
    scenario_dir = EXPERIMENT_ROOT / "read_100pct"
    scenario_dir.mkdir(parents=True, exist_ok=True)

    workload_path = WORKLOAD_SCRATCH / "read_workload.txt"
    generate_workload(READ_SPEC, workload_path)
    with open(workload_path) as f:
        lines = f.readlines()
    load_line_count = READ_LOAD_OP_COUNT + 1  # + section-1 filler insert
    load_lines = lines[:load_line_count]
    query_lines = lines[load_line_count:]
    assert len(query_lines) == READ_QUERY_OP_COUNT, (
        f"expected {READ_QUERY_OP_COUNT} query lines, got {len(query_lines)}")

    results = []
    for name in memtables:
        factory_id = MEMTABLES[name]

        # Load once (single-threaded, fresh DB), reused for every T.
        load_dir = scenario_dir / name / "load"
        load_dir.mkdir(parents=True, exist_ok=True)
        (load_dir / "workload.txt").write_text("".join(load_lines))
        load_cmd = [str(BIN_DIR / "working_version"), "-m", str(factory_id)
                   ] + READ_BUFFER_FLAGS + ["--stat", "0", "--progress", "0"]
        print(f"  read  {name:16s} loading {READ_LOAD_OP_COUNT:,} keys ...")
        t0 = time.monotonic()
        run(load_cmd, cwd=load_dir, log_path=load_dir / "rocksdb_stats.log")
        load_wall = time.monotonic() - t0
        print(f"    load done in {load_wall:.1f}s")
        (load_dir / "workload.txt").unlink()
        # Keep db/ under load/ — the query runs below reopen it directly.
        db_dir = load_dir / "db"
        if (load_dir / "db" / "LOG").exists():
            shutil.copy(str(load_dir / "db" / "LOG"), str(load_dir / "LOG"))

        for T in THREAD_COUNTS:
            run_dir = scenario_dir / name / f"t{T}"
            run_dir.mkdir(parents=True, exist_ok=True)
            # Reopen the SAME db/ directory the load phase created, without
            # destroying it (-d 0), so every thread count measures against
            # an identical on-disk dataset.
            if (run_dir / "db").exists():
                shutil.rmtree(run_dir / "db")
            shutil.copytree(db_dir, run_dir / "db")
            write_shards(query_lines, run_dir, T)

            cmd = [str(BIN_DIR / "working_version_mt"), "-d", "0",
                  "--threads", str(T), "-m", str(factory_id)
                  ] + READ_BUFFER_FLAGS + ["--stat", "0", "--progress", "0"]
            t0 = time.monotonic()
            run(cmd, cwd=run_dir, log_path=run_dir / "rocksdb_stats.log")
            wall = time.monotonic() - t0

            row = read_throughput_csv(run_dir)
            row["memtable"] = name
            row["wall_seconds"] = f"{wall:.3f}"
            results.append(row)
            print(f"  read  {name:16s} T={T:<2d} "
                  f"ops/s={float(row['ops_per_sec']):>10.1f} "
                  f"(wall {wall:.1f}s)")

            harvest_and_cleanup(run_dir)
            for t in range(T):
                (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)

        shutil.rmtree(db_dir)

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


def write_manifest():
    manifest = {
        "memtables": MEMTABLES,
        "thread_counts": THREAD_COUNTS,
        "write_scenario": {
            "spec": str(WRITE_SPEC.relative_to(REPO_ROOT)),
            "op_count": WRITE_OP_COUNT,
            "buffer_flags": WRITE_BUFFER_FLAGS,
            "buffer_bytes": 128 * 1024 * 1024,
        },
        "read_scenario": {
            "spec": str(READ_SPEC.relative_to(REPO_ROOT)),
            "load_op_count": READ_LOAD_OP_COUNT,
            "query_op_count": READ_QUERY_OP_COUNT,
            "buffer_flags": READ_BUFFER_FLAGS,
            "buffer_bytes": 128 * 1024 * 1024,
        },
        "harness": {
            "generator": "lib/Tectonic (tectonic-cli)",
            "executor": "working_version / working_version_mt "
                       "(src/run_workload.cc, src/run_workload_multithread.cc)",
        },
    }
    EXPERIMENT_ROOT.mkdir(parents=True, exist_ok=True)
    with open(EXPERIMENT_ROOT / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", choices=["write", "read"], required=True,
                        help="Run exactly one scenario (run one at a time).")
    parser.add_argument("--only", default=None,
                        help="Comma-separated subset of memtable names "
                             "(default: all 7).")
    parser.add_argument("--no-build", action="store_true",
                        help="Skip the cmake build step.")
    args = parser.parse_args()

    memtables = (args.only.split(",") if args.only
                else list(MEMTABLES.keys()))
    for name in memtables:
        if name not in MEMTABLES:
            sys.exit(f"unknown memtable {name!r}; choices: {list(MEMTABLES)}")

    if not args.no_build:
        build_binaries()

    write_manifest()

    if args.scenario == "write":
        run_write_scenario(memtables)
    else:
        run_read_scenario(memtables)


if __name__ == "__main__":
    main()
