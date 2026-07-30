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

# Sanity check: is write_buffer_size itself (not how many memtables can queue)
# a lever on the disk-bound ceiling? 32 MB (4x baseline) is big enough to
# meaningfully cut flush/compaction rounds while still forcing real flushes
# (450,000 * 1024B =~ 461 MB of workload > 32MB*mwbn=8=256MB of headroom).
# 128 MB (16x baseline, same as memtable_scalability_vs_threads_ondisk_
# scaled_l2's buffer) has more headroom (1 GiB at mwbn=8) than the total
# workload volume, so it should behave close to fully in-memory -- included
# as the decisive check: if throughput scales like in-memory here, that
# confirms flush/compaction I/O (not something else) is the entire story.
def _geometry_for(buffer_bytes):
    pages = buffer_bytes // (ENTRIES_PER_PAGE * ENTRY_SIZE)
    assert pages * ENTRIES_PER_PAGE * ENTRY_SIZE == buffer_bytes
    return ["-E", str(ENTRY_SIZE), "-B", str(ENTRIES_PER_PAGE),
           "-P", str(pages), "-T", "10"]


BUFFER_BYTES_SANITY = 32 * 1024 * 1024
BUFFER_GEOMETRY_SANITY = _geometry_for(BUFFER_BYTES_SANITY)
BUFFER_BYTES_SANITY_128M = 128 * 1024 * 1024
BUFFER_GEOMETRY_SANITY_128M = _geometry_for(BUFFER_BYTES_SANITY_128M)
MWBN_FIXED_FOR_BUFSIZE_SWEEP = 8

WRITE_OP_COUNT = 450_000

BG_JOBS_VALUES = [1, 2, 4, 8, 16]
MWBN_FIXED_FOR_BG_SWEEP = 8

MWBN_VALUES = [2, 4, 8, 16, 32]

UNORDERED_WRITE = True
LOW_PRI = 1

WRITE_SPEC = EXPERIMENT_ROOT / "write_100pct" / "concurrency_write_bottleneck_sweep.spec.json"

# mwbn_sweep_buf128mib (450,000 ops, 8 MB-sized op count) turned out to
# never accumulate the 10 L0 files level0_file_num_compaction_trigger needs
# (461 MB of data / 128 MB buffer =~ 3.6 flushes) -- zero real compaction
# ever ran, so that result didn't actually test disk-bound compaction at
# this buffer size. 7,000,000 ops (=~ 7.1 GB) reproduces the same ~57-flush
# compaction pressure the 8 MB / 450,000-op runs had (461 MB / 8 MB =~ 57),
# scaled up by the buffer size -- same op count already used by
# memtable_scalability_vs_threads_ondisk_scaled_l2 for the same reason.
WRITE_OP_COUNT_SCALED = 7_000_000
WRITE_SPEC_SCALED = (EXPERIMENT_ROOT / "write_100pct" /
                    "concurrency_write_bottleneck_sweep_scaled.spec.json")

MWBN_SCALED_VALUES = [1, 2, 4, 8, 16]
MWBN_SCALED_BG_JOBS_FIXED = 8


def _write_spec_file(spec_path, op_count):
    spec_path.parent.mkdir(parents=True, exist_ok=True)
    spec = {
        "character_set": "alphanumeric",
        "sections": [{
            "groups": [{
                "inserts": {
                    "op_count": op_count,
                    "key": {"uniform": {"len": 128}},
                    "val": {"uniform": {"len": 896}},
                }
            }]
        }],
    }
    with open(spec_path, "w") as f:
        json.dump(spec, f, indent=2)


def write_spec():
    _write_spec_file(WRITE_SPEC, WRITE_OP_COUNT)


def write_spec_scaled():
    _write_spec_file(WRITE_SPEC_SCALED, WRITE_OP_COUNT_SCALED)


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


def run_one(scenario_dir, tag, T, bg_jobs, mwbn, buffer_geometry=BUFFER_GEOMETRY,
           spec_path=None, op_count=None):
    spec_path = spec_path or WRITE_SPEC
    op_count = op_count or WRITE_OP_COUNT
    run_dir = scenario_dir / tag / f"t{T}"
    assert op_count % T == 0
    generate_thread_shards(spec_path, run_dir, T, op_count // T)

    flags = buffer_geometry + combo_flags(bg_jobs, mwbn)
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


# The bufsize_sweep result (8/32/128 MB, mwbn=8 fixed) showed 128 MB
# removes essentially all of the flush/compaction pressure the 8 MB
# baseline was hitting (throughput scaled 307K->1,251K ops/s from T=1 to
# T=8). Fixing the buffer at 128 MB here so the bg_jobs/mwbn one-at-a-time
# sweeps are no longer confounded by an undersized buffer -- these write to
# NEW subfolders (*_buf128mib) so the original 8 MB bg_sweep/mwbn_sweep
# results above are preserved, not overwritten.
BUFFER_GEOMETRY_FIXED = BUFFER_GEOMETRY_SANITY_128M
BUFFER_BYTES_FIXED = BUFFER_BYTES_SANITY_128M


def run_bg_sweep(thread_counts):
    scenario_dir = EXPERIMENT_ROOT / "write_100pct" / "bg_sweep_buf128mib"
    scenario_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for T in thread_counts:
        for bg_jobs in BG_JOBS_VALUES:
            if bg_jobs > T:
                continue
            tag = f"bg{bg_jobs}"
            row = run_one(scenario_dir, tag, T, bg_jobs, MWBN_FIXED_FOR_BG_SWEEP,
                         BUFFER_GEOMETRY_FIXED)
            results.append(row)
    write_results_csv(scenario_dir / "results.csv", results)


def run_mwbn_sweep(thread_counts):
    scenario_dir = EXPERIMENT_ROOT / "write_100pct" / "mwbn_sweep_buf128mib"
    scenario_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for T in thread_counts:
        bg_jobs = T
        for mwbn in MWBN_VALUES:
            tag = f"mwbn{mwbn}"
            row = run_one(scenario_dir, tag, T, bg_jobs, mwbn, BUFFER_GEOMETRY_FIXED)
            results.append(row)
    write_results_csv(scenario_dir / "results.csv", results)


def run_mwbn_sweep_scaled(thread_counts):
    scenario_dir = EXPERIMENT_ROOT / "write_100pct" / "mwbn_sweep_buf128mib_scaled"
    scenario_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for T in thread_counts:
        for mwbn in MWBN_SCALED_VALUES:
            tag = f"mwbn{mwbn}"
            row = run_one(scenario_dir, tag, T, MWBN_SCALED_BG_JOBS_FIXED, mwbn,
                         BUFFER_GEOMETRY_FIXED, WRITE_SPEC_SCALED,
                         WRITE_OP_COUNT_SCALED)
            results.append(row)
    write_results_csv(scenario_dir / "results.csv", results)


def run_bufsize_sweep(thread_counts):
    scenario_dir = EXPERIMENT_ROOT / "write_100pct" / "bufsize_sweep"
    scenario_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for buffer_bytes, geometry, label in [
            (BUFFER_BYTES, BUFFER_GEOMETRY, "8MB"),
            (BUFFER_BYTES_SANITY, BUFFER_GEOMETRY_SANITY, "32MB"),
            (BUFFER_BYTES_SANITY_128M, BUFFER_GEOMETRY_SANITY_128M, "128MB")]:
        for T in thread_counts:
            bg_jobs = T
            tag = f"buf{label}"
            row = run_one(scenario_dir, tag, T, bg_jobs,
                         MWBN_FIXED_FOR_BUFSIZE_SWEEP, geometry)
            row["buffer_bytes"] = buffer_bytes
            results.append(row)
    write_results_csv(scenario_dir / "results.csv", results,
                     extra_fields=["buffer_bytes"])


def write_results_csv(path: Path, rows: list, extra_fields: list = None):
    fields = ["threads", "max_background_jobs", "max_write_buffer_number"]
    fields += extra_fields or []
    fields += ["total_ops", "seconds", "ops_per_sec", "wall_seconds"]
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
            "buffer_bytes": BUFFER_BYTES,
        },
        "mwbn_sweep": {
            "thread_counts": thread_counts,
            "max_background_jobs": "= threads (exactly, per run)",
            "max_write_buffer_number_values": MWBN_VALUES,
            "buffer_bytes": BUFFER_BYTES,
        },
        "bg_sweep_buf128mib": {
            "thread_counts": thread_counts,
            "bg_jobs_values_tried": BG_JOBS_VALUES,
            "constraint": "bg_jobs <= threads (skipped otherwise)",
            "max_write_buffer_number": MWBN_FIXED_FOR_BG_SWEEP,
            "buffer_bytes": BUFFER_BYTES_FIXED,
            "note": "same as bg_sweep but buffer fixed at 128 MB instead of "
                "8 MB -- bufsize_sweep showed the 8 MB buffer was itself "
                "the dominant confound, so this isolates bg_jobs without it",
        },
        "mwbn_sweep_buf128mib": {
            "thread_counts": thread_counts,
            "max_background_jobs": "= threads (exactly, per run)",
            "max_write_buffer_number_values": MWBN_VALUES,
            "buffer_bytes": BUFFER_BYTES_FIXED,
            "note": "same as mwbn_sweep but buffer fixed at 128 MB instead "
                "of 8 MB, for the same reason as bg_sweep_buf128mib",
        },
        "mwbn_sweep_buf128mib_scaled": {
            "thread_counts": thread_counts,
            "max_background_jobs": MWBN_SCALED_BG_JOBS_FIXED,
            "max_write_buffer_number_values": MWBN_SCALED_VALUES,
            "buffer_bytes": BUFFER_BYTES_FIXED,
            "op_count": WRITE_OP_COUNT_SCALED,
            "spec": str(WRITE_SPEC_SCALED.relative_to(REPO_ROOT)),
            "note": "corrected version of mwbn_sweep_buf128mib -- that run "
                "used the 450,000-op spec sized for an 8 MB buffer, which "
                "at 128 MB only produces ~4 L0 files, never reaching "
                "level0_file_num_compaction_trigger=10, so zero real "
                "compaction ever ran. 7,000,000 ops restores the same "
                "~57-flush compaction pressure the 8 MB baseline had. "
                "bg_jobs fixed at 8 here (not = threads)",
        },
        "bufsize_sweep": {
            "thread_counts": thread_counts,
            "max_background_jobs": "= threads (exactly, per run)",
            "max_write_buffer_number": MWBN_FIXED_FOR_BUFSIZE_SWEEP,
            "buffer_bytes_values": [BUFFER_BYTES, BUFFER_BYTES_SANITY,
                                   BUFFER_BYTES_SANITY_128M],
            "note": "isolates write_buffer_size itself (not queue depth) as "
                "a lever on the disk-bound write ceiling found in mwbn_sweep "
                "-- both points use the same mwbn=8, bg_jobs=threads",
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
                "bufsize_sweep": "python3 run_scripts/run_ondisk_bottleneck_sweep_skiplist.py --sweep bufsize",
                "mwbn_sweep_buf128mib_scaled": "python3 run_scripts/run_ondisk_bottleneck_sweep_skiplist.py --sweep mwbn-scaled",
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
                "buffer_bytes": "bufsize_sweep only: write_buffer_size in "
                    "bytes (derived from -E/-B/-P, see AGENTS.md 'Buffer "
                    "Geometry'), the two values compared at fixed mwbn=8, "
                    "bg_jobs=threads -- isolates whether memtable size "
                    "itself (not queue depth) affects the disk-bound write "
                    "ceiling found in mwbn_sweep at T=8/16",
            },
        },
    }
    EXPERIMENT_ROOT.mkdir(parents=True, exist_ok=True)
    with open(EXPERIMENT_ROOT / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--sweep", choices=["bg", "mwbn", "bufsize", "mwbn-scaled"],
                        required=True)
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--thread-counts", default=None)
    args = parser.parse_args()

    thread_counts = ([int(x) for x in args.thread_counts.split(",")]
                     if args.thread_counts else THREAD_COUNTS)

    if not args.no_build:
        build_binaries()

    write_spec()
    if args.sweep == "mwbn-scaled":
        write_spec_scaled()
    write_manifest(thread_counts)

    if args.sweep == "bg":
        run_bg_sweep(thread_counts)
    elif args.sweep == "mwbn":
        run_mwbn_sweep(thread_counts)
    elif args.sweep == "mwbn-scaled":
        run_mwbn_sweep_scaled(thread_counts)
    else:
        run_bufsize_sweep(thread_counts)


if __name__ == "__main__":
    main()
