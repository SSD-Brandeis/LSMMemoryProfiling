#!/usr/bin/env python3

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
EXPERIMENT_ROOT = REPO_ROOT / "data" / "memtable_scalability_vs_threads_ondisk_small_l2"
WORKLOAD_SCRATCH = EXPERIMENT_ROOT / "_workload"
RUNNER_SCRIPT = "run_scripts/run_memtable_scalability_ondisk_small_l2.py"
PLOT_SCRIPT = "plot_scripts/plot_memtable_scalability_ondisk_small_l2.py"

THREAD_COUNTS = [1, 2, 4, 8, 16]

MEMTABLES = {
    "skiplist": 1,
    "vector": 2,
    "unsorted_vector": 5,
    "sorted_vector": 6,
    "simple_skiplist": 8,
    "art": 11,
    "tlx_btree": 12,
}

# P * B * E must equal BUFFER_BYTES (see AGENTS.md "Buffer Geometry"): E is the
# real entry size (128B key + 896B val), B*E = 4096B block size, P derived so
# the product is exact. -M is omitted so write_buffer_size comes from P*B*E,
# not a separately-specified (and possibly inconsistent) value.
BUFFER_BYTES = 8 * 1024 * 1024
ENTRY_SIZE = 128 + 896
ENTRIES_PER_PAGE = 4
BUFFER_SIZE_IN_PAGES = BUFFER_BYTES // (ENTRIES_PER_PAGE * ENTRY_SIZE)
assert BUFFER_SIZE_IN_PAGES * ENTRIES_PER_PAGE * ENTRY_SIZE == BUFFER_BYTES
WRITE_BUFFER_GEOMETRY = ["-E", str(ENTRY_SIZE), "-B", str(ENTRIES_PER_PAGE),
                        "-P", str(BUFFER_SIZE_IN_PAGES), "-T", "10"]
READ_BUFFER_GEOMETRY = WRITE_BUFFER_GEOMETRY
MIXED_BUFFER_GEOMETRY = WRITE_BUFFER_GEOMETRY

WRITE_DEFAULT_BG_JOBS = [8]
WRITE_DEFAULT_UNORDERED_WRITE = [False, True]

READ_DEFAULT_BG_JOBS = [8]
READ_DEFAULT_UNORDERED_WRITE = [False]

MIXED_DEFAULT_BG_JOBS = [8]
MIXED_DEFAULT_UNORDERED_WRITE = [False, True]

LOW_PRI = 1
MAX_WRITE_BUFFER_NUMBER = 8


def combo_flags(bg_jobs, unordered_write):
    return ["--bg_jobs", str(bg_jobs),
            "--max_write_buffer_number", str(MAX_WRITE_BUFFER_NUMBER),
            "--concurrent_memtable_write", "1",
            "--unordered_write", "1" if unordered_write else "0",
            "--lowpri", str(LOW_PRI)]


def combo_tag(bg_jobs, unordered_write):
    return f"bg{bg_jobs}_uw{1 if unordered_write else 0}"

WRITE_SPEC = EXPERIMENT_ROOT / "write_100pct" / "concurrency_write_ondisk_small_l2.spec.json"
READ_SPEC = EXPERIMENT_ROOT / "read_100pct" / "concurrency_read_ondisk_small_l2.spec.json"
# Mixed uses TWO specs: a load-only spec (replayed once, single-threaded, to
# force a real flush before timing starts) and a timed spec (self-contained
# insert+point_query group, generated independently per thread via
# generate_thread_shards -- same structure as the in-memory mixed spec).
MIXED_LOAD_SPEC = EXPERIMENT_ROOT / "mixed_50_50" / "concurrency_mixed_load_ondisk_small_l2.spec.json"
MIXED_SPEC = EXPERIMENT_ROOT / "mixed_50_50" / "concurrency_mixed_timed_ondisk_small_l2.spec.json"

WRITE_OP_COUNT = 450_000
READ_LOAD_OP_COUNT = 450_000
READ_QUERY_OP_COUNT = 10_000
MIXED_LOAD_OP_COUNT = 450_000
MIXED_INSERT_OP_COUNT = 10_000
MIXED_QUERY_OP_COUNT = 10_000


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
    """Splits `lines` into num_threads contiguous, balanced shard files.
    Only safe when `lines` has no cross-line ordering dependency that could
    be split across two different threads' files -- i.e. read's pure-query
    lines (every query only depends on the already-complete load phase)."""
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


def generate_thread_shards(spec_path: Path, run_dir: Path, num_threads: int,
                           expected_lines_per_thread: int):
    """Generates num_threads INDEPENDENT workloads directly via tectonic-cli
    (spec_path's op counts scaled by 1/num_threads via -s), one shard_t.txt
    per thread -- instead of generating one combined workload and chopping
    it into per-thread pieces. Needed for write (architectural consistency)
    and mixed's timed section (correctness: avoids a query in one thread's
    shard referencing a key inserted in a different thread's shard)."""
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
    """Moves db/LOG out to run_dir/LOG, then deletes db/ to save space."""
    db_log = run_dir / "db" / "LOG"
    if db_log.exists():
        shutil.move(str(db_log), str(run_dir / "LOG"))
    db_dir = run_dir / "db"
    if db_dir.exists():
        shutil.rmtree(db_dir)


def read_throughput(run_dir: Path) -> dict:
    return read_metrics_from_workload_log(run_dir)


def run_write_scenario(memtables, bg_jobs_list, unordered_write_list,
                       thread_counts):
    scenario_dir = EXPERIMENT_ROOT / "write_100pct"
    scenario_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for bg_jobs in bg_jobs_list:
        for unordered_write in unordered_write_list:
            tag = combo_tag(bg_jobs, unordered_write)
            flags = WRITE_BUFFER_GEOMETRY + combo_flags(
                bg_jobs, unordered_write)
            for name in memtables:
                factory_id = MEMTABLES[name]
                for T in thread_counts:
                    run_dir = scenario_dir / tag / name / f"t{T}"
                    assert WRITE_OP_COUNT % T == 0, (
                        f"WRITE_OP_COUNT={WRITE_OP_COUNT} not divisible "
                        f"by T={T}")
                    generate_thread_shards(WRITE_SPEC, run_dir, T,
                                          WRITE_OP_COUNT // T)

                    cmd = [str(BIN_DIR / "working_version_mt"), "--threads",
                          str(T), "-m", str(factory_id)] + flags + [
                          "--stat", "1", "--perf", "1", "--iostat", "1",
                          "--progress", "0"]
                    t0 = time.monotonic()
                    run(cmd, cwd=run_dir, log_path=run_dir / "rocksdb_stats.log")
                    wall = time.monotonic() - t0

                    row = read_throughput(run_dir)
                    row["memtable"] = name
                    row["max_background_jobs"] = bg_jobs
                    row["unordered_write"] = int(unordered_write)
                    row["wall_seconds"] = f"{wall:.3f}"
                    results.append(row)
                    print(f"  write {tag:10s} {name:16s} T={T:<2d} "
                          f"ops/s={float(row['ops_per_sec']):>10.1f} "
                          f"(wall {wall:.1f}s)")

                    harvest_and_cleanup(run_dir)
                    for t in range(T):
                        (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)

    write_results_csv(scenario_dir / "results.csv", results, sweep=True)


def run_read_scenario(memtables, bg_jobs_list, unordered_write_list,
                      thread_counts):
    if len(bg_jobs_list) > 1 or len(unordered_write_list) > 1:
        print("  note: read scenario ignores all but the first --bg-jobs / "
              "--unordered-write value (no sweep nesting for reads)")
    bg_jobs = bg_jobs_list[0]
    unordered_write = unordered_write_list[0]
    read_flags = combo_flags(bg_jobs, unordered_write)

    scenario_dir = EXPERIMENT_ROOT / "read_100pct"
    scenario_dir.mkdir(parents=True, exist_ok=True)

    workload_path = WORKLOAD_SCRATCH / "read_workload.txt"
    generate_workload(READ_SPEC, workload_path)
    with open(workload_path) as f:
        lines = f.readlines()
    # READ_SPEC's two groups (load inserts, then point_queries) are emitted
    # as two clean contiguous blocks -- see run_memtable_scalability_inmemory
    # .py's WRITE_SPEC comment -- so this is a plain positional split, no
    # filler insert involved.
    load_lines = lines[:READ_LOAD_OP_COUNT]
    query_lines = lines[READ_LOAD_OP_COUNT:]
    assert len(query_lines) == READ_QUERY_OP_COUNT, (
        f"expected {READ_QUERY_OP_COUNT} query lines, got {len(query_lines)}")

    results = []
    for name in memtables:
        factory_id = MEMTABLES[name]

        load_dir = scenario_dir / name / "load"
        load_dir.mkdir(parents=True, exist_ok=True)
        (load_dir / "workload.txt").write_text("".join(load_lines))
        load_cmd = [str(BIN_DIR / "working_version"), "-m", str(factory_id)
                   ] + READ_BUFFER_GEOMETRY + read_flags + [
                   "--stat", "1", "--perf", "1", "--iostat", "1",
                   "--progress", "0"]
        print(f"  read  {name:16s} loading {READ_LOAD_OP_COUNT:,} keys ...")
        t0 = time.monotonic()
        run(load_cmd, cwd=load_dir, log_path=load_dir / "rocksdb_stats.log")
        load_wall = time.monotonic() - t0
        print(f"    load done in {load_wall:.1f}s")
        (load_dir / "workload.txt").unlink()
        db_dir = load_dir / "db"
        if (load_dir / "db" / "LOG").exists():
            shutil.copy(str(load_dir / "db" / "LOG"), str(load_dir / "LOG"))

        for T in thread_counts:
            run_dir = scenario_dir / name / f"t{T}"
            run_dir.mkdir(parents=True, exist_ok=True)
            if (run_dir / "db").exists():
                shutil.rmtree(run_dir / "db")
            shutil.copytree(db_dir, run_dir / "db")
            write_shards(query_lines, run_dir, T)

            cmd = [str(BIN_DIR / "working_version_mt"), "-d", "0",
                  "--threads", str(T), "-m", str(factory_id)
                  ] + READ_BUFFER_GEOMETRY + read_flags + [
                  "--stat", "1", "--perf", "1", "--iostat", "1",
                  "--progress", "0"]
            t0 = time.monotonic()
            run(cmd, cwd=run_dir, log_path=run_dir / "rocksdb_stats.log")
            wall = time.monotonic() - t0

            row = read_throughput(run_dir)
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


def run_mixed_scenario(memtables, bg_jobs_list, unordered_write_list,
                       thread_counts):
    scenario_dir = EXPERIMENT_ROOT / "mixed_50_50"
    scenario_dir.mkdir(parents=True, exist_ok=True)

    total_mixed_ops = MIXED_INSERT_OP_COUNT + MIXED_QUERY_OP_COUNT

    load_workload_path = WORKLOAD_SCRATCH / "mixed_load_workload.txt"
    generate_workload(MIXED_LOAD_SPEC, load_workload_path)
    with open(load_workload_path) as f:
        load_lines = f.readlines()
    assert len(load_lines) == MIXED_LOAD_OP_COUNT, (
        f"expected {MIXED_LOAD_OP_COUNT} load lines, got {len(load_lines)}")

    results = []
    for bg_jobs in bg_jobs_list:
        for unordered_write in unordered_write_list:
            tag = combo_tag(bg_jobs, unordered_write)
            mixed_flags = combo_flags(bg_jobs, unordered_write)
            for name in memtables:
                factory_id = MEMTABLES[name]

                load_dir = scenario_dir / tag / name / "load"
                load_dir.mkdir(parents=True, exist_ok=True)
                (load_dir / "workload.txt").write_text("".join(load_lines))
                load_cmd = [str(BIN_DIR / "working_version"), "-m", str(factory_id)
                           ] + MIXED_BUFFER_GEOMETRY + mixed_flags + [
                           "--stat", "1", "--perf", "1", "--iostat", "1",
                           "--progress", "0"]
                print(f"  mixed {tag:10s} {name:16s} loading "
                      f"{MIXED_LOAD_OP_COUNT:,} keys ...")
                t0 = time.monotonic()
                run(load_cmd, cwd=load_dir, log_path=load_dir / "rocksdb_stats.log")
                load_wall = time.monotonic() - t0
                print(f"    load done in {load_wall:.1f}s")
                (load_dir / "workload.txt").unlink()
                db_dir = load_dir / "db"
                if (load_dir / "db" / "LOG").exists():
                    shutil.copy(str(load_dir / "db" / "LOG"), str(load_dir / "LOG"))

                for T in thread_counts:
                    run_dir = scenario_dir / tag / name / f"t{T}"
                    run_dir.mkdir(parents=True, exist_ok=True)
                    if (run_dir / "db").exists():
                        shutil.rmtree(run_dir / "db")
                    shutil.copytree(db_dir, run_dir / "db")
                    assert total_mixed_ops % T == 0, (
                        f"total_mixed_ops={total_mixed_ops} not divisible "
                        f"by T={T}")
                    generate_thread_shards(MIXED_SPEC, run_dir, T,
                                          total_mixed_ops // T)

                    cmd = [str(BIN_DIR / "working_version_mt"), "-d", "0",
                          "--threads", str(T), "-m", str(factory_id)
                          ] + MIXED_BUFFER_GEOMETRY + mixed_flags + [
                          "--stat", "1", "--perf", "1", "--iostat", "1",
                          "--progress", "0"]
                    t0 = time.monotonic()
                    run(cmd, cwd=run_dir, log_path=run_dir / "rocksdb_stats.log")
                    wall = time.monotonic() - t0

                    row = read_throughput(run_dir)
                    row["memtable"] = name
                    row["max_background_jobs"] = bg_jobs
                    row["unordered_write"] = int(unordered_write)
                    row["wall_seconds"] = f"{wall:.3f}"
                    results.append(row)
                    print(f"  mixed {tag:10s} {name:16s} T={T:<2d} "
                          f"ops/s={float(row['ops_per_sec']):>10.1f} "
                          f"(wall {wall:.1f}s)")

                    harvest_and_cleanup(run_dir)
                    for t in range(T):
                        (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)

                shutil.rmtree(db_dir)

    write_results_csv(scenario_dir / "results.csv", results, sweep=True)


def write_results_csv(path: Path, rows: list, sweep: bool = False):
    fields = ["memtable", "threads"]
    if sweep:
        fields += ["max_background_jobs", "unordered_write"]
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


def write_manifest(scenario, bg_jobs_list, unordered_write_list,
                   thread_counts):
    manifest_path = EXPERIMENT_ROOT / "manifest.json"
    manifest = {}
    if manifest_path.exists():
        with open(manifest_path) as f:
            manifest = json.load(f)

    manifest["memtables"] = MEMTABLES
    scenario_key = f"{scenario}_scenario"
    scenario_record = {
        "thread_counts": thread_counts,
        "bg_jobs_swept": bg_jobs_list,
        "unordered_write_swept": unordered_write_list,
        "concurrent_memtable_write": True,
    }
    if scenario == "write":
        scenario_record.update({
            "spec": str(WRITE_SPEC.relative_to(REPO_ROOT)),
            "op_count": WRITE_OP_COUNT,
            "buffer_geometry": WRITE_BUFFER_GEOMETRY,
            "buffer_bytes": BUFFER_BYTES,
        })
    elif scenario == "read":
        scenario_record.update({
            "spec": str(READ_SPEC.relative_to(REPO_ROOT)),
            "load_op_count": READ_LOAD_OP_COUNT,
            "query_op_count": READ_QUERY_OP_COUNT,
            "buffer_geometry": READ_BUFFER_GEOMETRY,
            "buffer_bytes": BUFFER_BYTES,
        })
    else:
        scenario_record.update({
            "load_spec": str(MIXED_LOAD_SPEC.relative_to(REPO_ROOT)),
            "timed_spec": str(MIXED_SPEC.relative_to(REPO_ROOT)),
            "load_op_count": MIXED_LOAD_OP_COUNT,
            "insert_op_count": MIXED_INSERT_OP_COUNT,
            "query_op_count": MIXED_QUERY_OP_COUNT,
            "buffer_geometry": MIXED_BUFFER_GEOMETRY,
            "buffer_bytes": BUFFER_BYTES,
        })
    manifest[scenario_key] = scenario_record

    manifest["harness"] = {
        "generator": "lib/Tectonic (tectonic-cli)",
        "executor": "working_version / working_version_mt "
                   "(src/run_workload.cc, src/run_workload_multithread.cc)",
    }
    manifest["provenance"] = {
        "runner_script": RUNNER_SCRIPT,
        "plot_script": PLOT_SCRIPT,
        "data_dir": relative_to_repo_or_abs(EXPERIMENT_ROOT),
        "plot_dir": relative_to_repo_or_abs(EXPERIMENT_ROOT / "plots"),
        "artifact_dir": relative_to_repo_or_abs(EXPERIMENT_ROOT),
        "command_setup": {
            "write": "python3 run_scripts/run_memtable_scalability_ondisk_small_l2.py --scenario write",
            "read": "python3 run_scripts/run_memtable_scalability_ondisk_small_l2.py --scenario read",
            "mixed": "python3 run_scripts/run_memtable_scalability_ondisk_small_l2.py --scenario mixed",
            "plot": "python3 plot_scripts/plot_memtable_scalability_ondisk_small_l2.py",
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
            "speedup": "ops_per_sec(T) / ops_per_sec(T=1) for the same "
                "memtable, computed at plot time (not stored in "
                "results.csv)",
            "max_background_jobs": "fixed at 8 for every scenario here "
                "(rocksdb::Options::max_background_jobs, --bg_jobs)",
            "max_write_buffer_number": "fixed at 8 for every scenario here "
                "(rocksdb::Options::max_write_buffer_number, "
                "--max_write_buffer_number; RocksDB default elsewhere in "
                "this codebase is 2). See "
                "data/ondisk_write_stall_diagnostic/ -- 2 caused write "
                "stalls under concurrent writers with a small buffer; 8 "
                "was confirmed to produce zero stall events",
            "unordered_write": "swept false/true for the write and mixed "
                "scenarios (rocksdb::Options::unordered_write, "
                "--unordered_write); requires concurrent_memtable_write="
                "true",
            "low_pri": "fixed at 1/true for every run in this script "
                "(write_options->low_pri, --lowpri; matches "
                "include/db_env.h's own default) so writes are "
                "deprioritized behind real flush/compaction, which does "
                "run during the timed window here",
            "buffer_bytes": "8 MiB, deliberately much smaller than the "
                "128 MiB used by the large-scale ondisk experiment, so "
                "this experiment's small op counts (in-memory experiment "
                "1's op counts, reused here) still force real flushes to "
                "disk instead of fitting entirely in the memtable",
        },
    }
    EXPERIMENT_ROOT.mkdir(parents=True, exist_ok=True)
    with open(EXPERIMENT_ROOT / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)


def main():
    global EXPERIMENT_ROOT, WORKLOAD_SCRATCH
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", choices=["write", "read", "mixed"], required=True,
                        help="Run exactly one scenario (run one at a time).")
    parser.add_argument("--only", default=None,
                        help="Comma-separated subset of memtable names "
                             "(default: all 7).")
    parser.add_argument("--no-build", action="store_true",
                        help="Skip the cmake build step.")
    parser.add_argument("--bg-jobs", default=None,
                        help="Comma-separated max_background_jobs values to "
                             "sweep (default: 8 for all scenarios).")
    parser.add_argument("--unordered-write", default=None,
                        help="Comma-separated 0/1 values to sweep (default: "
                             "0,1 for write and mixed; 0 for read).")
    parser.add_argument("--thread-counts", default=None,
                        help="Comma-separated thread counts (default: "
                             "1,2,4,8,16).")
    parser.add_argument("--data-root", default=None,
                        help="Override the data output root (default: "
                             "data/memtable_scalability_vs_threads_ondisk_"
                             "small_l2). Use this for sanity checks only, so "
                             "they don't overwrite real results/manifest.json.")
    args = parser.parse_args()

    if args.data_root:
        EXPERIMENT_ROOT = Path(args.data_root).resolve()
        WORKLOAD_SCRATCH = EXPERIMENT_ROOT / "_workload"

    memtables = (args.only.split(",") if args.only
                else list(MEMTABLES.keys()))
    for name in memtables:
        if name not in MEMTABLES:
            sys.exit(f"unknown memtable {name!r}; choices: {list(MEMTABLES)}")

    default_bg_jobs = (WRITE_DEFAULT_BG_JOBS if args.scenario == "write"
                      else MIXED_DEFAULT_BG_JOBS if args.scenario == "mixed"
                      else READ_DEFAULT_BG_JOBS)
    default_unordered_write = (WRITE_DEFAULT_UNORDERED_WRITE
                              if args.scenario == "write"
                              else MIXED_DEFAULT_UNORDERED_WRITE
                              if args.scenario == "mixed"
                              else READ_DEFAULT_UNORDERED_WRITE)
    bg_jobs_list = ([int(x) for x in args.bg_jobs.split(",")]
                    if args.bg_jobs else default_bg_jobs)
    unordered_write_list = ([bool(int(x)) for x in
                             args.unordered_write.split(",")]
                           if args.unordered_write else default_unordered_write)
    thread_counts = ([int(x) for x in args.thread_counts.split(",")]
                     if args.thread_counts else THREAD_COUNTS)

    if not args.no_build:
        build_binaries()

    write_manifest(args.scenario, bg_jobs_list, unordered_write_list,
                  thread_counts)

    if args.scenario == "write":
        run_write_scenario(memtables, bg_jobs_list, unordered_write_list,
                          thread_counts)
    elif args.scenario == "read":
        run_read_scenario(memtables, bg_jobs_list, unordered_write_list,
                         thread_counts)
    else:
        run_mixed_scenario(memtables, bg_jobs_list, unordered_write_list,
                          thread_counts)


if __name__ == "__main__":
    main()
