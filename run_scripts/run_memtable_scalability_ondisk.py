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
EXPERIMENT_ROOT = REPO_ROOT / "data" / "memtable_scalability_vs_threads_ondisk"
WORKLOAD_SCRATCH = EXPERIMENT_ROOT / "_workload"
RUNNER_SCRIPT = "run_scripts/run_memtable_scalability_ondisk.py"
PLOT_SCRIPT = "plot_scripts/plot_memtable_scalability_ondisk.py"

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

# 128 MiB write buffer,
BUFFER_BYTES = 128 * 1024 * 1024
WRITE_BUFFER_GEOMETRY = ["-E", "32768", "-B", "32", "-P", "32768", "-T", "6",
                        "-M", str(BUFFER_BYTES)]

READ_BUFFER_GEOMETRY = ["-E", "262144", "-B", "4", "-P", "32768", "-T", "6",
                       "-M", str(BUFFER_BYTES)]
MIXED_BUFFER_GEOMETRY = WRITE_BUFFER_GEOMETRY


WRITE_DEFAULT_BG_JOBS = [1, 8, 16]
WRITE_DEFAULT_UNORDERED_WRITE = [False, True]

READ_DEFAULT_BG_JOBS = [8]
READ_DEFAULT_UNORDERED_WRITE = [False]

MIXED_DEFAULT_BG_JOBS = [8]
MIXED_DEFAULT_UNORDERED_WRITE = [False]



LOW_PRI = 1


def combo_flags(bg_jobs, unordered_write):
    return ["--bg_jobs", str(bg_jobs),
            "--concurrent_memtable_write", "1",
            "--unordered_write", "1" if unordered_write else "0",
            "--lowpri", str(LOW_PRI)]


def combo_tag(bg_jobs, unordered_write):
    return f"bg{bg_jobs}_uw{1 if unordered_write else 0}"

# sorted_vector is excluded 
WRITE_SPEC = EXPERIMENT_ROOT / "write_100pct" / "concurrency_write.spec.json"
READ_SPEC = EXPERIMENT_ROOT / "read_100pct" / "concurrency_read.spec.json"
MIXED_SPEC = EXPERIMENT_ROOT / "mixed_50_50" / "concurrency_mixed.spec.json"
WRITE_OP_COUNT = 3_000_000
READ_LOAD_OP_COUNT = 1_500_000
READ_QUERY_OP_COUNT = 200_000
# Must match concurrency_mixed.spec.json's op_counts 

MIXED_LOAD_OP_COUNT = 1_000_000
MIXED_INSERT_OP_COUNT = 1_000_000
MIXED_QUERY_OP_COUNT = 1_000_000




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


def read_throughput(run_dir: Path) -> dict:

    return read_metrics_from_workload_log(run_dir)


def run_write_scenario(memtables, bg_jobs_list, unordered_write_list,
                       thread_counts):
    scenario_dir = EXPERIMENT_ROOT / "write_100pct"
    scenario_dir.mkdir(parents=True, exist_ok=True)


    workload_path = WORKLOAD_SCRATCH / f"write_workload_{WRITE_SPEC.stem}.txt"
    generate_workload(WRITE_SPEC, workload_path)
    with open(workload_path) as f:
        lines = f.readlines()
    assert len(lines) == WRITE_OP_COUNT, (
        f"expected {WRITE_OP_COUNT} lines, got {len(lines)}")

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
                    write_shards(lines, run_dir, T)

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
    # No sweep here (see module docstring / WRITE_DEFAULT_* comment) -- the
    # read scenario always uses a single bg_jobs/unordered_write setting, so
    # directory layout stays flat (read_100pct/<memtable>/...), unlike the
    # write scenario's bg<N>_uw<0|1>/<memtable>/... nesting.
    if len(bg_jobs_list) > 1 or len(unordered_write_list) > 1:
        print("  note: read scenario ignores all but the first --bg-jobs / "
              "--unordered-write value (no sweep nesting for reads)")
    bg_jobs = bg_jobs_list[0]
    unordered_write = unordered_write_list[0]
    read_flags = combo_flags(bg_jobs, unordered_write)

    scenario_dir = EXPERIMENT_ROOT / "read_100pct"
    scenario_dir.mkdir(parents=True, exist_ok=True)

    workload_path = WORKLOAD_SCRATCH / f"read_workload_{READ_SPEC.stem}.txt"
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
                   ] + READ_BUFFER_GEOMETRY + read_flags + [
                   "--stat", "1", "--perf", "1", "--iostat", "1",
                   "--progress", "0"]
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

        for T in thread_counts:
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
    # Same no-sweep, load-once-then-copy-per-T pattern as run_read_scenario
    # (see its comment) -- the load phase is real disk I/O here (unlike the
    # in-memory --load_file mechanism), so the same DB directory can be
    # safely copied and reopened (-d 0) for each thread count.
    if len(bg_jobs_list) > 1 or len(unordered_write_list) > 1:
        print("  note: mixed scenario ignores all but the first --bg-jobs / "
              "--unordered-write value (no sweep nesting for mixed)")
    bg_jobs = bg_jobs_list[0]
    unordered_write = unordered_write_list[0]
    mixed_flags = combo_flags(bg_jobs, unordered_write)

    scenario_dir = EXPERIMENT_ROOT / "mixed_50_50"
    scenario_dir.mkdir(parents=True, exist_ok=True)

    # Keyed by MIXED_SPEC's stem (see run_write_scenario's comment) -- lets
    # --mixed-spec point at the smaller vector-variant spec without reusing
    # the regular mixed_workload.txt cache.
    workload_path = WORKLOAD_SCRATCH / f"mixed_workload_{MIXED_SPEC.stem}.txt"
    generate_workload(MIXED_SPEC, workload_path)
    with open(workload_path) as f:
        lines = f.readlines()
    load_lines = lines[:MIXED_LOAD_OP_COUNT]
    mixed_lines = lines[MIXED_LOAD_OP_COUNT:]
    expected_mixed = MIXED_INSERT_OP_COUNT + MIXED_QUERY_OP_COUNT
    assert len(mixed_lines) == expected_mixed, (
        f"expected {expected_mixed} mixed-section lines, got {len(mixed_lines)}")

    results = []
    for name in memtables:
        factory_id = MEMTABLES[name]

        # Load once (single-threaded, fresh DB), reused for every T -- this
        # already forces at least one real flush (~124 MB load vs a 128 MiB
        # buffer).
        load_dir = scenario_dir / name / "load"
        load_dir.mkdir(parents=True, exist_ok=True)
        (load_dir / "workload.txt").write_text("".join(load_lines))
        load_cmd = [str(BIN_DIR / "working_version"), "-m", str(factory_id)
                   ] + MIXED_BUFFER_GEOMETRY + mixed_flags + [
                   "--stat", "1", "--perf", "1", "--iostat", "1",
                   "--progress", "0"]
        print(f"  mixed {name:16s} loading {MIXED_LOAD_OP_COUNT:,} keys ...")
        t0 = time.monotonic()
        run(load_cmd, cwd=load_dir, log_path=load_dir / "rocksdb_stats.log")
        load_wall = time.monotonic() - t0
        print(f"    load done in {load_wall:.1f}s")
        (load_dir / "workload.txt").unlink()
        # Keep db/ under load/ — the T-threaded runs below reopen it directly.
        db_dir = load_dir / "db"
        if (load_dir / "db" / "LOG").exists():
            shutil.copy(str(load_dir / "db" / "LOG"), str(load_dir / "LOG"))

        for T in thread_counts:
            run_dir = scenario_dir / name / f"t{T}"
            run_dir.mkdir(parents=True, exist_ok=True)
            # Reopen the SAME db/ directory the load phase created, without
            # destroying it (-d 0), so every thread count starts from an
            # identical on-disk dataset before its own (real, flush-forcing)
            # insert+query mix runs.
            if (run_dir / "db").exists():
                shutil.rmtree(run_dir / "db")
            shutil.copytree(db_dir, run_dir / "db")
            write_shards(mixed_lines, run_dir, T)

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
            row["wall_seconds"] = f"{wall:.3f}"
            results.append(row)
            print(f"  mixed {name:16s} T={T:<2d} "
                  f"ops/s={float(row['ops_per_sec']):>10.1f} "
                  f"(wall {wall:.1f}s)")

            harvest_and_cleanup(run_dir)
            for t in range(T):
                (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)

        shutil.rmtree(db_dir)

    write_results_csv(scenario_dir / "results.csv", results)


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
    """repo-relative path, or the absolute path if outside REPO_ROOT (e.g.
    --data-root pointed at a scratch dir for a sanity check)."""
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
            "spec": str(MIXED_SPEC.relative_to(REPO_ROOT)),
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
            "write": "python3 run_scripts/run_memtable_scalability_ondisk.py --scenario write",
            "read": "python3 run_scripts/run_memtable_scalability_ondisk.py --scenario read",
            "mixed": "python3 run_scripts/run_memtable_scalability_ondisk.py --scenario mixed",
            "plot": "python3 plot_scripts/plot_memtable_scalability_ondisk.py",
        },
        "metric_definitions": {
            "ops_per_sec": "total_ops / seconds, where total_ops is the "
                "fixed op count for the run (every thread replays its "
                "shard exactly once) and seconds is however long that "
                "took. Parsed directly from each run's own workload.log "
                "'Threads:'/'Total Ops:'/'Workload Execution Time:'/'Ops "
                "Per Sec:' lines (see run_scripts/workload_log_metrics.py), "
            "wall_seconds": "end-to-end subprocess wall time for the "
                "run as measured by the orchestrating Python script "
                "(includes DB::Open/cache-drop overhead, unlike "
                "'seconds' which is the binary's own measurement window)",
            "speedup": "ops_per_sec(T) / ops_per_sec(T=1) for the same "
                "memtable, computed at plot time (not stored in "
                "results.csv)",
            "max_background_jobs": "swept 1/8/16 for the write scenario "
                "(rocksdb::Options::max_background_jobs, --bg_jobs)",
            "unordered_write": "swept false/true for the write scenario "
                "(rocksdb::Options::unordered_write, --unordered_write); "
                "requires concurrent_memtable_write=true",
            "low_pri": "fixed at 1/true for every run in this script "
                "(write_options->low_pri, --lowpri; matches "
                "include/db_env.h's own default) so writes are "
                "deprioritized behind real flush/compaction, which does "
                "run during the timed window here",
        },
    }
    EXPERIMENT_ROOT.mkdir(parents=True, exist_ok=True)
    with open(EXPERIMENT_ROOT / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)


def main():
    global EXPERIMENT_ROOT, WORKLOAD_SCRATCH, WRITE_SPEC, WRITE_OP_COUNT
    global MIXED_SPEC, MIXED_LOAD_OP_COUNT, MIXED_INSERT_OP_COUNT, MIXED_QUERY_OP_COUNT
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
                             "sweep (default: 1,8,16 for write; 8 for "
                             "read/mixed).")
    parser.add_argument("--unordered-write", default=None,
                        help="Comma-separated 0/1 values to sweep (default: "
                             "0,1 for write; 0 for read/mixed).")
    parser.add_argument("--thread-counts", default=None,
                        help="Comma-separated thread counts (default: "
                             "1,2,4,8,16).")
    parser.add_argument("--data-root", default=None,
                        help="Override the data output root (default: "
                             "data/memtable_scalability_vs_threads_ondisk). Use "
                             "this for sanity checks so they don't "
                             "overwrite real results/manifest.json.")
    parser.add_argument("--write-spec", default=None,
                        help="Override WRITE_SPEC path (write scenario "
                             "only). Must be paired with --write-op-count.")
    parser.add_argument("--write-op-count", type=int, default=None,
                        help="Override WRITE_OP_COUNT to match "
                             "--write-spec's op_count.")
    parser.add_argument("--mixed-spec", default=None,
                        help="Override MIXED_SPEC path (mixed scenario "
                             "only). Must be paired with "
                             "--mixed-load-op-count/--mixed-insert-op-count/"
                             "--mixed-query-op-count.")
    parser.add_argument("--mixed-load-op-count", type=int, default=None,
                        help="Override MIXED_LOAD_OP_COUNT to match "
                             "--mixed-spec's first section op_count.")
    parser.add_argument("--mixed-insert-op-count", type=int, default=None,
                        help="Override MIXED_INSERT_OP_COUNT to match "
                             "--mixed-spec's second section insert op_count.")
    parser.add_argument("--mixed-query-op-count", type=int, default=None,
                        help="Override MIXED_QUERY_OP_COUNT to match "
                             "--mixed-spec's second section point_query "
                             "op_count.")
    args = parser.parse_args()

    if args.data_root:
        EXPERIMENT_ROOT = Path(args.data_root).resolve()
        WORKLOAD_SCRATCH = EXPERIMENT_ROOT / "_workload"
    if args.write_spec:
        if args.write_op_count is None:
            sys.exit("--write-spec requires --write-op-count")
        WRITE_SPEC = Path(args.write_spec).resolve()
        WRITE_OP_COUNT = args.write_op_count
    if args.mixed_spec:
        if (args.mixed_load_op_count is None or args.mixed_insert_op_count is None
                or args.mixed_query_op_count is None):
            sys.exit("--mixed-spec requires --mixed-load-op-count, "
                     "--mixed-insert-op-count, and --mixed-query-op-count")
        MIXED_SPEC = Path(args.mixed_spec).resolve()
        MIXED_LOAD_OP_COUNT = args.mixed_load_op_count
        MIXED_INSERT_OP_COUNT = args.mixed_insert_op_count
        MIXED_QUERY_OP_COUNT = args.mixed_query_op_count

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
