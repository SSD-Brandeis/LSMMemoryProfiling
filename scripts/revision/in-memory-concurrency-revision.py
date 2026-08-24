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
EXPERIMENT_ROOT = REPO_ROOT / "data" / "memtable_scalability_vs_threads_inmemory_lowpri0_uw1"
WORKLOAD_SCRATCH = EXPERIMENT_ROOT / "_workload"
RUNNER_SCRIPT = "run_scripts/run_memtable_scalability_inmemory.py"
PLOT_SCRIPT = "plot_scripts/plot_memtable_scalability_inmemory.py"

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

BUFFER_BYTES = 2 * 1024 * 1024 * 1024
ENTRY_SIZE = 32768                                    # -E
ENTRIES_PER_PAGE = 32                                 # -B
BUFFER_SIZE_IN_PAGES = BUFFER_BYTES // (ENTRIES_PER_PAGE * ENTRY_SIZE)  # -P
assert BUFFER_SIZE_IN_PAGES * ENTRIES_PER_PAGE * ENTRY_SIZE == BUFFER_BYTES
BUFFER_GEOMETRY = ["-E", str(ENTRY_SIZE), "-B", str(ENTRIES_PER_PAGE),
                   "-P", str(BUFFER_SIZE_IN_PAGES), "-T", "6"]
WRITE_BUFFER_GEOMETRY = BUFFER_GEOMETRY
READ_BUFFER_GEOMETRY = BUFFER_GEOMETRY
MIXED_BUFFER_GEOMETRY = BUFFER_GEOMETRY

# unordered_write is now fixed at 1 for all three scenarios 

WRITE_DEFAULT_BG_JOBS = [8]
WRITE_DEFAULT_UNORDERED_WRITE = [True]

READ_DEFAULT_BG_JOBS = [8]
READ_DEFAULT_UNORDERED_WRITE = [True]

MIXED_DEFAULT_BG_JOBS = [8]
MIXED_DEFAULT_UNORDERED_WRITE = [True]

REPS = 3


# In-memory experiment: no compaction ever runs during the timed window (see
# module docstring), so there is nothing for compactions to need priority
# over -- pin low_pri=0 (write_options->low_pri = false) so writes are never
# artificially deprioritized. This is the opposite of the on-disk script,
# which sets low_pri=1 since real flush/compaction contends with writes
# there.
LOW_PRI = 0


def combo_flags(bg_jobs, unordered_write):
    return ["--bg_jobs", str(bg_jobs),
            "--concurrent_memtable_write", "1",
            "--unordered_write", "1" if unordered_write else "0",
            "--lowpri", str(LOW_PRI)]


def combo_tag(bg_jobs, unordered_write):
    return f"bg{bg_jobs}_uw{1 if unordered_write else 0}"



WRITE_SPEC = EXPERIMENT_ROOT / "write_100pct" / "concurrency_write_inmemory.spec.json"
READ_SPEC = EXPERIMENT_ROOT / "read_100pct" / "concurrency_read_inmemory.spec.json"
MIXED_SPEC = EXPERIMENT_ROOT / "mixed_50_50" / "concurrency_mixed_inmemory.spec.json"

# Data volumes below are all well under BUFFER_BYTES (4 GiB) even accounting
# for per-entry memtable overhead (skiplist tower pointers etc.), which the
# nominal key+val byte count doesn't include:
#   write:   300_000 * (24+100) ~=  37 MB   (~1% of buffer)
#   read:    100_000 * (24+900) ~=  92 MB   (~2% of buffer)
#   mixed:    20_000 * (24+100) ~= 2.5 MB   (<1% of buffer, insert+query combined)
WRITE_OP_COUNT = 300_000
READ_LOAD_OP_COUNT = 100_000
READ_QUERY_OP_COUNT = 10_000
# Must match concurrency_mixed_inmemory.spec.json's op_counts exactly
# (10_000 insert + 10_000 point_query in its one insert+query group -- no
# load phase, see comment above).
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
    """Reads threads/total_ops/seconds/ops_per_sec directly from
    run_dir/workload.log's "Threads:"/"Total Ops:"/"Workload Execution
    Time:"/"Ops Per Sec:" lines (see workload_log_metrics.py for exactly
    which lines and why) -- NOT from throughput.csv. Plotted data must be
    traceable to a specific line in the raw log a human can open and check."""
    return read_metrics_from_workload_log(run_dir)


def run_write_scenario(memtables, bg_jobs_list, unordered_write_list,
                       thread_counts, reps):
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
                    assert WRITE_OP_COUNT % T == 0, (
                        f"WRITE_OP_COUNT={WRITE_OP_COUNT} not divisible "
                        f"by T={T}")
                    # Shards are generated ONCE per (name, T) and the exact
                    # same content is reused for all `reps` runs -- reps
                    # measure run-to-run system/timing noise on identical
                    # data, not data variance. shard_dir is a scratch
                    # location outside any single rep's run_dir so it isn't
                    # deleted by that rep's own shard cleanup below.
                    shard_dir = scenario_dir / tag / name / f"t{T}" / "_shards"
                    generate_thread_shards(WRITE_SPEC, shard_dir, T,
                                          WRITE_OP_COUNT // T)
                    for rep in range(1, reps + 1):
                        run_dir = scenario_dir / tag / name / f"t{T}" / f"rep{rep}"
                        run_dir.mkdir(parents=True, exist_ok=True)
                        for t in range(T):
                            shutil.copy(shard_dir / f"shard_{t}.txt",
                                       run_dir / f"shard_{t}.txt")

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
                        row["rep"] = rep
                        row["wall_seconds"] = f"{wall:.3f}"
                        results.append(row)
                        print(f"  write {tag:10s} {name:16s} T={T:<2d} "
                              f"rep={rep} "
                              f"ops/s={float(row['ops_per_sec']):>10.1f} "
                              f"(wall {wall:.1f}s)")

                        harvest_and_cleanup(run_dir)
                        for t in range(T):
                            (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)
                    shutil.rmtree(shard_dir, ignore_errors=True)

    write_results_csv(scenario_dir / "results.csv", results, sweep=True)


def run_read_scenario(memtables, bg_jobs_list, unordered_write_list,
                      thread_counts, reps):

    if len(bg_jobs_list) > 1 or len(unordered_write_list) > 1:
        print("  note: read scenario ignores all but the first --bg-jobs / "
              "--unordered-write value (no sweep nesting for reads)")
    bg_jobs = bg_jobs_list[0]
    unordered_write = unordered_write_list[0]
    read_flags = combo_flags(bg_jobs, unordered_write)

    scenario_dir = EXPERIMENT_ROOT / "read_100pct"
    scenario_dir.mkdir(parents=True, exist_ok=True)

    # Workload is generated ONCE (not per rep)
    workload_path = WORKLOAD_SCRATCH / "read_workload.txt"
    generate_workload(READ_SPEC, workload_path)
    with open(workload_path) as f:
        lines = f.readlines()

    load_lines = lines[:READ_LOAD_OP_COUNT]
    query_lines = lines[READ_LOAD_OP_COUNT:]
    assert len(query_lines) == READ_QUERY_OP_COUNT, (
        f"expected {READ_QUERY_OP_COUNT} query lines, got {len(query_lines)}")

    results = []
    for name in memtables:
        factory_id = MEMTABLES[name]

        for T in thread_counts:
            for rep in range(1, reps + 1):
                run_dir = scenario_dir / name / f"t{T}" / f"rep{rep}"
                run_dir.mkdir(parents=True, exist_ok=True)
                load_path = run_dir / "load.txt"
                load_path.write_text("".join(load_lines))
                write_shards(query_lines, run_dir, T)


                cmd = [str(BIN_DIR / "working_version_mt"),
                      "--load_file", str(load_path),
                      "--threads", str(T), "-m", str(factory_id)
                      ] + READ_BUFFER_GEOMETRY + read_flags + [
                      "--stat", "1", "--perf", "1", "--iostat", "1",
                      "--progress", "0"]
                t0 = time.monotonic()
                run(cmd, cwd=run_dir, log_path=run_dir / "rocksdb_stats.log")
                wall = time.monotonic() - t0

                row = read_throughput(run_dir)
                row["memtable"] = name
                row["rep"] = rep
                row["wall_seconds"] = f"{wall:.3f}"
                results.append(row)
                print(f"  read  {name:16s} T={T:<2d} rep={rep} "
                      f"ops/s={float(row['ops_per_sec']):>10.1f} "
                      f"(wall {wall:.1f}s)")

                load_path.unlink(missing_ok=True)
                harvest_and_cleanup(run_dir)
                for t in range(T):
                    (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)

    write_results_csv(scenario_dir / "results.csv", results)


def run_mixed_scenario(memtables, bg_jobs_list, unordered_write_list,
                       thread_counts, reps):
    scenario_dir = EXPERIMENT_ROOT / "mixed_50_50"
    scenario_dir.mkdir(parents=True, exist_ok=True)

    total_mixed_ops = MIXED_INSERT_OP_COUNT + MIXED_QUERY_OP_COUNT

    results = []
    for bg_jobs in bg_jobs_list:
        for unordered_write in unordered_write_list:
            tag = combo_tag(bg_jobs, unordered_write)
            mixed_flags = combo_flags(bg_jobs, unordered_write)
            for name in memtables:
                factory_id = MEMTABLES[name]
                for T in thread_counts:
                    assert total_mixed_ops % T == 0, (
                        f"total_mixed_ops={total_mixed_ops} not divisible "
                        f"by T={T}")
                    # Shards generated ONCE per (name, T), identical content
                    # reused for all `reps` runs -- see the matching comment
                    # in run_write_scenario.
                    shard_dir = scenario_dir / tag / name / f"t{T}" / "_shards"
                    generate_thread_shards(MIXED_SPEC, shard_dir, T,
                                          total_mixed_ops // T)
                    for rep in range(1, reps + 1):
                        run_dir = scenario_dir / tag / name / f"t{T}" / f"rep{rep}"
                        run_dir.mkdir(parents=True, exist_ok=True)
                        for t in range(T):
                            shutil.copy(shard_dir / f"shard_{t}.txt",
                                       run_dir / f"shard_{t}.txt")


                        cmd = [str(BIN_DIR / "working_version_mt"),
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
                        row["rep"] = rep
                        row["wall_seconds"] = f"{wall:.3f}"
                        results.append(row)
                        print(f"  mixed {tag:10s} {name:16s} T={T:<2d} "
                              f"rep={rep} "
                              f"ops/s={float(row['ops_per_sec']):>10.1f} "
                              f"(wall {wall:.1f}s)")

                        harvest_and_cleanup(run_dir)
                        for t in range(T):
                            (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)
                    shutil.rmtree(shard_dir, ignore_errors=True)

    write_results_csv(scenario_dir / "results.csv", results, sweep=True)


def write_results_csv(path: Path, rows: list, sweep: bool = False):
    fields = ["memtable", "threads"]
    if sweep:
        fields += ["max_background_jobs", "unordered_write"]
    fields += ["rep", "total_ops", "seconds", "ops_per_sec", "wall_seconds"]
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
                   thread_counts, reps):
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
        "reps": reps,
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
            "write": "python3 run_scripts/run_memtable_scalability_inmemory.py --scenario write",
            "read": "python3 run_scripts/run_memtable_scalability_inmemory.py --scenario read",
            "mixed": "python3 run_scripts/run_memtable_scalability_inmemory.py --scenario mixed",
            "plot": "python3 plot_scripts/plot_memtable_scalability_inmemory.py",
        },
        "metric_definitions": {
            "ops_per_sec": "total_ops / seconds, where total_ops is the "
                "fixed op count for the run (every thread replays its "
                "shard exactly once) and seconds is however long that "
                "took. Parsed directly from each run's own workload.log "
                "'Threads:'/'Total Ops:'/'Workload Execution Time:'/'Ops "
                "Per Sec:' lines (see run_scripts/workload_log_metrics.py), "
                "NOT from throughput.csv -- both are written from the same "
                "in-memory variables (src/run_workload_multithread.cc:"
                "467-494) so the values agree, but the log is the one "
                "actually read here and the one a human can open to check. "
                "The timer stops BEFORE db->Close() so the mandatory "
                "flush-on-close does not pollute this number",
            "wall_seconds": "end-to-end subprocess wall time for the "
                "run as measured by the orchestrating Python script "
                "(includes DB::Open/Close overhead, unlike 'seconds' "
                "which is the binary's own measurement window)",
            "speedup": "ops_per_sec(T) / ops_per_sec(T=1) for the same "
                "memtable, computed at plot time (not stored in "
                "results.csv)",
            "max_background_jobs": "fixed at 8 for all three scenarios "
                "(rocksdb::Options::max_background_jobs, --bg_jobs); "
                "irrelevant here since no flush ever runs during the "
                "timed window",
            "unordered_write": "fixed at true (1) for all three scenarios "
                "(rocksdb::Options::unordered_write, --unordered_write); "
                "requires concurrent_memtable_write=true. No longer swept "
                "against false -- prior runs with the 0/1 sweep live under "
                "the old data-root memtable_scalability_vs_threads_"
                "inmemory_lowpri0_uw1",
            "reps": "each (memtable, threads) cell is run REPS=3 times "
                "against IDENTICAL workload data (generated/split once per "
                "(name, T) or once for the whole read scenario, then copied "
                "unchanged into each rep's own run_dir) -- reps measure "
                "run-to-run system/timing noise only, not data variance. "
                "plot_scripts/plot_memtable_scalability_inmemory.py "
                "averages ops_per_sec across the reps' rows in results.csv "
                "at plot time (not pre-averaged in the CSV itself, so each "
                "rep's number is still individually traceable to its own "
                "rep{N}/workload.log)",
            "low_pri": "fixed at 0/false for every run in this script "
                "(write_options->low_pri, --lowpri; include/db_env.h's own "
                "default is true) since no compaction ever runs during the "
                "timed window here, so there is nothing for writes to be "
                "deprioritized behind",
            "workload_uniformity": "read_100pct and mixed_50_50 each use "
                "exactly one spec/op-count, applied identically to all 7 "
                "memtables (including vector/unsorted_vector/sorted_vector) "
                "-- sized small enough (see READ_SPEC/MIXED_SPEC comments "
                "in this file) that even the three vector-backed memtables "
                "stay tractable, rather than giving different memtables "
                "different op counts, which would not be a valid "
                "comparison. A separate, larger-scale experiment that "
                "excludes the three vector variants is run from a "
                "different --data-root.",
        },
        "in_memory_notes": {
            "buffer_sizing": "write_buffer_size is set via P*B*E "
                "E=32768, B=32 are now shared identically by all three "
                "scenarios (write, read, mixed), giving B*E=1MB (a "
                "deliberate, empirically-confirmed deviation from the 4KB "
                "default -- see the WRITE_BUFFER_GEOMETRY comment in this "
                "file for the ConcurrentArena shard-contention fix this "
                "avoids). P is solved as BUFFER_BYTES/(B*E)=2048 so P*B*E "
                "lands on exactly 2 GiB (2048*32*32768=2,147,483,648), "
                "verified safely under the 32-bit unsigned-int ceiling "
                "(4,294,967,295) that GetBufferSize()'s fallback "
                "multiplication runs in -- unlike the old 4 GiB/-M setup, "
                "whose equivalent P*B*E product landed exactly on 2^32 and "
                "would have silently wrapped to 0 had -M been relied on "
                "there instead of the P*B*E path used here",
            "data_volume": "op counts are scaled down so nominal "
                "key+val bytes stay well under 2 GiB (<2%, confirmed "
                "empirically via a real flush_started log line: "
                "total_data_size=40,200,000 bytes for the write scenario's "
                "300,000 x (24+100) workload), leaving headroom for "
                "per-entry memtable overhead (e.g. skiplist tower pointers) "
                "that GetBufferSize() doesn't itself account for. The only "
                "flush observed in any run's LOG has flush_reason=\"Get "
                "Live Files\" (the harness's own post-timing stats query, "
                "which flushes by default) -- never a memtable-full flush "
                "from write volume, confirmed by grepping every write_100pct "
                "LOG in the prior 4 GiB run",
            "final_flush_caveat": "avoid_flush_during_shutdown is "
                "hardcoded false in include/db_env.h with no CLI flag, "
                "so db->Close() always flushes whatever remains in the "
                "memtable once, after the timed window ends -- this "
                "experiment is not literally zero-disk-IO end-to-end, "
                "but that unavoidable final flush cannot affect the "
                "measured ops_per_sec",
            "wal": "disableWAL defaults to true (include/db_env.h), so "
                "WAL writes are not a disk-IO confound here either",
        },
    }
    EXPERIMENT_ROOT.mkdir(parents=True, exist_ok=True)
    with open(EXPERIMENT_ROOT / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)


def main():
    global EXPERIMENT_ROOT, WORKLOAD_SCRATCH, WRITE_SPEC, WRITE_OP_COUNT
    global READ_SPEC, READ_LOAD_OP_COUNT, READ_QUERY_OP_COUNT
    global MIXED_SPEC, MIXED_INSERT_OP_COUNT, MIXED_QUERY_OP_COUNT
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
                             "1 for all three scenarios; no longer swept "
                             "against 0).")
    parser.add_argument("--thread-counts", default=None,
                        help="Comma-separated thread counts (default: "
                             "1,2,4,8,16).")
    parser.add_argument("--reps", type=int, default=REPS,
                        help=f"Number of repeated runs per (memtable, "
                             f"threads) cell against IDENTICAL workload "
                             f"data (default: {REPS}).")
    parser.add_argument("--data-root", default=None,
                        help="Override the data output root (default: "
                             "data/memtable_scalability_vs_threads_inmemory_"
                             "lowpri0_uw1). Use this for sanity checks, or "
                             "for a separate larger-scale/vector-excluded "
                             "follow-up experiment, so they don't "
                             "overwrite real results/manifest.json.")
    parser.add_argument("--write-spec", default=None,
                        help="Override WRITE_SPEC path (write scenario "
                             "only). Must be paired with --write-op-count.")
    parser.add_argument("--write-op-count", type=int, default=None,
                        help="Override WRITE_OP_COUNT to match "
                             "--write-spec's op_count.")
    parser.add_argument("--read-spec", default=None,
                        help="Override READ_SPEC path (read scenario only). "
                             "Must be paired with --read-load-op-count and "
                             "--read-query-op-count.")
    parser.add_argument("--read-load-op-count", type=int, default=None,
                        help="Override READ_LOAD_OP_COUNT to match "
                             "--read-spec's first section op_count.")
    parser.add_argument("--read-query-op-count", type=int, default=None,
                        help="Override READ_QUERY_OP_COUNT to match "
                             "--read-spec's second section point_query "
                             "op_count.")
    parser.add_argument("--mixed-spec", default=None,
                        help="Override MIXED_SPEC path (mixed scenario "
                             "only). Must be paired with "
                             "--mixed-insert-op-count/--mixed-query-op-count.")
    parser.add_argument("--mixed-insert-op-count", type=int, default=None,
                        help="Override MIXED_INSERT_OP_COUNT to match "
                             "--mixed-spec's insert op_count.")
    parser.add_argument("--mixed-query-op-count", type=int, default=None,
                        help="Override MIXED_QUERY_OP_COUNT to match "
                             "--mixed-spec's point_query op_count.")
    args = parser.parse_args()

    if args.data_root:
        EXPERIMENT_ROOT = Path(args.data_root).resolve()
        WORKLOAD_SCRATCH = EXPERIMENT_ROOT / "_workload"
    if args.write_spec:
        if args.write_op_count is None:
            sys.exit("--write-spec requires --write-op-count")
        WRITE_SPEC = Path(args.write_spec).resolve()
        WRITE_OP_COUNT = args.write_op_count
    if args.read_spec:
        if args.read_load_op_count is None or args.read_query_op_count is None:
            sys.exit("--read-spec requires --read-load-op-count and "
                     "--read-query-op-count")
        READ_SPEC = Path(args.read_spec).resolve()
        READ_LOAD_OP_COUNT = args.read_load_op_count
        READ_QUERY_OP_COUNT = args.read_query_op_count
    if args.mixed_spec:
        if args.mixed_insert_op_count is None or args.mixed_query_op_count is None:
            sys.exit("--mixed-spec requires --mixed-insert-op-count and "
                     "--mixed-query-op-count")
        MIXED_SPEC = Path(args.mixed_spec).resolve()
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
                  thread_counts, args.reps)

    if args.scenario == "write":
        run_write_scenario(memtables, bg_jobs_list, unordered_write_list,
                          thread_counts, args.reps)
    elif args.scenario == "read":
        run_read_scenario(memtables, bg_jobs_list, unordered_write_list,
                         thread_counts, args.reps)
    else:
        run_mixed_scenario(memtables, bg_jobs_list, unordered_write_list,
                          thread_counts, args.reps)


if __name__ == "__main__":
    main()
