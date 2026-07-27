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

# 4 GiB write buffer, set directly via -M (DBEnv::SetBufferSize, a 64-bit
# `long`), NOT via E*B*P. DBEnv::GetBufferSize() computes
# buffer_size_in_pages * entries_per_page * entry_size entirely in 32-bit
# unsigned-int arithmetic before widening to size_t -- E=128,B=32,P=1048576
# hits exactly 2^32 and silently wraps to 0, which sets write_buffer_size=0
# and causes RocksDB to flush (and write-stall) almost immediately. -M
# bypasses that expression entirely. E/B/P are kept at the on-disk
# experiment's original small values so arena_block_size (B*E),
# vector_preallocation_size_in_bytes (B*P), and the workload monitor's
# window size (B*P) stay identical/small -- only write_buffer_size itself
# (and derivatives that read it via GetBufferSize(), e.g.
# max_bytes_for_level_base) change.
BUFFER_BYTES = 4 * 1024 * 1024 * 1024
# -E is bumped from 128 (the on-disk experiment's original, record-size-ish
# value) to 32768 for a reason unrelated to write_buffer_size (that's -M's
# job): arena_block_size = B*E (DBEnv::GetBlockSize()) feeds directly into
# RocksDB's ConcurrentArena, whose per-core shard size is
# min(128KB, arena_block_size/8). At the old B*E=4096, each shard was only
# 512 bytes -- room for ~3-4 entries before EVERY thread had to refill from
# one shared, mutex-protected pool, independent of and prior to any
# memtable-rep-level locking. Confirmed via gdb thread sampling (most
# threads parked in ConcurrentArena::Allocate/sched_yield at T=16) and
# fixed empirically: with B*E=32*32768=1MiB (hitting the 128KB shard cap),
# skiplist went from a declining 1.03M ops/s at T=16 back down near T=1's
# 702K, to genuine scaling: 1.58M (T1) -> 3.23M (T16). B is left untouched
# specifically because it also drives vector_preallocation_size_in_bytes
# (B*P) for the vector-family memtables -- bumping E instead of B changes
# only the arena shard size, not their preallocation behavior.
WRITE_BUFFER_GEOMETRY = ["-E", "32768", "-B", "32", "-P", "32768", "-T", "6",
                        "-M", str(BUFFER_BYTES)]
# Same arena-shard-contention fix as WRITE_BUFFER_GEOMETRY above, applied to
# the read scenario's B=4: E bumped from 1024 to 262144 so B*E=4*262144=1MiB
# (hits the 128KB ConcurrentArena shard cap), instead of the original
# B*E=4096 (512-byte shards, ~3-4 entries before a global-mutex refill).
READ_BUFFER_GEOMETRY = ["-E", "262144", "-B", "4", "-P", "32768", "-T", "6",
                       "-M", str(BUFFER_BYTES)]
MIXED_BUFFER_GEOMETRY = WRITE_BUFFER_GEOMETRY

WRITE_DEFAULT_BG_JOBS = [8]
WRITE_DEFAULT_UNORDERED_WRITE = [True]

READ_DEFAULT_BG_JOBS = [8]
READ_DEFAULT_UNORDERED_WRITE = [False]

MIXED_DEFAULT_BG_JOBS = [8]
MIXED_DEFAULT_UNORDERED_WRITE = [False]


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

# All three scenarios use ONE workload size shared identically by every
# memtable in the default 7-memtable list -- including vector/unsorted_vector
# /sorted_vector, whose Get()-under-interleaving and O(N) insert/scan costs
# (see conversation: vectorrep.cc's bucket_snapshot_ resort-on-Get,
# unsortedvectorrep.cc's linear-scan Get, sortedvectorrep.cc's O(N)
# insert-position shift) make the original, much larger read/mixed sizes
# impractically slow for them. Sizes below were picked from a direct T=1
# sanity sweep (scratch dir, not committed) of exactly these three
# memtables, so every number here was actually measured, not extrapolated.
# Comparing memtables against DIFFERENT op counts would not be a valid
# comparison, so there is deliberately no separate/smaller spec for a subset
# of memtables here -- read_100pct/mixed_50_50 each have exactly one spec,
# used by all 7. (A separate, larger-scale follow-up experiment that
# excludes the three vector variants entirely is run from a different
# --data-root; see conversation.)
# Read uses its own in-memory spec: ONE section with TWO groups -- group 0
# is the load (READ_LOAD_OP_COUNT inserts), group 1 is the point queries
# (READ_QUERY_OP_COUNT). Two groups in the SAME section (not one group with
# a "filler" insert, and not two separate sections) is deliberate and
# load-bearing: tectonic emits group 0's ops as one contiguous block
# followed by group 1's as another (so the split into "load" vs "query"
# lines below is still clean), AND point_queries in group 1 correctly
# reference the full diverse keyspace from group 0's inserts -- confirmed
# empirically (169/200 unique keys referenced in a test run). The OLD
# structure (a single group containing a 1-op "filler" insert plus the real
# point_queries) was a real, previously undiscovered bug: RocksDB's own
# schema docs state key-sharing is scoped to the group the operations are
# in, so every one of those point queries only ever referenced that same 1
# filler key, never any of the loaded keys -- confirmed empirically (only 1
# unique key referenced out of 10,000 queries). This affected every prior
# read_100pct result, independent of and prior to any threading concern.
# The load section is replayed via working_version_mt's --load_file (added
# specifically for this), single-threaded, in the SAME process as the timed
# T-threaded query phase -- so nothing needs to persist across a process
# restart, and (since WAL is disabled) no flush is ever needed. The load
# phase is itself timed (workload.log's [load phase] block) but excluded
# from throughput.csv/ops_per_sec/the plots. The query lines (only, not the
# load lines) are then divided across the T thread-shard files -- safe to
# split this way (unlike mixed, below) because every query only references
# the load phase's keys, which are already fully and permanently present
# before any thread starts, so there is no cross-thread ordering dependency.
#
# Write and mixed, by contrast, generate a SEPARATE, INDEPENDENT workload
# per thread (via tectonic-cli's -s/--scale flag, scaling the full spec's
# op counts by 1/T), rather than generating one combined workload and
# chopping it into per-thread shards. This matters for mixed specifically:
# its spec interleaves inserts and point_queries in one group, so a query
# can reference a key inserted just a few lines earlier IN THE SAME STREAM.
# If that combined stream is chopped into contiguous per-thread pieces
# (the old approach), a query assigned to thread A can reference a key
# whose insert landed in thread B's piece -- and since threads run
# concurrently with no synchronization on relative progress, there is no
# guarantee B has executed that insert before A executes the query.
# Confirmed empirically: in a representative case, 159/200 (79.5%) of a
# chopped mixed section's queries referenced a key inserted in a DIFFERENT
# thread's chunk, and the real harness logged hundreds of silent "Not
# Found" results per thread in this experiment's own already-collected
# mixed_50_50 data (a Get() miss doesn't crash -- see
# src/run_workload_multithread.cc:141-143 -- so the run reported a clean
# ops_per_sec despite this). Generating each thread's workload
# independently eliminates this: every query in a thread's own file only
# ever references a key THAT SAME thread inserts earlier in THAT SAME file,
# so correctness no longer depends on any other thread's timing at all.
# Write has no queries, so it was never at risk of this specific bug, but
# uses the same independent-per-thread generation for architectural
# consistency (there is no correctness downside: fresh independent random
# keys per thread are just as valid as one big chopped keyspace for a pure
# insert workload).
#
# Mixed also no longer has a load phase at all: unlike read (whose timed
# phase is pure queries, so it needs pre-existing data to query against),
# mixed's timed phase already contains its own inserts, so tectonic can
# generate a fully self-contained (insert, query) stream per thread with
# nothing to pre-populate -- confirmed empirically (0 unfound keys in an
# insert+point_query group with no preceding section at all).
#
# Specs live alongside the experiment data they produce (not in the shared
# lib/Tectonic/specs/ pool used by unrelated specs), so a human looking at
# e.g. write_100pct/ finds the exact spec that generated its workload right
# there. Deliberately built from EXPERIMENT_ROOT's default value at module
# load time, not re-evaluated -- a --data-root override (used for sanity
# checks, or for the separate larger-scale/vector-excluded follow-up
# experiment) changes where results go, not where these default specs live.
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
    """Splits `lines` into num_threads contiguous, balanced shard files
    named shard_0.txt .. shard_{T-1}.txt inside run_dir. Only safe when
    `lines` has no cross-line ordering dependency that could be split across
    two different threads' files -- i.e. read scenario's pure-query lines
    (every query only depends on the already-complete load phase, never on
    another query/insert line). NOT used for write or mixed -- see
    generate_thread_shards below."""
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
    it into per-thread pieces. See the long comment above WRITE_SPEC for why
    this matters for mixed (and is harmless-but-consistent for write)."""
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

    workload_path = WORKLOAD_SCRATCH / "read_workload.txt"
    generate_workload(READ_SPEC, workload_path)
    with open(workload_path) as f:
        lines = f.readlines()
    # READ_SPEC's two groups (load inserts, then point_queries) are emitted
    # as two clean contiguous blocks -- see the long comment above
    # WRITE_SPEC -- so this is a plain positional split, no filler insert
    # involved anymore.
    load_lines = lines[:READ_LOAD_OP_COUNT]
    query_lines = lines[READ_LOAD_OP_COUNT:]
    assert len(query_lines) == READ_QUERY_OP_COUNT, (
        f"expected {READ_QUERY_OP_COUNT} query lines, got {len(query_lines)}")

    results = []
    for name in memtables:
        factory_id = MEMTABLES[name]

        for T in thread_counts:
            run_dir = scenario_dir / name / f"t{T}"
            run_dir.mkdir(parents=True, exist_ok=True)
            load_path = run_dir / "load.txt"
            load_path.write_text("".join(load_lines))
            write_shards(query_lines, run_dir, T)

            # Single process: --load_file replays load.txt single-threaded
            # right after DB::Open() (before the T query shard threads start
            # and before the throughput timer begins), so the loaded data
            # never leaves this one memtable/process -- no restart, no flush
            # needed. The load phase is itself timed (workload.log's
            # [load phase] block), but throughput.csv/ops_per_sec below
            # covers ONLY the T-threaded query phase, per
            # run_workload_multithread.cc.
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
            row["wall_seconds"] = f"{wall:.3f}"
            results.append(row)
            print(f"  read  {name:16s} T={T:<2d} "
                  f"ops/s={float(row['ops_per_sec']):>10.1f} "
                  f"(wall {wall:.1f}s)")

            load_path.unlink(missing_ok=True)
            harvest_and_cleanup(run_dir)
            for t in range(T):
                (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)

    write_results_csv(scenario_dir / "results.csv", results)


def run_mixed_scenario(memtables, bg_jobs_list, unordered_write_list,
                       thread_counts):
    # Same no-sweep pattern as run_read_scenario (see its comment).
    if len(bg_jobs_list) > 1 or len(unordered_write_list) > 1:
        print("  note: mixed scenario ignores all but the first --bg-jobs / "
              "--unordered-write value (no sweep nesting for mixed)")
    bg_jobs = bg_jobs_list[0]
    unordered_write = unordered_write_list[0]
    mixed_flags = combo_flags(bg_jobs, unordered_write)

    scenario_dir = EXPERIMENT_ROOT / "mixed_50_50"
    scenario_dir.mkdir(parents=True, exist_ok=True)

    total_mixed_ops = MIXED_INSERT_OP_COUNT + MIXED_QUERY_OP_COUNT

    results = []
    for name in memtables:
        factory_id = MEMTABLES[name]

        for T in thread_counts:
            run_dir = scenario_dir / name / f"t{T}"
            assert total_mixed_ops % T == 0, (
                f"total_mixed_ops={total_mixed_ops} not divisible by T={T}")
            generate_thread_shards(MIXED_SPEC, run_dir, T,
                                  total_mixed_ops // T)

            # No load phase, no --load_file: MIXED_SPEC's single insert+
            # point_query group is generated independently per thread (see
            # generate_thread_shards and the long comment above WRITE_SPEC),
            # so each thread's own file is already fully self-contained --
            # every point_query in it references a key that same thread
            # inserts earlier in that same file.
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
            row["wall_seconds"] = f"{wall:.3f}"
            results.append(row)
            print(f"  mixed {name:16s} T={T:<2d} "
                  f"ops/s={float(row['ops_per_sec']):>10.1f} "
                  f"(wall {wall:.1f}s)")

            harvest_and_cleanup(run_dir)
            for t in range(T):
                (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)

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
            "max_background_jobs": "swept 1/8/16 for the write scenario "
                "(rocksdb::Options::max_background_jobs, --bg_jobs); "
                "irrelevant here since no flush ever runs during the "
                "timed window",
            "unordered_write": "swept false/true for the write scenario "
                "(rocksdb::Options::unordered_write, --unordered_write); "
                "requires concurrent_memtable_write=true",
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
            "buffer_sizing": "write_buffer_size is set via -M "
                "(DBEnv::SetBufferSize, a 64-bit long) to exactly 4 GiB, "
                "NOT via E*B*P: DBEnv::GetBufferSize() "
                "(include/db_env.h) computes "
                "buffer_size_in_pages*entries_per_page*entry_size in "
                "32-bit unsigned arithmetic before widening to size_t, so "
                "E=128,B=32,P=1048576 (chosen to hit 4 GiB) wraps to "
                "exactly 0 at 2^32 -- this was caught during the sanity "
                "run below by gdb-attaching a hung process and seeing an "
                "active CompactionJob + a writer blocked on the rate "
                "limiter, i.e. constant flush/stall despite the intended "
                "4 GiB buffer. E/B/P are kept at the on-disk experiment's "
                "small values (only used for arena_block_size, "
                "vector_preallocation_size_in_bytes, and the workload "
                "monitor's window size, all B*E or B*P, safely small)",
            "data_volume": "op counts are scaled down so nominal "
                "key+val bytes stay well under 4 GiB (<10%), leaving "
                "headroom for per-entry memtable overhead (e.g. skiplist "
                "tower pointers) that GetBufferSize() doesn't itself "
                "account for",
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
                             "0,1 for write; 0 for read/mixed).")
    parser.add_argument("--thread-counts", default=None,
                        help="Comma-separated thread counts (default: "
                             "1,2,4,8,16).")
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
