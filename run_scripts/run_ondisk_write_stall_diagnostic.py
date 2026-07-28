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
EXPERIMENT_ROOT = REPO_ROOT / "data" / "ondisk_write_stall_diagnostic"
WORKLOAD_SCRATCH = EXPERIMENT_ROOT / "_workload"
RUNNER_SCRIPT = "run_scripts/run_ondisk_write_stall_diagnostic.py"
PLOT_SCRIPT = "plot_scripts/plot_ondisk_write_stall_diagnostic.py"

THREAD_COUNTS = [1, 2, 4, 8, 16]
MEMTABLES = {"skiplist": 1, "art": 11}

# Deliberately smaller than the 8 MiB used in ondisk_small, to speed up this
# diagnostic run and to trigger memtable switches (and therefore stalls)
# more often -- this experiment's point is to characterize the stall
# mechanism, not to be directly comparable to the small/scaled experiments.
# P * B * E must equal BUFFER_BYTES (see AGENTS.md "Buffer Geometry"): E is
# the real entry size (24B key + 100B val); P is derived from a target of
# ~2 MiB and only approximates it since 124 does not divide a power of two
# exactly. -M is omitted so write_buffer_size comes from P*B*E.
ENTRY_SIZE = 24 + 100
ENTRIES_PER_PAGE = 4
BUFFER_SIZE_IN_PAGES = round(2 * 1024 * 1024 / (ENTRIES_PER_PAGE * ENTRY_SIZE))
BUFFER_BYTES = BUFFER_SIZE_IN_PAGES * ENTRIES_PER_PAGE * ENTRY_SIZE
WRITE_BUFFER_GEOMETRY = ["-E", str(ENTRY_SIZE), "-B", str(ENTRIES_PER_PAGE),
                        "-P", str(BUFFER_SIZE_IN_PAGES), "-T", "10"]

MAX_WRITE_BUFFER_NUMBER_VALUES = [2, 4, 8]
BG_JOBS_VALUES = [1, 8, 16]
UNORDERED_WRITE = True  # fixed -- the dip was clearest here; not the axis under test
LOW_PRI = 1

WRITE_SPEC = EXPERIMENT_ROOT / "write_100pct" / "concurrency_write_stall_diagnostic.spec.json"
WRITE_OP_COUNT = 300_000


def combo_flags(bg_jobs, max_write_buffer_number):
    return ["--bg_jobs", str(bg_jobs),
            "--max_write_buffer_number", str(max_write_buffer_number),
            "--concurrent_memtable_write", "1",
            "--unordered_write", "1" if UNORDERED_WRITE else "0",
            "--lowpri", str(LOW_PRI)]


def combo_tag(max_write_buffer_number, bg_jobs):
    return f"mwbn{max_write_buffer_number}_bg{bg_jobs}"


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


def count_stalls(run_dir: Path) -> int:
    log_path = run_dir / "LOG"
    if not log_path.exists():
        return 0
    text = log_path.read_text(errors="replace")
    return text.count("Stopping writes because we have")


def run_write_scenario(memtables, thread_counts):
    scenario_dir = EXPERIMENT_ROOT / "write_100pct"
    scenario_dir.mkdir(parents=True, exist_ok=True)

    results = []
    for mwbn in MAX_WRITE_BUFFER_NUMBER_VALUES:
        for bg_jobs in BG_JOBS_VALUES:
            tag = combo_tag(mwbn, bg_jobs)
            flags = WRITE_BUFFER_GEOMETRY + combo_flags(bg_jobs, mwbn)
            for name in memtables:
                factory_id = MEMTABLES[name]
                for T in thread_counts:
                    run_dir = scenario_dir / tag / name / f"t{T}"
                    assert WRITE_OP_COUNT % T == 0
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
                    row["max_write_buffer_number"] = mwbn
                    row["max_background_jobs"] = bg_jobs
                    row["wall_seconds"] = f"{wall:.3f}"

                    harvest_and_cleanup(run_dir)
                    row["stall_events"] = count_stalls(run_dir)
                    results.append(row)
                    print(f"  write {tag:10s} {name:8s} T={T:<2d} "
                          f"ops/s={float(row['ops_per_sec']):>10.1f} "
                          f"stalls={row['stall_events']} (wall {wall:.1f}s)")

                    for t in range(T):
                        (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)

    write_results_csv(scenario_dir / "results.csv", results)


def read_throughput(run_dir: Path) -> dict:
    return read_metrics_from_workload_log(run_dir)


def write_results_csv(path: Path, rows: list):
    fields = ["memtable", "threads", "max_write_buffer_number",
              "max_background_jobs", "total_ops", "seconds", "ops_per_sec",
              "wall_seconds", "stall_events"]
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
        "memtables": MEMTABLES,
        "purpose": "Diagnose the ondisk write-throughput dip at higher "
            "thread counts (see conversation) -- sweeps "
            "max_write_buffer_number x max_background_jobs to test whether "
            "raising the memtable-buffering headroom (currently hardcoded "
            "to 2, see include/db_env.h) removes the write-stall-driven "
            "decline.",
        "write_scenario": {
            "thread_counts": thread_counts,
            "max_write_buffer_number_swept": MAX_WRITE_BUFFER_NUMBER_VALUES,
            "bg_jobs_swept": BG_JOBS_VALUES,
            "unordered_write": UNORDERED_WRITE,
            "low_pri": LOW_PRI,
            "spec": str(WRITE_SPEC.relative_to(REPO_ROOT)),
            "op_count": WRITE_OP_COUNT,
            "buffer_geometry": WRITE_BUFFER_GEOMETRY,
            "buffer_bytes": BUFFER_BYTES,
        },
        "provenance": {
            "runner_script": RUNNER_SCRIPT,
            "plot_script": PLOT_SCRIPT,
            "data_dir": relative_to_repo_or_abs(EXPERIMENT_ROOT),
            "plot_dir": relative_to_repo_or_abs(EXPERIMENT_ROOT / "plots"),
            "command_setup": {
                "write": "python3 run_scripts/run_ondisk_write_stall_diagnostic.py",
                "plot": "python3 plot_scripts/plot_ondisk_write_stall_diagnostic.py",
            },
            "metric_definitions": {
                "ops_per_sec": "total_ops / seconds, parsed from workload.log",
                "stall_events": "count of 'Stopping writes because we have "
                    "N immutable memtables' WARN lines in this run's "
                    "RocksDB LOG -- a direct count of hard write stalls "
                    "triggered by hitting max_write_buffer_number",
                "max_write_buffer_number": "swept 2/4/8 "
                    "(rocksdb::Options::max_write_buffer_number, "
                    "--max_write_buffer_number -- newly added CLI flag, "
                    "see include/parse_arguments.h); RocksDB default "
                    "everywhere else in this codebase is the hardcoded 2 "
                    "in include/db_env.h, with no override until now",
                "max_background_jobs": "swept 1/8/16 "
                    "(rocksdb::Options::max_background_jobs, --bg_jobs)",
                "buffer_bytes": "2 MiB -- deliberately smaller than "
                    "ondisk_small's 8 MiB to keep this diagnostic fast and "
                    "to trigger memtable switches (and stalls) more often",
            },
        },
    }
    EXPERIMENT_ROOT.mkdir(parents=True, exist_ok=True)
    with open(EXPERIMENT_ROOT / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--only", default=None,
                        help="Comma-separated subset of memtable names "
                             "(default: skiplist,art).")
    parser.add_argument("--no-build", action="store_true")
    parser.add_argument("--thread-counts", default=None,
                        help="Comma-separated thread counts (default: "
                             "1,2,4,8,16).")
    args = parser.parse_args()

    memtables = (args.only.split(",") if args.only
                else list(MEMTABLES.keys()))
    for name in memtables:
        if name not in MEMTABLES:
            sys.exit(f"unknown memtable {name!r}; choices: {list(MEMTABLES)}")

    thread_counts = ([int(x) for x in args.thread_counts.split(",")]
                     if args.thread_counts else THREAD_COUNTS)

    if not args.no_build:
        build_binaries()

    write_manifest(thread_counts)
    run_write_scenario(memtables, thread_counts)


if __name__ == "__main__":
    main()
