#!/usr/bin/env python3

import argparse
import csv
import json
import shutil
import subprocess
import sys
import time
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
BIN_DIR = REPO_ROOT / "bin"
EXPERIMENT_ROOT = REPO_ROOT / "data" / "memtable_scalability_vs_threads"
WORKLOAD_SCRATCH = EXPERIMENT_ROOT / "_workload"
RUNNER_SCRIPT = "run_scripts/run_memtable_scalability.py"
PLOT_SCRIPT = "plot_scripts/plot_memtable_scalability.py"

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

# 128 MB write buffer (E * B * P bytes), matching runexp_correctness.sh's

WRITE_BUFFER_GEOMETRY = ["-E", "128", "-B", "32", "-P", "32768", "-T", "6"]
READ_BUFFER_GEOMETRY = ["-E", "1024", "-B", "4", "-P", "32768", "-T", "6"]


WRITE_DEFAULT_BG_JOBS = [1, 8, 16]
WRITE_DEFAULT_UNORDERED_WRITE = [False, True]

READ_DEFAULT_BG_JOBS = [8]
READ_DEFAULT_UNORDERED_WRITE = [False]


def combo_flags(bg_jobs, unordered_write):
    return ["--bg_jobs", str(bg_jobs),
            "--concurrent_memtable_write", "1",
            "--unordered_write", "1" if unordered_write else "0"]


def combo_tag(bg_jobs, unordered_write):
    return f"bg{bg_jobs}_uw{1 if unordered_write else 0}"

WRITE_SPEC = REPO_ROOT / "lib/Tectonic/specs/concurrency_write.spec.json"
READ_SPEC = REPO_ROOT / "lib/Tectonic/specs/concurrency_read.spec.json"
WRITE_OP_COUNT = 3_000_000
READ_LOAD_OP_COUNT = 1_500_000  
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


def run_write_scenario(memtables, bg_jobs_list, unordered_write_list,
                       thread_counts):
    scenario_dir = EXPERIMENT_ROOT / "write_100pct"
    scenario_dir.mkdir(parents=True, exist_ok=True)

    workload_path = WORKLOAD_SCRATCH / "write_workload.txt"
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
                          "--stat", "0", "--progress", "0"]
                    t0 = time.monotonic()
                    run(cmd, cwd=run_dir, log_path=run_dir / "rocksdb_stats.log")
                    wall = time.monotonic() - t0

                    row = read_throughput_csv(run_dir)
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
                   "--stat", "0", "--progress", "0"]
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
                  "--stat", "0", "--progress", "0"]
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
            "buffer_bytes": 128 * 1024 * 1024,
        })
    else:
        scenario_record.update({
            "spec": str(READ_SPEC.relative_to(REPO_ROOT)),
            "load_op_count": READ_LOAD_OP_COUNT,
            "query_op_count": READ_QUERY_OP_COUNT,
            "buffer_geometry": READ_BUFFER_GEOMETRY,
            "buffer_bytes": 128 * 1024 * 1024,
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
            "write": "python3 run_scripts/run_memtable_scalability.py --scenario write",
            "read": "python3 run_scripts/run_memtable_scalability.py --scenario read",
            "plot": "python3 plot_scripts/plot_memtable_scalability.py",
        },
        "metric_definitions": {
            "ops_per_sec": "total_ops / seconds, where total_ops is the "
                "fixed op count for the run (every thread replays its "
                "shard exactly once) and seconds is however long that "
                "took, as measured/reported by working_version_mt's "
                "throughput.csv",
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
        },
    }
    EXPERIMENT_ROOT.mkdir(parents=True, exist_ok=True)
    with open(EXPERIMENT_ROOT / "manifest.json", "w") as f:
        json.dump(manifest, f, indent=2)


def main():
    global EXPERIMENT_ROOT, WORKLOAD_SCRATCH, WRITE_SPEC, WRITE_OP_COUNT
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", choices=["write", "read"], required=True,
                        help="Run exactly one scenario (run one at a time).")
    parser.add_argument("--only", default=None,
                        help="Comma-separated subset of memtable names "
                             "(default: all 7).")
    parser.add_argument("--no-build", action="store_true",
                        help="Skip the cmake build step.")
    parser.add_argument("--bg-jobs", default=None,
                        help="Comma-separated max_background_jobs values to "
                             "sweep (default: 1,8,16 for write; 8 for read).")
    parser.add_argument("--unordered-write", default=None,
                        help="Comma-separated 0/1 values to sweep (default: "
                             "0,1 for write; 0 for read).")
    parser.add_argument("--thread-counts", default=None,
                        help="Comma-separated thread counts (default: "
                             "1,2,4,8,16).")
    parser.add_argument("--data-root", default=None,
                        help="Override the data output root (default: "
                             "data/memtable_scalability_vs_threads). Use "
                             "this for sanity checks so they don't "
                             "overwrite real results/manifest.json.")
    parser.add_argument("--write-spec", default=None,
                        help="Override WRITE_SPEC path (write scenario "
                             "only). Must be paired with --write-op-count.")
    parser.add_argument("--write-op-count", type=int, default=None,
                        help="Override WRITE_OP_COUNT to match "
                             "--write-spec's op_count.")
    args = parser.parse_args()

    if args.data_root:
        EXPERIMENT_ROOT = Path(args.data_root).resolve()
        WORKLOAD_SCRATCH = EXPERIMENT_ROOT / "_workload"
    if args.write_spec:
        if args.write_op_count is None:
            sys.exit("--write-spec requires --write-op-count")
        WRITE_SPEC = Path(args.write_spec).resolve()
        WRITE_OP_COUNT = args.write_op_count

    memtables = (args.only.split(",") if args.only
                else list(MEMTABLES.keys()))
    for name in memtables:
        if name not in MEMTABLES:
            sys.exit(f"unknown memtable {name!r}; choices: {list(MEMTABLES)}")

    default_bg_jobs = (WRITE_DEFAULT_BG_JOBS if args.scenario == "write"
                      else READ_DEFAULT_BG_JOBS)
    default_unordered_write = (WRITE_DEFAULT_UNORDERED_WRITE
                              if args.scenario == "write"
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
    else:
        run_read_scenario(memtables, bg_jobs_list, unordered_write_list,
                         thread_counts)


if __name__ == "__main__":
    main()
