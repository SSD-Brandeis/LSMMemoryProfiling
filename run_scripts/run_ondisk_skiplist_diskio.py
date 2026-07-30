#!/usr/bin/env python3

"""Does skiplist's on-disk write throughput actually track the real,
measured disk write bandwidth? Same winning combo as
run_ondisk_bottleneck_sweep_all_memtables.py (bg_jobs=8, mwb=16, buffer=
128MB, 6,000,000 ops), skiplist only, but with the OS-level `iostat` tool
sampling the real block device backing data/ (confirmed via `df -T` to be
/dev/sda3) once per second around each run, so client-observed insert
throughput (converted to MB/s) can be plotted directly against measured
disk write throughput (MB/s).

Note: RocksDB's own --iostat flag is not used for this -- its
IOStatsContext is thread-local and only ever printed from the main thread,
which never itself performs I/O (all writes happen on the client/
background threads), so it has always reported an empty/near-zero result
in this harness. `iostat` (sysstat) instead samples the real block device.
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
EXPERIMENT_ROOT = REPO_ROOT / "data" / "ondisk_skiplist_diskio"
WORKLOAD_SCRATCH = EXPERIMENT_ROOT / "_workload"
RUNNER_SCRIPT = "run_scripts/run_ondisk_skiplist_diskio.py"
PLOT_SCRIPT = "plot_scripts/plot_ondisk_skiplist_diskio.py"

THREAD_COUNTS = [1, 2, 4, 8, 16]
MEMTABLE_NAME = "skiplist"
MEMTABLE_FACTORY_ID = 1
BLOCK_DEVICE = "/dev/sda3"

# Same geometry as run_ondisk_bottleneck_sweep_all_memtables.py (see
# AGENTS.md "Buffer Geometry"): E = real entry size, B*E = 4096B page,
# P derived so P*B*E is exact.
BUFFER_BYTES = 128 * 1024 * 1024
ENTRY_SIZE = 128 + 896
ENTRIES_PER_PAGE = 4
BUFFER_SIZE_IN_PAGES = BUFFER_BYTES // (ENTRIES_PER_PAGE * ENTRY_SIZE)
assert BUFFER_SIZE_IN_PAGES * ENTRIES_PER_PAGE * ENTRY_SIZE == BUFFER_BYTES
BUFFER_GEOMETRY = ["-E", str(ENTRY_SIZE), "-B", str(ENTRIES_PER_PAGE),
                   "-P", str(BUFFER_SIZE_IN_PAGES), "-T", "10"]

BG_JOBS_FIXED = 8
MWBN_FIXED = 16
WRITE_OP_COUNT = 6_000_000
UNORDERED_WRITE = True
LOW_PRI = 1

WRITE_SPEC = EXPERIMENT_ROOT / "write_100pct" / "concurrency_write_skiplist_diskio.spec.json"


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


def combo_flags():
    return ["--bg_jobs", str(BG_JOBS_FIXED),
            "--max_write_buffer_number", str(MWBN_FIXED),
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


def parse_iostat_wkbps(iostat_log: Path) -> float:
    """Averages wkB/s across all but the first sda3 sample line (the first
    is iostat's since-boot cumulative average, not a real-time sample)."""
    lines = [l for l in iostat_log.read_text().splitlines()
            if l.strip().startswith(BLOCK_DEVICE.rsplit("/", 1)[-1])]
    samples = [float(l.split()[8]) for l in lines[1:]]
    return sum(samples) / len(samples) if samples else 0.0


def run_one(scenario_dir, T):
    run_dir = scenario_dir / f"t{T}"
    assert WRITE_OP_COUNT % T == 0
    generate_thread_shards(WRITE_SPEC, run_dir, T, WRITE_OP_COUNT // T)

    iostat_log_path = run_dir / "iostat.log"
    iostat_proc = subprocess.Popen(
        ["iostat", "-d", "-x", BLOCK_DEVICE, "1"],
        stdout=open(iostat_log_path, "w"), stderr=subprocess.DEVNULL)

    flags = BUFFER_GEOMETRY + combo_flags()
    cmd = [str(BIN_DIR / "working_version_mt"), "--threads", str(T),
          "-m", str(MEMTABLE_FACTORY_ID)] + flags + [
          "--stat", "1", "--perf", "1", "--iostat", "1", "--progress", "0"]
    t0 = time.monotonic()
    run(cmd, cwd=run_dir, log_path=run_dir / "rocksdb_stats.log")
    wall = time.monotonic() - t0

    iostat_proc.terminate()
    iostat_proc.wait()
    avg_wkbps = parse_iostat_wkbps(iostat_log_path)

    row = read_metrics_from_workload_log(run_dir)
    row["threads"] = T
    row["insert_mb_per_sec"] = f"{float(row['ops_per_sec']) * ENTRY_SIZE / 1e6:.3f}"
    row["disk_write_mb_per_sec"] = f"{avg_wkbps / 1024:.3f}"
    row["wall_seconds"] = f"{wall:.3f}"
    print(f"  T={T:<2d} ops/s={float(row['ops_per_sec']):>10.1f} "
          f"insert={row['insert_mb_per_sec']:>8s} MB/s "
          f"disk_write={row['disk_write_mb_per_sec']:>8s} MB/s (wall {wall:.1f}s)")

    harvest_and_cleanup(run_dir)
    for t in range(T):
        (run_dir / f"shard_{t}.txt").unlink(missing_ok=True)
    return row


def run_write_scenario(thread_counts):
    scenario_dir = EXPERIMENT_ROOT / "write_100pct"
    scenario_dir.mkdir(parents=True, exist_ok=True)
    results = []
    for T in thread_counts:
        results.append(run_one(scenario_dir, T))
        write_results_csv(scenario_dir / "results.csv", results)


def write_results_csv(path: Path, rows: list):
    fields = ["threads", "total_ops", "seconds", "ops_per_sec",
             "insert_mb_per_sec", "disk_write_mb_per_sec", "wall_seconds"]
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
        "buffer_geometry": BUFFER_GEOMETRY,
        "buffer_bytes": BUFFER_BYTES,
        "op_count": WRITE_OP_COUNT,
        "unordered_write": UNORDERED_WRITE,
        "concurrent_memtable_write": True,
        "block_device_sampled": BLOCK_DEVICE,
        "spec": str(WRITE_SPEC.relative_to(REPO_ROOT)),
        "note": "Tests whether client-observed insert throughput "
            "(ops_per_sec * entry_size, converted to MB/s) tracks the "
            "real, OS-level measured disk write throughput (`iostat -d -x "
            f"{BLOCK_DEVICE} 1`, wkB/s column, averaged over all but the "
            "first -- cumulative-since-boot -- sample). RocksDB's own "
            "--iostat flag is NOT used as the disk-io source: its "
            "IOStatsContext is thread-local and only printed from the "
            "main thread, which never itself performs I/O, so it has "
            "always reported an empty/near-zero result in this harness. "
            "Same winning combo (bg_jobs=8, mwb=16, buffer=128MB, "
            "6,000,000 ops) as run_ondisk_bottleneck_sweep_all_memtables.py. "
            "Caveat: wkB/s reflects ALL write traffic to the shared root "
            "filesystem device, not RocksDB-attributed-only.",
        "harness": {
            "generator": "lib/Tectonic (tectonic-cli)",
            "executor": "working_version_mt (src/run_workload_multithread.cc)",
            "disk_io_sampler": "iostat (sysstat), -d -x, 1s interval",
        },
        "provenance": {
            "runner_script": RUNNER_SCRIPT,
            "plot_script": PLOT_SCRIPT,
            "data_dir": relative_to_repo_or_abs(EXPERIMENT_ROOT),
            "plot_dir": relative_to_repo_or_abs(EXPERIMENT_ROOT / "plots"),
            "artifact_dir": relative_to_repo_or_abs(EXPERIMENT_ROOT),
            "command_setup": {
                "write": "python3 run_scripts/run_ondisk_skiplist_diskio.py",
                "plot": "python3 plot_scripts/plot_ondisk_skiplist_diskio.py",
            },
            "metric_definitions": {
                "ops_per_sec": "total_ops / seconds, parsed from workload.log "
                    "(see run_scripts/workload_log_metrics.py)",
                "insert_mb_per_sec": "ops_per_sec * entry_size (1024B) / 1e6 "
                    "-- client-observed insert throughput converted to MB/s "
                    "for direct comparison against disk_write_mb_per_sec",
                "disk_write_mb_per_sec": "average of iostat -d -x's wkB/s "
                    f"column for {BLOCK_DEVICE}, sampled once per second for "
                    "the run's duration, excluding the first sample "
                    "(cumulative since boot, not a real-time reading), "
                    "converted kB/s -> MB/s",
                "wall_seconds": "end-to-end subprocess wall time as measured "
                    "by the orchestrating Python script",
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
