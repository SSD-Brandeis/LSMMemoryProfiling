
import re
from pathlib import Path

_THREADS_RE = re.compile(r"^Threads:\s*(\d+)\s*$")
_TOTAL_OPS_RE = re.compile(r"^Total Ops:\s*(\d+)\s*$")
_OPS_PER_SEC_RE = re.compile(r"^Ops Per Sec:\s*([0-9.eE+-]+)\s*$")
_WORKLOAD_EXEC_TIME_RE = re.compile(r"^Workload Execution Time:\s*(\d+)\s*$")


def read_metrics_from_workload_log(run_dir: Path) -> dict:

    path = run_dir / "workload.log"
    threads = total_ops = ops_per_sec = workload_exec_time_ns = None
    with open(path) as f:
        for line in f:
            line = line.strip()
            m = _THREADS_RE.match(line)
            if m:
                threads = int(m.group(1))
                continue
            m = _TOTAL_OPS_RE.match(line)
            if m:
                total_ops = int(m.group(1))
                continue
            m = _OPS_PER_SEC_RE.match(line)
            if m:
                ops_per_sec = m.group(1)
                continue
            m = _WORKLOAD_EXEC_TIME_RE.match(line)
            if m:
                workload_exec_time_ns = int(m.group(1))
                continue

    missing = [name for name, val in
              [("Threads", threads), ("Total Ops", total_ops),
               ("Ops Per Sec", ops_per_sec),
               ("Workload Execution Time", workload_exec_time_ns)]
              if val is None]
    if missing:
        raise RuntimeError(f"{path}: missing line(s) for {', '.join(missing)}")

    seconds = workload_exec_time_ns / 1e9
    return {
        "threads": str(threads),
        "total_ops": str(total_ops),
        "seconds": f"{seconds:.6f}",
        "ops_per_sec": ops_per_sec,
    }
