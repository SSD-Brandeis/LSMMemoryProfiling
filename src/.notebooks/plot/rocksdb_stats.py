import re
from typing import List, Dict, Any

ALL_TICKERS = set([
    "rocksdb.block.cache.miss",
    "rocksdb.block.cache.hit",
    "rocksdb.block.cache.add",
    "rocksdb.block.cache.add.failures",
    "rocksdb.block.cache.index.miss",
    "rocksdb.block.cache.index.hit",
    "rocksdb.block.cache.index.add",
    "rocksdb.block.cache.index.bytes.insert",
    "rocksdb.block.cache.filter.miss",
    "rocksdb.block.cache.filter.hit",
    "rocksdb.block.cache.filter.add",
    "rocksdb.block.cache.filter.bytes.insert",
    "rocksdb.block.cache.data.miss",
    "rocksdb.block.cache.data.hit",
    "rocksdb.block.cache.data.add",
    "rocksdb.block.cache.data.bytes.insert",
    "rocksdb.block.cache.bytes.read",
    "rocksdb.block.cache.bytes.write",
    "rocksdb.block.cache.compression.dict.miss",
    "rocksdb.block.cache.compression.dict.hit",
    "rocksdb.block.cache.compression.dict.add",
    "rocksdb.block.cache.compression.dict.bytes.insert",
    "rocksdb.block.cache.add.redundant",
    "rocksdb.block.cache.index.add.redundant",
    "rocksdb.block.cache.filter.add.redundant",
    "rocksdb.block.cache.data.add.redundant",
    "rocksdb.block.cache.compression.dict.add.redundant",
    "rocksdb.secondary.cache.hits",
    "rocksdb.secondary.cache.filter.hits",
    "rocksdb.secondary.cache.index.hits",
    "rocksdb.secondary.cache.data.hits",
    "rocksdb.compressed.secondary.cache.dummy.hits",
    "rocksdb.compressed.secondary.cache.hits",
    "rocksdb.compressed.secondary.cache.promotions",
    "rocksdb.compressed.secondary.cache.promotion.skips",
    "rocksdb.bloom.filter.useful",
    "rocksdb.bloom.filter.full.positive",
    "rocksdb.bloom.filter.full.true.positive",
    "rocksdb.bloom.filter.prefix.checked",
    "rocksdb.bloom.filter.prefix.useful",
    "rocksdb.bloom.filter.prefix.true.positive",
    "rocksdb.persistent.cache.hit",
    "rocksdb.persistent.cache.miss",
    "rocksdb.sim.block.cache.hit",
    "rocksdb.sim.block.cache.miss",
    "rocksdb.memtable.hit",
    "rocksdb.memtable.miss",
    "rocksdb.l0.hit",
    "rocksdb.l1.hit",
    "rocksdb.l2andup.hit",
    "rocksdb.compaction.key.drop.new",
    "rocksdb.compaction.key.drop.obsolete",
    "rocksdb.compaction.key.drop.range_del",
    "rocksdb.compaction.key.drop.user",
    "rocksdb.compaction.range_del.drop.obsolete",
    "rocksdb.compaction.optimized.del.drop.obsolete",
    "rocksdb.compaction.cancelled",
    "rocksdb.number.keys.written",
    "rocksdb.number.keys.read",
    "rocksdb.number.keys.updated",
    "rocksdb.bytes.written",
    "rocksdb.bytes.read",
    "rocksdb.number.db.seek",
    "rocksdb.number.db.next",
    "rocksdb.number.db.prev",
    "rocksdb.number.db.seek.found",
    "rocksdb.number.db.next.found",
    "rocksdb.number.db.prev.found",
    "rocksdb.db.iter.bytes.read",
    "rocksdb.number.iter.skip",
    "rocksdb.number.reseeks.iteration",
    "rocksdb.num.iterator.created",
    "rocksdb.num.iterator.deleted",
    "rocksdb.no.file.opens",
    "rocksdb.no.file.errors",
    "rocksdb.stall.micros",
    "rocksdb.db.mutex.wait.micros",
    "rocksdb.number.multiget.get",
    "rocksdb.number.multiget.keys.read",
    "rocksdb.number.multiget.bytes.read",
    "rocksdb.number.multiget.keys.found",
    "rocksdb.number.merge.failures",
    "rocksdb.getupdatessince.calls",
    "rocksdb.wal.synced",
    "rocksdb.wal.bytes",
    "rocksdb.write.self",
    "rocksdb.write.other",
    "rocksdb.write.wal",
    "rocksdb.compact.read.bytes",
    "rocksdb.compact.write.bytes",
    "rocksdb.flush.write.bytes",
    "rocksdb.compact.read.marked.bytes",
    "rocksdb.compact.read.periodic.bytes",
    "rocksdb.compact.read.ttl.bytes",
    "rocksdb.compact.write.marked.bytes",
    "rocksdb.compact.write.periodic.bytes",
    "rocksdb.compact.write.ttl.bytes",
    "rocksdb.number.direct.load.table.properties",
    "rocksdb.number.superversion_acquires",
    "rocksdb.number.superversion_releases",
    "rocksdb.number.superversion_cleanups",
    "rocksdb.number.block.compressed",
    "rocksdb.number.block.decompressed",
    "rocksdb.bytes.compressed.from",
    "rocksdb.bytes.compressed.to",
    "rocksdb.bytes.compression_bypassed",
    "rocksdb.bytes.compression.rejected",
    "rocksdb.number.block_compression_bypassed",
    "rocksdb.number.block_compression_rejected",
    "rocksdb.bytes.decompressed.from",
    "rocksdb.bytes.decompressed.to",
    "rocksdb.merge.operation.time.nanos",
    "rocksdb.filter.operation.time.nanos",
    "rocksdb.compaction.total.time.cpu_micros",
    "rocksdb.row.cache.hit",
    "rocksdb.row.cache.miss",
    "rocksdb.read.amp.estimate.useful.bytes",
    "rocksdb.read.amp.total.read.bytes",
    "rocksdb.number.rate_limiter.drains",
    "rocksdb.blobdb.num.put",
    "rocksdb.blobdb.num.write",
    "rocksdb.blobdb.num.get",
    "rocksdb.blobdb.num.multiget",
    "rocksdb.blobdb.num.seek",
    "rocksdb.blobdb.num.next",
    "rocksdb.blobdb.num.prev",
    "rocksdb.blobdb.num.keys.written",
    "rocksdb.blobdb.num.keys.read",
    "rocksdb.blobdb.bytes.written",
    "rocksdb.blobdb.bytes.read",
    "rocksdb.blobdb.write.inlined",
    "rocksdb.blobdb.write.inlined.ttl",
    "rocksdb.blobdb.write.blob",
    "rocksdb.blobdb.write.blob.ttl",
    "rocksdb.blobdb.blob.file.bytes.written",
    "rocksdb.blobdb.blob.file.bytes.read",
    "rocksdb.blobdb.blob.file.synced",
    "rocksdb.blobdb.blob.index.expired.count",
    "rocksdb.blobdb.blob.index.expired.size",
    "rocksdb.blobdb.blob.index.evicted.count",
    "rocksdb.blobdb.blob.index.evicted.size",
    "rocksdb.blobdb.gc.num.files",
    "rocksdb.blobdb.gc.num.new.files",
    "rocksdb.blobdb.gc.failures",
    "rocksdb.blobdb.gc.num.keys.relocated",
    "rocksdb.blobdb.gc.bytes.relocated",
    "rocksdb.blobdb.fifo.num.files.evicted",
    "rocksdb.blobdb.fifo.num.keys.evicted",
    "rocksdb.blobdb.fifo.bytes.evicted",
    "rocksdb.blobdb.cache.miss",
    "rocksdb.blobdb.cache.hit",
    "rocksdb.blobdb.cache.add",
    "rocksdb.blobdb.cache.add.failures",
    "rocksdb.blobdb.cache.bytes.read",
    "rocksdb.blobdb.cache.bytes.write",
    "rocksdb.txn.overhead.mutex.prepare",
    "rocksdb.txn.overhead.mutex.old.commit.map",
    "rocksdb.txn.overhead.duplicate.key",
    "rocksdb.txn.overhead.mutex.snapshot",
    "rocksdb.txn.get.tryagain",
    "rocksdb.files.marked.trash",
    "rocksdb.files.marked.trash.deleted",
    "rocksdb.files.deleted.immediately",
    "rocksdb.error.handler.bg.error.count",
    "rocksdb.error.handler.bg.io.error.count",
    "rocksdb.error.handler.bg.retryable.io.error.count",
    "rocksdb.error.handler.autoresume.count",
    "rocksdb.error.handler.autoresume.retry.total.count",
    "rocksdb.error.handler.autoresume.success.count",
    "rocksdb.memtable.payload.bytes.at.flush",
    "rocksdb.memtable.garbage.bytes.at.flush",
    "rocksdb.verify_checksum.read.bytes",
    "rocksdb.backup.read.bytes",
    "rocksdb.backup.write.bytes",
    "rocksdb.remote.compact.read.bytes",
    "rocksdb.remote.compact.write.bytes",
    "rocksdb.remote.compact.resumed.bytes",
    "rocksdb.hot.file.read.bytes",
    "rocksdb.warm.file.read.bytes",
    "rocksdb.cool.file.read.bytes",
    "rocksdb.cold.file.read.bytes",
    "rocksdb.ice.file.read.bytes",
    "rocksdb.hot.file.read.count",
    "rocksdb.warm.file.read.count",
    "rocksdb.cool.file.read.count",
    "rocksdb.cold.file.read.count",
    "rocksdb.ice.file.read.count",
    "rocksdb.last.level.read.bytes",
    "rocksdb.last.level.read.count",
    "rocksdb.non.last.level.read.bytes",
    "rocksdb.non.last.level.read.count",
    "rocksdb.last.level.seek.filtered",
    "rocksdb.last.level.seek.filter.match",
    "rocksdb.last.level.seek.data",
    "rocksdb.last.level.seek.data.useful.no.filter",
    "rocksdb.last.level.seek.data.useful.filter.match",
    "rocksdb.non.last.level.seek.filtered",
    "rocksdb.non.last.level.seek.filter.match",
    "rocksdb.non.last.level.seek.data",
    "rocksdb.non.last.level.seek.data.useful.no.filter",
    "rocksdb.non.last.level.seek.data.useful.filter.match",
    "rocksdb.block.checksum.compute.count",
    "rocksdb.block.checksum.mismatch.count",
    "rocksdb.multiget.coroutine.count",
    "rocksdb.read.async.micros",
    "rocksdb.async.read.error.count",
    "rocksdb.table.open.prefetch.tail.miss",
    "rocksdb.table.open.prefetch.tail.hit",
    "rocksdb.timestamp.filter.table.checked",
    "rocksdb.timestamp.filter.table.filtered",
    "rocksdb.readahead.trimmed",
    "rocksdb.fifo.max.size.compactions",
    "rocksdb.fifo.ttl.compactions",
    "rocksdb.fifo.change_temperature.compactions",
    "rocksdb.prefetch.bytes",
    "rocksdb.prefetch.bytes.useful",
    "rocksdb.prefetch.hits",
    "rocksdb.footer.corruption.count",
    "rocksdb.file.read.corruption.retry.count",
    "rocksdb.file.read.corruption.retry.success.count",
    "rocksdb.number.wbwi.ingest",
    "rocksdb.sst.user.defined.index.load.fail.count",
])

ALL_HISTOGRAMS = set([
    "rocksdb.db.get.micros",
    "rocksdb.db.write.micros",
    "rocksdb.db.write.total.micros",
    "rocksdb.compaction.times.micros",
    "rocksdb.compaction.times.cpu_micros",
    "rocksdb.subcompaction.setup.times.micros",
    "rocksdb.table.sync.micros",
    "rocksdb.compaction.outfile.sync.micros",
    "rocksdb.wal.file.sync.micros",
    "rocksdb.manifest.file.sync.micros",
    "rocksdb.table.open.io.micros",
    "rocksdb.db.multiget.micros",
    "rocksdb.read.block.compaction.micros",
    "rocksdb.read.block.get.micros",
    "rocksdb.write.raw.block.micros",
    "rocksdb.numfiles.in.singlecompaction",
    "rocksdb.db.seek.micros",
    "rocksdb.db.write.stall",
    "rocksdb.sst.read.micros",
    "rocksdb.file.read.flush.micros",
    "rocksdb.file.read.compaction.micros",
    "rocksdb.file.read.db.open.micros",
    "rocksdb.file.read.get.micros",
    "rocksdb.file.read.multiget.micros",
    "rocksdb.file.read.db.iterator.micros",
    "rocksdb.file.read.verify.db.checksum.micros",
    "rocksdb.file.read.verify.file.checksums.micros",
    "rocksdb.sst.write.micros",
    "rocksdb.file.write.flush.micros",
    "rocksdb.file.write.compaction.micros",
    "rocksdb.file.write.db.open.micros",
    "rocksdb.num.subcompactions.scheduled",
    "rocksdb.bytes.per.read",
    "rocksdb.bytes.per.write",
    "rocksdb.bytes.per.multiget",
    "rocksdb.compression.times.nanos",
    "rocksdb.decompression.times.nanos",
    "rocksdb.read.num.merge_operands",
    "rocksdb.blobdb.key.size",
    "rocksdb.blobdb.value.size",
    "rocksdb.blobdb.write.micros",
    "rocksdb.blobdb.get.micros",
    "rocksdb.blobdb.multiget.micros",
    "rocksdb.blobdb.seek.micros",
    "rocksdb.blobdb.next.micros",
    "rocksdb.blobdb.prev.micros",
    "rocksdb.blobdb.blob.file.write.micros",
    "rocksdb.blobdb.blob.file.read.micros",
    "rocksdb.blobdb.blob.file.sync.micros",
    "rocksdb.blobdb.compression.micros",
    "rocksdb.blobdb.decompression.micros",
    "rocksdb.db.flush.micros",
    "rocksdb.sst.batch.size",
    "rocksdb.multiget.io.batch.size",
    "rocksdb.num.index.and.filter.blocks.read.per.level",
    "rocksdb.num.sst.read.per.level",
    "rocksdb.num.level.read.per.multiget",
    "rocksdb.error.handler.autoresume.retry.count",
    "rocksdb.async.read.bytes",
    "rocksdb.poll.wait.micros",
    "rocksdb.compaction.prefetch.bytes",
    "rocksdb.prefetched.bytes.discarded",
    "rocksdb.async.prefetch.abort.micros",
    "rocksdb.table.open.prefetch.tail.read.bytes",
    "rocksdb.num.op.per.transaction",
    "rocksdb.multiscan.op.prepare.iterators.micros",
])


def parse_rocksdb_log(file_path: str) -> List[Dict[str, Any]]:
    with open(file_path, "r") as f:
        lines = f.readlines()

    phases = []
    current_phase = None
    section = None  # "stats" | "perf" | "io" | None

    for line in lines:
        line = line.strip()

        if line.startswith("Column Family Name:"):
            if current_phase:
                _finalize_phase(current_phase)
                phases.append(current_phase)

            current_phase = _new_phase()
            _parse_cf_header(line, current_phase)
            section = None
            continue

        if current_phase is None:
            continue

        # --- Parse level stats ---
        _parse_level_stats(line, current_phase)

        # --- Parse meta ---
        _parse_meta(line, current_phase)

        # --- Section switches ---
        if line.startswith("[Rocksdb Stats]"):
            section = "stats"
            continue

        if line.startswith("[Perf Context]"):
            section = "perf"
            continue

        if line.startswith("[IO Stats Context]"):
            section = "io"
            continue

        # --- Section-specific parsing ---
        if section == "stats":
            if _parse_ticker(line, current_phase):
                continue
            if _parse_histogram(line, current_phase):
                continue

        elif section == "perf":
            _parse_perf_context(line, current_phase)

        elif section == "io":
            _parse_io_stats(line, current_phase)

    # finalize last phase
    if current_phase:
        _finalize_phase(current_phase)
        phases.append(current_phase)

    return phases


def _new_phase():
    return {
        "meta": {
            "levels": {}
        },
        "tickers": {},
        "histograms": {},
        "perf": {},
        "io": {},
    }


def _parse_cf_header(line: str, phase: Dict):
    # Example:
    # Column Family Name: default, Size: 3313895293 bytes, Files Count: 3450

    parts = [p.strip() for p in line.split(",")]

    phase["meta"]["cf_name"] = parts[0].split(":")[1].strip()
    phase["meta"]["cf_size_bytes"] = int(parts[1].split(":")[1].split()[0])
    phase["meta"]["cf_file_count"] = int(parts[2].split(":")[1])


def _parse_level_stats(line: str, phase: Dict):
    if not line.startswith("Level:"):
        return

    # Level: 0, Files: 14, Size: 9609636 bytes
    parts = [p.strip() for p in line.split(",")]

    level = int(parts[0].split(":")[1])
    files = int(parts[1].split(":")[1])
    size = int(parts[2].split(":")[1].split()[0])

    phase["meta"]["levels"][level] = {
        "files": files,
        "size_bytes": size
    }


def _parse_meta(line: str, phase: Dict):
    def extract():
        return int(line.split(":")[1].strip())

    if line.startswith("Workload Execution Time"):
        phase["meta"]["workload_time"] = extract()
    elif line.startswith("Inserts Execution Time"):
        phase["meta"]["insert_time"] = extract()
    elif line.startswith("Updates Execution Time"):
        phase["meta"]["update_time"] = extract()
    elif line.startswith("PointQuery Execution Time"):
        phase["meta"]["point_query_time"] = extract()
    elif line.startswith("PointDelete Execution Time"):
        phase["meta"]["point_delete_time"] = extract()
    elif line.startswith("RangeQuery Execution Time"):
        phase["meta"]["range_query_time"] = extract()


def _parse_ticker(line: str, phase: Dict) -> bool:
    m = re.match(r"(\S+)\s+COUNT:\s+(\d+)", line)
    if not m:
        return False

    metric, value = m.groups()
    phase["tickers"][metric] = int(value)
    return True


def _parse_histogram(line: str, phase: Dict) -> bool:
    m = re.match(
        r"(\S+)\s+P50\s*:\s*(\S+)\s+P95\s*:\s*(\S+)\s+P99\s*:\s*(\S+)\s+P100\s*:\s*(\S+)\s+COUNT\s*:\s*(\d+)\s+SUM\s*:\s*(\d+)",
        line
    )
    if not m:
        return False

    metric = m.group(1)

    phase["histograms"][metric] = {
        "P50": float(m.group(2)),
        "P95": float(m.group(3)),
        "P99": float(m.group(4)),
        "P100": float(m.group(5)),
        "COUNT": int(m.group(6)),
        "SUM": int(m.group(7)),
    }

    return True


def _parse_perf_context(line: str, phase: Dict):
    # Format: key = value, key = value, ...
    # Values may be empty (e.g. "bloom_filter_useful =")
    for token in line.split(","):
        token = token.strip()
        if "=" not in token:
            continue
        key, _, raw_val = token.partition("=")
        key = key.strip()
        raw_val = raw_val.strip()
        if key and raw_val:
            try:
                phase["perf"][key] = int(raw_val)
            except ValueError:
                phase["perf"][key] = raw_val


def _parse_io_stats(line: str, phase: Dict):
    # Format: key = value, key = value, ...
    for token in line.split(","):
        token = token.strip()
        if "=" not in token:
            continue
        key, _, raw_val = token.partition("=")
        key = key.strip()
        raw_val = raw_val.strip()
        if key and raw_val:
            try:
                phase["io"][key] = int(raw_val)
            except ValueError:
                phase["io"][key] = raw_val


def _finalize_phase(phase: Dict):
    # Fill missing tickers
    for metric in ALL_TICKERS:
        phase["tickers"].setdefault(metric, 0)

    # Fill missing histograms
    for metric in ALL_HISTOGRAMS:
        if metric not in phase["histograms"]:
            phase["histograms"][metric] = {
                "P50": 0.0,
                "P95": 0.0,
                "P99": 0.0,
                "P100": 0.0,
                "COUNT": 0,
                "SUM": 0,
            }


def to_dataframe(phases):
    import pandas as pd

    rows = []

    for i, p in enumerate(phases):
        row = {}

        # meta
        row.update(p["meta"])
        row["phase"] = i

        # flatten levels
        for lvl, data in p["meta"]["levels"].items():
            row[f"level_{lvl}_files"] = data["files"]
            row[f"level_{lvl}_size"] = data["size_bytes"]

        # tickers
        row.update(p["tickers"])

        # histograms
        for metric, stats in p["histograms"].items():
            for stat_name, val in stats.items():
                row[f"{metric}.{stat_name}"] = val

        # perf context (prefix to avoid collisions)
        for key, val in p["perf"].items():
            row[f"perf.{key}"] = val

        # io stats context
        for key, val in p["io"].items():
            row[f"io.{key}"] = val

        rows.append(row)

    return pd.DataFrame(rows)