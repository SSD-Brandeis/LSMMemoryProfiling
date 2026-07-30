#!/bin/bash
set -euo pipefail

DIR=/home/cc/LSMMemoryProfiling/data/disk_raw_benchmark
cd "$DIR"

echo "=== Sustained single-writer test: 1GB chunks, oflag=direct, ~90s total ==="
echo "chunk,seconds,mb_per_sec" > sustained.csv
END=$((SECONDS + 90))
i=0
while [ $SECONDS -lt $END ]; do
  i=$((i+1))
  OUT=$(dd if=/dev/zero of=./sustained_chunk.bin bs=1M count=1024 oflag=direct 2>&1)
  SEC=$(echo "$OUT" | grep -oP '[\d.]+(?= s,)')
  MBPS=$(echo "$OUT" | grep -oP '[\d.]+(?= MB/s)')
  echo "$i,$SEC,$MBPS" >> sustained.csv
  echo "  chunk $i: ${SEC}s, ${MBPS} MB/s"
  rm -f ./sustained_chunk.bin
done

echo
echo "=== Concurrency sweep: N parallel writers, 1GB each, oflag=direct ==="
echo "concurrency,total_mb,total_seconds,aggregate_mb_per_sec" > concurrency.csv
for N in 1 2 4 8 16; do
  T0=$(date +%s.%N)
  pids=()
  for j in $(seq 1 $N); do
    dd if=/dev/zero of=./conc_${N}_${j}.bin bs=1M count=1024 oflag=direct >/tmp/dd_${N}_${j}.log 2>&1 &
    pids+=($!)
  done
  for p in "${pids[@]}"; do wait $p; done
  T1=$(date +%s.%N)
  ELAPSED=$(echo "$T1 - $T0" | bc)
  TOTAL_MB=$((N * 1024))
  AGG=$(echo "scale=1; $TOTAL_MB / $ELAPSED" | bc)
  echo "  N=$N: ${TOTAL_MB}MB in ${ELAPSED}s => ${AGG} MB/s aggregate"
  echo "$N,$TOTAL_MB,$ELAPSED,$AGG" >> concurrency.csv
  rm -f ./conc_${N}_*.bin
  rm -f /tmp/dd_${N}_*.log
done

echo "=== DONE ==="
