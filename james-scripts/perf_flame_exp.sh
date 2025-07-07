#!/usr/bin/env bash
set -euo pipefail

# allow unprivileged perf & kernel symbols
sudo sh -c 'echo 0 >/proc/sys/kernel/perf_event_paranoid'
sudo sh -c 'echo 0 >/proc/sys/kernel/kptr_restrict'

# install FlameGraph if missing
FLAMEHOME="$HOME/.local/bin/FlameGraph"
if [ ! -d "$FLAMEHOME" ]; then
  mkdir -p "$(dirname "$FLAMEHOME")"
  git clone https://github.com/brendangregg/FlameGraph.git \
    "$FLAMEHOME" --depth=1
fi

# clean up old data
rm -f perf.data perf_report.txt out.folded flamegraph.svg

# record at 999Hz on all CPUs, using frame pointer
perf record \
  -F 999 \
  -a \
  --call-graph fp \
  --output=perf.data \
  -- \
  ./bin/working_version \
    -E 128 \
    -m 1 

# text report
perf report \
  --input=perf.data \
  --stdio \
  --percent-limit=0.1 \
  --sort comm,dso,symbol,period \
  > perf_report.txt

echo "==> Text report saved to perf_report.txt"

# collapse for flamegraph
perf script --input=perf.data \
  | "$FLAMEHOME"/stackcollapse-perf.pl \
  > out.folded

# render SVG
"$FLAMEHOME"/flamegraph.pl \
  --title="Working_Version CPU FlameGraph" \
  --countname="samples" \
  out.folded \
  > flamegraph.svg

echo "==> FlameGraph generated: flamegraph.svg"
