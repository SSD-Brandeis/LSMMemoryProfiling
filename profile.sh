#!/bin/bash
sudo sh -c 'echo 0 >/proc/sys/kernel/perf_event_paranoid'
sudo sh -c 'echo 0 >/proc/sys/kernel/kptr_restrict'

if [ ! -d ~/.local/bin/FlameGraph ]; then
  mkdir -p ~/.local/bin/
  git clone https://github.com/brendangregg/FlameGraph.git ~/.local/bin/FlameGraph --depth=1
fi

rm -f ./out.perf ./out.folded ./flamegraph.svg
perf record -F 999 -a -g  ./bin/working_version -I 3050 -E 4096 -m 6 > workload_sorted.log
perf script > out.perf
~/.local/bin/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
~/.local/bin/FlameGraph/flamegraph.pl out.folded > flamegraph.svg
