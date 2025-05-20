sudo sh -c 'echo 0 >/proc/sys/kernel/perf_event_paranoid'
sudo sh -c 'echo 0 >/proc/sys/kernel/kptr_restrict'

if [ ! -d ~/.local/bin/FlameGraph ]; then
  mkdir -p ~/.local/bin/
  git clone https://github.com/brendangregg/FlameGraph.git ~/.local/bin/FlameGraph --depth=1
fi

rm -f perf.data out.perf out.folded flamegraph.svg

# perf record -F 999 -a -g ./bin/working_version -E 128 -m 1 > workload_sorted.log
perf record -F 999 -g -- ./bin/working_version -E 128 -m 3 > workload_sorted.log
perf script > out.perf



~/.local/bin/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
~/.local/bin/FlameGraph/flamegraph.pl out.folded > flamegraph_hash_skiplist_s_999.svg






#which function accounts for most of the time? 
#which function is responsible for sorting/snapshot/searching the entries after the data structure is sorted? 
#Scalable way to figure out those functions  
#Read path (PQ path), understand for the different vector implementations, how do they perform in terms of overall latency 
# which part of the codebase account for largest part of latency 
# A stacked bar plot for every implmentation to show the different breakdown of the time. 
#If doing PQ through raw operation, vector is only going to be sorted once, which is why do sort only takes a very small portion of time 





# #!/bin/bash
# sudo sh -c 'echo 0 >/proc/sys/kernel/perf_event_paranoid'
# sudo sh -c 'echo 0 >/proc/sys/kernel/kptr_restrict'

# if [ ! -d ~/.local/bin/FlameGraph ]; then
#   mkdir -p ~/.local/bin/
#   git clone https://github.com/brendangregg/FlameGraph.git ~/.local/bin/FlameGraph --depth=1
# fi

# rm -f ./out.perf ./out.folded ./flamegraph.svg
# perf record -F 999 -a -g  ./bin/working_version -I 3050 -E 4096 -m 6 > workload_sorted.log
# perf script > out.perf
# ~/.local/bin/FlameGraph/stackcollapse-perf.pl out.perf > out.folded
# ~/.local/bin/FlameGraph/flamegraph.pl out.folded > flamegraph.svg


