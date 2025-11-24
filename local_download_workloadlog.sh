mkdir -p /home/cc/LSMMemoryProfiling/.filter_result_getpath_interleave

rsync -av \
  --prune-empty-dirs \
  --include '*/' \
  --exclude 'workload.txt' \
  --exclude '.DS_Store' \
  --exclude 'Icon?' \
  /home/cc/LSMMemoryProfiling/.result/ \
  /home/cc/LSMMemoryProfiling/.filter_result_getpath_interleave/



  # --exclude 'run1.log' \
  # --exclude 'run2.log' \
  # --exclude 'run3.log' \
  # --exclude 'stats.log' \

