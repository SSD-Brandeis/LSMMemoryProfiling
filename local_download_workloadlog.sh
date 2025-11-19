mkdir -p /home/cc/LSMMemoryProfiling/.filter_result_totalorderflag

rsync -av \
  --prune-empty-dirs \
  --include '*/' \
  --exclude 'workload.txt' \
  --exclude '.DS_Store' \
  --exclude 'Icon?' \
  /home/cc/LSMMemoryProfiling/.result/ \
  /home/cc/LSMMemoryProfiling/.filter_result_totalorderflag/



  # --exclude 'run1.log' \
  # --exclude 'run2.log' \
  # --exclude 'run3.log' \
  # --exclude 'stats.log' \

