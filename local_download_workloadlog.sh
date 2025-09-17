mkdir -p /home/cc/LSMMemoryProfiling/.filter_result_400kbucket

rsync -av \
  --prune-empty-dirs \
  --include '*/' \
  --exclude 'stats.log' \
  --exclude 'workload.txt' \
  --exclude '.DS_Store' \
  --exclude 'Icon?' \
  /home/cc/LSMMemoryProfiling/.result/ \
  /home/cc/LSMMemoryProfiling/.filter_result_400kbucket/



  # --exclude 'run1.log' \
  # --exclude 'run2.log' \
  # --exclude 'run3.log' \

