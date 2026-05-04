mkdir -p /home/cc/LSMMemoryProfiling/concurrency

rsync -av \
  --prune-empty-dirs \
  --include '*/' \
  --exclude 'workload.txt' \
  --exclude '.DS_Store' \
  --exclude 'Icon?' \
  --exclude 'stats.log' \
  /home/cc/LSMMemoryProfiling/.vstats/ \
  /home/cc/LSMMemoryProfiling/concurrency/



  # --exclude 'run1.log' \
  # --exclude 'run2.log' \
  # --exclude 'run3.log' \


