mkdir -p /home/cc/LSMMemoryProfiling/.filter_result_feb24_linklist_insert

rsync -av \
  --prune-empty-dirs \
  --include '*/' \
  --exclude 'workload.txt' \
  --exclude '.DS_Store' \
  --exclude 'Icon?' \
  --exclude 'stats.log' \
  /home/cc/LSMMemoryProfiling/.results/ \
  /home/cc/LSMMemoryProfiling/.filter_result_feb24_linklist_insert/



  # --exclude 'run1.log' \
  # --exclude 'run2.log' \
  # --exclude 'run3.log' \


