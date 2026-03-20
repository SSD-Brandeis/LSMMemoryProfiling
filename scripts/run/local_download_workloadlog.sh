mkdir -p /home/cc/LSMMemoryProfiling/.filter_result_mar16_commonprefix_fig19D
rsync -av \
  --prune-empty-dirs \
  --include '*/' \
  --exclude 'workload.txt' \
  --exclude '.DS_Store' \
  --exclude 'Icon?' \
  --exclude 'stats.log' \
  /home/cc/LSMMemoryProfiling/.result/ \
  /home/cc/LSMMemoryProfiling/.filter_result_mar16_commonprefix_fig19D/



  # --exclude 'run1.log' \
  # --exclude 'run2.log' \
  # --exclude 'run3.log' \
  # --exclude 'stats.log' \

