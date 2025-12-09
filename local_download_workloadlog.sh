mkdir -p /home/cc/LSMMemoryProfiling/.filter_result_compare_vec_variants_get_internal_debug

rsync -av \
  --prune-empty-dirs \
  --include '*/' \
  --exclude 'workload.txt' \
  --exclude '.DS_Store' \
  --exclude 'Icon?' \
  /home/cc/LSMMemoryProfiling/.result/ \
  /home/cc/LSMMemoryProfiling/.filter_result_compare_vec_variants_get_internal_debug/



  # --exclude 'run1.log' \
  # --exclude 'run2.log' \
  # --exclude 'run3.log' \
  # --exclude 'stats.log' \

