mkdir -p /home/cc/LSMMemoryProfiling/.filter_result_multiphase_ondisk_3phase_1mb_t6_newwl
rsync -av \
  --prune-empty-dirs \
  --include '*/' \
  --exclude 'workload.txt' \
  --exclude '.DS_Store' \
  --exclude 'Icon?' \
  --exclude 'stats.log' \
  /home/cc/LSMMemoryProfiling/.results/ \
  /home/cc/LSMMemoryProfiling/.filter_result_multiphase_ondisk_3phase_1mb_t6_newwl/



  # --exclude 'run1.log' \
  # --exclude 'run2.log' \
  # --exclude 'run3.log' \
  # --exclude 'stats.log' \

