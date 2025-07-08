mkdir -p /home/cc/LSMMemoryProfiling/.filter_result
rsync -av --include '*/' --include 'workload.log' --exclude '*' /home/cc/LSMMemoryProfiling/.result/7_5_rawop_low_pri_false_default_refill/ /home/cc/LSMMemoryProfiling/.filter_result/7_5_rawop_low_pri_false_default_refill/
