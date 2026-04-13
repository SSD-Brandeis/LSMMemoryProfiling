# notebooks/plot/utils.py

from typing import Optional
import re

def buffer_dir(
    buffer: str, prefix_len: Optional[int] = None, bucket_count: Optional[int] = None
) -> str:
    if buffer == "AlwaysSortedVector":
        return "AlwayssortedVector-dynamic"
    elif buffer == "UnsortedVector":
        return "UnsortedVector-dynamic"
    elif buffer == "Vector":
        return "Vector-dynamic"
    elif buffer == "hash_linked_list":
        if prefix_len is None or bucket_count is None:
            raise ValueError("prefix_len and bucket_count must be provided")
        return f"hash_linked_list-X{prefix_len}-H{bucket_count}"
    elif buffer == "hash_linked_list_optimized":
        if prefix_len is None or bucket_count is None:
            raise ValueError("prefix_len and bucket_count must be provided")
        return f"hash_linked_list_optimized-X{prefix_len}-H{bucket_count}"
    elif buffer == "hash_skip_list":
        if prefix_len is None or bucket_count is None:
            raise ValueError("prefix_len and bucket_count must be provided")
        return f"hash_skip_list-X{prefix_len}-H{bucket_count}"
    elif buffer == "skiplist":
        return "skiplist"
    
    # New buffers introduced to the environment
    elif buffer == "vector-dynamic":
        return "vector-dynamic"
    elif buffer == "unsortedvector-dynamic":
        return "unsortedvector-dynamic"
    elif buffer == "alwayssortedVector-dynamic":
        return "alwayssortedVector-dynamic"
    elif buffer == "linkedlist":
        return "linkedlist"
    elif buffer == "simple_skiplist":
        return "simple_skiplist"
    elif buffer == "hash_vector":
        if bucket_count is None:
            raise ValueError("bucket_count must be provided for hash_vector")
        return f"hash_vector-H{bucket_count}"
    else:
        raise ValueError(f"Unknown buffer type: {buffer}")


def process_LOG_file(path: str) -> int:
    """Extract total data size from a LOG file"""
    pattern = re.compile(r'"total_data_size":\s(\d+),')
    with open(path) as fh:
        for line in fh:
            match = pattern.search(line)
            if match:
                return int(match.group(1))
    return 0

def process_workload_log(path: str):
    """Process workload log file to extract relevant metric"""
    pass