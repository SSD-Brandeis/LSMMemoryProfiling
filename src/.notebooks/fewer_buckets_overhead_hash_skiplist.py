import matplotlib.pyplot as plt

from plot import *


PROJ_DIR = "/Users/shubham/SSD Lab/LSMMemoryProfiling/"
CURR_DIR = Path(__file__).parent

files = [
    # f"{PROJ_DIR}.vstats/sanitycheck-lsmbuffer-concurrent-write-off-WAL-0-1000000/buffer-2/rocksdb_stats.log",
    f"{PROJ_DIR}.vstats/sanitycheck-lsmbuffer-concurrent-write-off-WAL-0-1000000/buffer-3/rocksdb_stats.log",
    f"{PROJ_DIR}.vstats/sanitycheck-lsmbuffer-concurrent-write-off-WAL-0-1000000/buffer-1/rocksdb_stats.log",
]
labels = [
    # "Vector", 
    "hash_skip_list",
    "skiplist", 
]

plt.figure(figsize=(3.5, 2))

for filename, label in zip(files, labels):
    latencies = []

    with open(filename) as f:
        for line in f:
            line = line.strip()
            if line.startswith("I:"):
                latencies.append(int(line.split()[1]) / 10**6)

    print(sum(latencies))
    st = line_styles.get(label, {})
    if "marker" in st:
        del st["marker"]
    if label == "hash_skip_list":
        st['alpha'] = 0.8
    plt.plot(range(len(latencies)), latencies, **st)  # added alpha, removed marker

plt.ylim(0)
plt.xlabel("insert", labelpad=-1)
plt.ylabel("latency (ms)", labelpad=-1)

plt.yticks([0, 10, 20], ["0", "10", "20"])
plt.xticks([0, 500000, 1000000], ["0", "0.5M", "1M"])

# plt.title("Latency vs I Count")
plt.legend(
    loc="upper left",
    bbox_to_anchor=(0.05, 0.97),
    ncol=1,
    frameon=False,
    borderaxespad=0,
    labelspacing=0,
    borderpad=0,
    handlelength=1.0,
)
# plt.tight_layout()
plt.savefig(
    CURR_DIR / f"fewer-buckets-overhead.pdf",
    bbox_inches="tight",
    pad_inches=0.02,
)
print(f"[saved] fewer-buckets-overhead.pdf")

