#!/usr/bin/env python3
import os, pandas as pd, matplotlib.pyplot as plt
from typing import Dict, List

HERE = os.path.abspath(os.path.dirname(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
CRAWL_ROOT = os.path.join(ROOT, ".filter_result_metadata")
EXTRACT_DIR = os.path.join(HERE, "extracted_data")
PLOTS_ROOT = os.path.join(ROOT, "plots")
for d in (EXTRACT_DIR, PLOTS_ROOT):
    os.makedirs(d, exist_ok=True)

BUFFER_MB = 8
BYTES_PER_MB = 1_048_576
VALID_ENTRY_SIZES = [8, 16, 32, 64, 128, 256, 512, 1024]
PAGE_BUCKETS = {2048: "2kb", 4096: "4kb", 8192: "8kb"}

STYLE: Dict[str, Dict] = {
    "hash_linked_list":          {"color":"purple","marker":"D","linestyle":":"},
    "hash_skip_list":            {"color":"red","marker":"^","linestyle":"--"},
    "skiplist":                  {"color":"blue","marker":"o","linestyle":"-"},
    "AlwayssortedVector":        {"color":"black","marker":"o","linestyle":"-"},
    "UnsortedVector":            {"color":"brown","marker":"s","linestyle":"--"},
    "Vector":                    {"color":"teal","marker":"D","linestyle":"-."},
}

def scan_piece(prefix: str, tokenised: str) -> int:
    for part in tokenised.split("-"):
        if part.startswith(prefix) and part[1:].isdigit():
            return int(part[1:])
    return 0

def first_total(path: str) -> int:
    with open(path) as fh:
        for ln in fh:
            if '"total_data_size":' in ln and not any(
               bad in ln for bad in ("max_total_data_size",
                                     "hard_total_data_size",
                                     "soft_total_data_size")):
                tail = ln.split('"total_data_size":',1)[1].lstrip()
                num = tail.split(",",1)[0].split(" ",1)[0]
                return int(num)
    raise ValueError(f'"total_data_size": not found in {path}')

def get_style(name: str) -> Dict:
    for key, st in STYLE.items():
        if name.startswith(key):
            return st
    return {"color":"gray","marker":"x","linestyle":"-"}

records: List[Dict] = []
for algo_dir, _, files in os.walk(CRAWL_ROOT):
    if "LOG1" not in files: continue
    bt = os.path.basename(algo_dir).split("-",1)[0]
    param = os.path.basename(os.path.dirname(algo_dir))
    E = scan_piece("E", param)
    B = scan_piece("B", param)
    bucket = PAGE_BUCKETS.get(B*E,"unknown")
    if bucket=="unknown" or E==0 or B==0: continue
    totals = []
    for run in (1,2,3):
        p = os.path.join(algo_dir,f"LOG{run}")
        if os.path.exists(p): totals.append(first_total(p))
    if len(totals)!=3: continue
    avg = sum(totals)/3
    md = (BUFFER_MB*BYTES_PER_MB-avg)/BYTES_PER_MB
    records.append({"page_bucket":bucket,"buffer_type":bt,
                    "entry_size":E,"metadata_over_mb":md})

df = pd.DataFrame(records)
if df.empty: raise RuntimeError("No data")
df.to_csv(os.path.join(EXTRACT_DIR,"metadata_overhead_all.csv"),index=False)

pos = list(range(len(VALID_ENTRY_SIZES)))
for bucket in ("2kb","4kb","8kb"):
    sub = df[df.page_bucket==bucket]
    if sub.empty: continue
    pivot = sub.groupby(["buffer_type","entry_size"]).metadata_over_mb.mean().unstack()
    fig,ax = plt.subplots(figsize=(9,5))
    for bt in pivot.index:
        ys = [pivot.loc[bt].get(sz,None) for sz in VALID_ENTRY_SIZES]
        ax.plot(pos,ys,label=bt,**get_style(bt))
    ax.set_title(f"Metadata Overhead vs Entry Size ({bucket} page)")
    ax.set_xlabel("Entry size [bytes]")
    ax.set_ylabel("Metadata overhead (MB)")
    ax.set_xticks(pos,[str(x) for x in VALID_ENTRY_SIZES])
    ax.set_ylim(0,8)
    ax.grid(True,linestyle="--",alpha=0.3)
    ax.legend(title="Buffer type",fontsize=8,ncol=2)
    fig.tight_layout()
    out = os.path.join(PLOTS_ROOT,bucket,f"metadata_overhead_{bucket}.pdf")
    os.makedirs(os.path.dirname(out),exist_ok=True)
    fig.savefig(out)
    plt.close(fig)
    print(f"[saved] {out}")
