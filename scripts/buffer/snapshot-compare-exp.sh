#!/bin/bash
set -e


bash ./scripts/rebuild.sh


TAG=snapshot-compare-exp
ENTRY_SIZE=128
LAMBDA=0.0625          # key = 8B, val = 120B
PAGE_SIZE=4096
BUFFER_SIZE_MB=128
PAGES_PER_FILE=$(( BUFFER_SIZE_MB * 1024 * 1024 / PAGE_SIZE ))   # 32768 pages = 128MB
SIZE_RATIO=6
LOW_PRI=0
ROCKSDB_STATS=1
SHOW_PROGRESS=1

KEY_LEN=$(python3 -c "print(int($ENTRY_SIZE * $LAMBDA))")         # 8
VAL_LEN=$(python3 -c "print(int($ENTRY_SIZE * (1 - $LAMBDA)))")   # 120

ENTRIES_PER_PAGE=$(( PAGE_SIZE / ENTRY_SIZE ))

# Overhead is measured as fraction of total buffer capacity:
#   INSERTS = buffer_bytes * (100 - overhead_pct) / (entry_size * 100)
# Using skiplist's overhead for both implementations so they share one workload.
# TODO: update once you confirm the exact skiplist fill count
SKIPLIST_OVERHEAD_PCT=23
INSERTS=$(( BUFFER_SIZE_MB * 1024 * 1024 * (100 - SKIPLIST_OVERHEAD_PCT) / (ENTRY_SIZE * 100) ))

POINT_QUERIES=10000    # issued after buffer is filled; snapshot_ns printed per PQ in vector

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
BIN="$REPO_ROOT/bin/working_version"
TECTONIC_CLI="$REPO_ROOT/bin/tectonic-cli"

BASE_DIR="$REPO_ROOT/.vstats/${TAG}"
mkdir -p "$BASE_DIR/unsortedvector-preallocated"
mkdir -p "$BASE_DIR/vector-preallocated"
mkdir -p "$BASE_DIR/sortedvector-preallocated"
mkdir -p "$BASE_DIR/skiplist"

echo -e "\n========================================"
echo "TAG              : $TAG"
echo "ENTRY_SIZE       : $ENTRY_SIZE B  (key=${KEY_LEN}B  val=${VAL_LEN}B)"
echo "BUFFER           : ${BUFFER_SIZE_MB}MB  (PAGES_PER_FILE=$PAGES_PER_FILE)"
echo "INSERTS          : $INSERTS  (skiplist overhead=${SKIPLIST_OVERHEAD_PCT}%)"
echo "POINT_QUERIES    : $POINT_QUERIES  (after fill)"
echo "SIZE_RATIO       : $SIZE_RATIO"
echo -e "========================================\n"


########################################
# Workload: fill buffer with inserts (group 1), then 10K non-empty PQs (group 2).
# Separate groups → sequential execution; shared section keyset → PQs hit real keys.
# snapshot_ns is printed to stdout (→ rocksdb_stats.log) by VectorRep::Get
# on every PQ issued against a mutable vector.
########################################
python3 - <<EOF
import json
spec = {
  "sections": [{
    "groups": [
      {
        "inserts": {
          "op_count": $INSERTS,
          "key": {"uniform": {"len": $KEY_LEN}},
          "val": {"uniform": {"len": $VAL_LEN}}
        }
      },
      {
        "point_queries": {
          "op_count": $POINT_QUERIES,
          "selection": {"uniform": {"min": 0, "max": 1}}
        }
      }
    ]
  }]
}
with open("$BASE_DIR/workload.specs.json", "w") as f:
    json.dump(spec, f, indent=2)
print("Wrote workload.specs.json")
EOF

pushd "$BASE_DIR" > /dev/null
"$TECTONIC_CLI" generate -w workload.specs.json
popd > /dev/null

########################################
echo "Running unsortedvector-preallocated..."
cd "$BASE_DIR/unsortedvector-preallocated"
cp "$BASE_DIR/workload.txt" .
"$BIN" \
    --memtable_factory=5 \
    -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
mv db/LOG LOG
rm -rf db workload.txt
cd "$REPO_ROOT"
echo -e "\n"
sleep 5

########################################
echo "Running vector-preallocated..."
cd "$BASE_DIR/vector-preallocated"
cp "$BASE_DIR/workload.txt" .
"$BIN" \
    --memtable_factory=2 \
    -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
mv db/LOG LOG
rm -rf db workload.txt
cd "$REPO_ROOT"
echo -e "\n"
sleep 5

########################################
echo "Running sortedvector-preallocated..."
cd "$BASE_DIR/sortedvector-preallocated"
cp "$BASE_DIR/workload.txt" .
"$BIN" \
    --memtable_factory=6 \
    -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
mv db/LOG LOG
rm -rf db workload.txt
cd "$REPO_ROOT"
echo -e "\n"
sleep 5

########################################
echo "Running skiplist..."
cd "$BASE_DIR/skiplist"
cp "$BASE_DIR/workload.txt" .
"$BIN" \
    --memtable_factory=1 \
    -E "$ENTRY_SIZE" -B "$ENTRIES_PER_PAGE" -P "$PAGES_PER_FILE" -T "$SIZE_RATIO" \
    --lowpri "$LOW_PRI" --stat "$ROCKSDB_STATS" --progress "$SHOW_PROGRESS" > rocksdb_stats.log
mv db/LOG LOG
rm -rf db workload.txt
cd "$REPO_ROOT"
echo -e "\n"


cd "$REPO_ROOT"
echo "Done."
echo "snapshot_ns lines → .vstats/${TAG}/vector-preallocated/rocksdb_stats.log"
echo "Snapshot compare experiments finished."


source .env

SLACK_WEBHOOK_URL=${SLACK_WEBHOOK_URL}
HOSTNAME=$(hostname)

MESSAGE="snapshot-compare Experiments Completed on ${HOSTNAME}"
PAYLOAD="{\"text\": \"${MESSAGE}\"}"

curl -X POST -H 'Content-type: application/json' --data "${PAYLOAD}" ${SLACK_WEBHOOK_URL}



## ==============    USE THE BELOW vectorrep.cc for capturing snapshot time in snapshot.log file ----

# //  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
# //  This source code is licensed under both the GPLv2 (found in the
# //  COPYING file in the root directory) and Apache 2.0 License
# //  (found in the LICENSE.Apache file in the root directory).
# //
# #include <algorithm>
# #include <chrono>
# #include <cstdio>
# #include <memory>
# #include <set>
# #include <type_traits>
# #include <unordered_set>

# #include "db/memtable.h"
# #include "memory/arena.h"
# #include "memtable/stl_wrappers.h"
# #include "port/port.h"
# #include "rocksdb/memtablerep.h"
# #include "rocksdb/utilities/options_type.h"
# #include "util/mutexlock.h"

# namespace ROCKSDB_NAMESPACE {
# namespace {

# class VectorRep : public MemTableRep {
#  public:
#   VectorRep(const KeyComparator& compare, Allocator* allocator, size_t count);

#   // Insert key into the collection. (The caller will pack key and value into a
#   // single buffer and pass that in as the parameter to Insert)
#   // REQUIRES: nothing that compares equal to key is currently in the
#   // collection.
#   void Insert(KeyHandle handle) override;

#   void InsertConcurrently(KeyHandle handle) override;

#   // Returns true iff an entry that compares equal to key is in the collection.
#   bool Contains(const char* key) const override;

#   void MarkReadOnly() override;

#   size_t ApproximateMemoryUsage() override;

#   void Get(const LookupKey& k, void* callback_args,
#            bool (*callback_func)(void* arg, const char* entry)) override;

#   void BatchPostProcess() override;

#   ~VectorRep() override;

#   class Iterator : public MemTableRep::Iterator {
#     class VectorRep* vrep_;
#     std::shared_ptr<std::vector<const char*>> bucket_;
#     std::vector<const char*>::const_iterator mutable cit_;
#     const KeyComparator& compare_;
#     std::string tmp_;  // For passing to EncodeKey
#     bool mutable sorted_;
#     void DoSort() const;

#    public:
#     explicit Iterator(class VectorRep* vrep,
#                       std::shared_ptr<std::vector<const char*>> bucket,
#                       const KeyComparator& compare);

#     // Initialize an iterator over the specified collection.
#     // The returned iterator is not valid.
#     // explicit Iterator(const MemTableRep* collection);
#     ~Iterator() override = default;

#     // Returns true iff the iterator is positioned at a valid node.
#     bool Valid() const override;

#     // Returns the key at the current position.
#     // REQUIRES: Valid()
#     const char* key() const override;

#     // Advances to the next position.
#     // REQUIRES: Valid()
#     void Next() override;

#     // Advances to the previous position.
#     // REQUIRES: Valid()
#     void Prev() override;

#     // Advance to the first entry with a key >= target
#     void Seek(const Slice& user_key, const char* memtable_key) override;

#     // Seek and do some memory validation
#     Status SeekAndValidate(const Slice& internal_key, const char* memtable_key,
#                            bool allow_data_in_errors,
#                            bool detect_key_out_of_order,
#                            const std::function<Status(const char*, bool)>&
#                                key_validation_callback) override;

#     // Advance to the first entry with a key <= target
#     void SeekForPrev(const Slice& user_key, const char* memtable_key) override;

#     // Position at the first entry in collection.
#     // Final state of iterator is Valid() iff collection is not empty.
#     void SeekToFirst() override;

#     // Position at the last entry in collection.
#     // Final state of iterator is Valid() iff collection is not empty.
#     void SeekToLast() override;
#   };

#   // Return an iterator over the keys in this representation.
#   MemTableRep::Iterator* GetIterator(Arena* arena) override;

#  private:
#   friend class Iterator;
#   ALIGN_AS(CACHE_LINE_SIZE) RelaxedAtomic<size_t> bucket_size_;
#   using Bucket = std::vector<const char*>;
#   std::shared_ptr<Bucket> bucket_;
#   mutable port::RWMutex rwlock_;
#   bool immutable_;
#   bool sorted_;
#   const KeyComparator& compare_;
#   FILE* snapshot_log_;
#   // Thread-local vector to buffer concurrent writes.
#   using TlBucket = std::vector<const char*>;
#   ThreadLocalPtr tl_writes_;

#   static void DeleteTlBucket(void* ptr) {
#     auto* v = static_cast<TlBucket*>(ptr);
#     delete v;
#   }
# };

# void VectorRep::Insert(KeyHandle handle) {
#   auto* key = static_cast<char*>(handle);
#   {
#     WriteLock l(&rwlock_);
#     assert(!immutable_);
#     bucket_->push_back(key);
#   }
#   bucket_size_.FetchAddRelaxed(1);
# }

# void VectorRep::InsertConcurrently(KeyHandle handle) {
#   auto* v = static_cast<TlBucket*>(tl_writes_.Get());
#   if (!v) {
#     v = new TlBucket();
#     tl_writes_.Reset(v);
#   }
#   v->push_back(static_cast<char*>(handle));
# }

# // Returns true iff an entry that compares equal to key is in the collection.
# bool VectorRep::Contains(const char* key) const {
#   ReadLock l(&rwlock_);
#   return std::find(bucket_->begin(), bucket_->end(), key) != bucket_->end();
# }

# void VectorRep::MarkReadOnly() {
#   WriteLock l(&rwlock_);
#   immutable_ = true;
# }

# size_t VectorRep::ApproximateMemoryUsage() {
#   return bucket_size_.LoadRelaxed() *
#          sizeof(std::remove_reference<decltype(*bucket_)>::type::value_type);
# }

# void VectorRep::BatchPostProcess() {
#   auto* v = static_cast<TlBucket*>(tl_writes_.Get());
#   if (v) {
#     {
#       WriteLock l(&rwlock_);
#       assert(!immutable_);
#       for (auto& key : *v) {
#         bucket_->push_back(key);
#       }
#     }
#     bucket_size_.FetchAddRelaxed(v->size());
#     delete v;
#     tl_writes_.Reset(nullptr);
#   }
# }

# VectorRep::VectorRep(const KeyComparator& compare, Allocator* allocator,
#                      size_t count)
#     : MemTableRep(allocator),
#       bucket_size_(0),
#       bucket_(new Bucket()),
#       immutable_(false),
#       sorted_(false),
#       compare_(compare),
#       snapshot_log_(fopen("snapshot_ns.log", "a")),
#       tl_writes_(DeleteTlBucket) {
#   bucket_.get()->reserve(count);
# }

# VectorRep::~VectorRep() {
#   if (snapshot_log_) {
#     fflush(snapshot_log_);
#     fclose(snapshot_log_);
#   }
# }

# VectorRep::Iterator::Iterator(class VectorRep* vrep,
#                               std::shared_ptr<std::vector<const char*>> bucket,
#                               const KeyComparator& compare)
#     : vrep_(vrep),
#       bucket_(bucket),
#       cit_(bucket_->end()),
#       compare_(compare),
#       sorted_(false) {}

# void VectorRep::Iterator::DoSort() const {
#   // vrep is non-null means that we are working on an immutable memtable
#   if (!sorted_ && vrep_ != nullptr) {
#     WriteLock l(&vrep_->rwlock_);
#     if (!vrep_->sorted_) {
#       std::sort(bucket_->begin(), bucket_->end(),
#                 stl_wrappers::Compare(compare_));
#       cit_ = bucket_->begin();
#       vrep_->sorted_ = true;
#     }
#     sorted_ = true;
#   }
#   if (!sorted_) {
#     std::sort(bucket_->begin(), bucket_->end(),
#               stl_wrappers::Compare(compare_));
#     cit_ = bucket_->begin();
#     sorted_ = true;
#   }
#   assert(sorted_);
#   assert(vrep_ == nullptr || vrep_->sorted_);
# }

# // Returns true iff the iterator is positioned at a valid node.
# bool VectorRep::Iterator::Valid() const {
#   DoSort();
#   return cit_ != bucket_->end();
# }

# // Returns the key at the current position.
# // REQUIRES: Valid()
# const char* VectorRep::Iterator::key() const {
#   assert(sorted_);
#   return *cit_;
# }

# // Advances to the next position.
# // REQUIRES: Valid()
# void VectorRep::Iterator::Next() {
#   assert(sorted_);
#   if (cit_ == bucket_->end()) {
#     return;
#   }
#   ++cit_;
# }

# // Advances to the previous position.
# // REQUIRES: Valid()
# void VectorRep::Iterator::Prev() {
#   assert(sorted_);
#   if (cit_ == bucket_->begin()) {
#     // If you try to go back from the first element, the iterator should be
#     // invalidated. So we set it to past-the-end. This means that you can
#     // treat the container circularly.
#     cit_ = bucket_->end();
#   } else {
#     --cit_;
#   }
# }

# // Advance to the first entry with a key >= target
# void VectorRep::Iterator::Seek(const Slice& user_key,
#                                const char* memtable_key) {
#   DoSort();
#   // Do binary search to find first value not less than the target
#   const char* encoded_key =
#       (memtable_key != nullptr) ? memtable_key : EncodeKey(&tmp_, user_key);
#   cit_ = std::equal_range(bucket_->begin(), bucket_->end(), encoded_key,
#                           [this](const char* a, const char* b) {
#                             return compare_(a, b) < 0;
#                           })
#              .first;
# }

# Status VectorRep::Iterator::SeekAndValidate(
#     const Slice& /* internal_key */, const char* /* memtable_key */,
#     bool /* allow_data_in_errors */, bool /* detect_key_out_of_order */,
#     const std::function<Status(const char*, bool)>&
#     /* key_validation_callback */) {
#   if (vrep_) {
#     WriteLock l(&vrep_->rwlock_);
#     if (bucket_->begin() == bucket_->end()) {
#       // Memtable is empty
#       return Status::OK();
#     } else {
#       return Status::NotSupported("SeekAndValidate() not implemented");
#     }
#   } else {
#     return Status::NotSupported("SeekAndValidate() not implemented");
#   }
# }

# // Advance to the first entry with a key <= target
# void VectorRep::Iterator::SeekForPrev(const Slice& /*user_key*/,
#                                       const char* /*memtable_key*/) {
#   assert(false);
# }

# // Position at the first entry in collection.
# // Final state of iterator is Valid() iff collection is not empty.
# void VectorRep::Iterator::SeekToFirst() {
#   DoSort();
#   cit_ = bucket_->begin();
# }

# // Position at the last entry in collection.
# // Final state of iterator is Valid() iff collection is not empty.
# void VectorRep::Iterator::SeekToLast() {
#   DoSort();
#   cit_ = bucket_->end();
#   if (bucket_->size() != 0) {
#     --cit_;
#   }
# }

# void VectorRep::Get(const LookupKey& k, void* callback_args,
#                     bool (*callback_func)(void* arg, const char* entry)) {
#   rwlock_.ReadLock();
#   VectorRep* vector_rep;
#   std::shared_ptr<Bucket> bucket;
#   if (immutable_) {
#     vector_rep = this;
#   } else {
#     vector_rep = nullptr;
#     auto snap_start = std::chrono::high_resolution_clock::now();
#     bucket.reset(new Bucket(*bucket_));  // make a copy
#     auto snap_end = std::chrono::high_resolution_clock::now();
#     if (snapshot_log_) {
#       fprintf(snapshot_log_, "SNAP: %ld\n",
#               std::chrono::duration_cast<std::chrono::nanoseconds>(
#                   snap_end - snap_start).count());
#     }
#   }
#   VectorRep::Iterator iter(vector_rep, immutable_ ? bucket_ : bucket, compare_);
#   rwlock_.ReadUnlock();

#   for (iter.Seek(k.user_key(), k.memtable_key().data());
#        iter.Valid() && callback_func(callback_args, iter.key()); iter.Next()) {
#   }
# }

# MemTableRep::Iterator* VectorRep::GetIterator(Arena* arena) {
#   char* mem = nullptr;
#   if (arena != nullptr) {
#     mem = arena->AllocateAligned(sizeof(Iterator));
#   }
#   ReadLock l(&rwlock_);
#   // Do not sort here. The sorting would be done the first time
#   // a Seek is performed on the iterator.
#   if (immutable_) {
#     if (arena == nullptr) {
#       return new Iterator(this, bucket_, compare_);
#     } else {
#       return new (mem) Iterator(this, bucket_, compare_);
#     }
#   } else {
#     std::shared_ptr<Bucket> tmp;
#     tmp.reset(new Bucket(*bucket_));  // make a copy
#     if (arena == nullptr) {
#       return new Iterator(nullptr, tmp, compare_);
#     } else {
#       return new (mem) Iterator(nullptr, tmp, compare_);
#     }
#   }
# }
# }  // namespace

# static std::unordered_map<std::string, OptionTypeInfo> vector_rep_table_info = {
#     {"count",
#      {0, OptionType::kSizeT, OptionVerificationType::kNormal,
#       OptionTypeFlags::kNone}},
# };

# VectorRepFactory::VectorRepFactory(size_t count) : count_(count) {
#   RegisterOptions("VectorRepFactoryOptions", &count_, &vector_rep_table_info);
# }

# MemTableRep* VectorRepFactory::CreateMemTableRep(
#     const MemTableRep::KeyComparator& compare, Allocator* allocator,
#     const SliceTransform*, Logger* /*logger*/) {
#   return new VectorRep(compare, allocator, count_);
# }
# }  // namespace ROCKSDB_NAMESPACE
