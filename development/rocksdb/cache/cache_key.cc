//  Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#include "cache/cache_key.h"

#include <algorithm>
#include <atomic>

#include "rocksdb/advanced_cache.h"
#include "table/unique_id_impl.h"
#include "util/hash.h"
#include "util/math.h"

namespace ROCKSDB_NAMESPACE {

// Value space plan for CacheKey:
//
// file_num_etc64_ | offset_etc64_ | Only generated by
// ---------------+---------------+------------------------------------------
//              0 |             0 | Reserved for "empty" CacheKey()
//              0 |  > 0, < 1<<63 | CreateUniqueForCacheLifetime
//              0 |      >= 1<<63 | CreateUniqueForProcessLifetime
//            > 0 |           any | OffsetableCacheKey.WithOffset

CacheKey CacheKey::CreateUniqueForCacheLifetime(Cache *cache) {
  // +1 so that we can reserve all zeros for "unset" cache key
  uint64_t id = cache->NewId() + 1;
  // Ensure we don't collide with CreateUniqueForProcessLifetime
  assert((id >> 63) == 0U);
  return CacheKey(0, id);
}

CacheKey CacheKey::CreateUniqueForProcessLifetime() {
  // To avoid colliding with CreateUniqueForCacheLifetime, assuming
  // Cache::NewId counts up from zero, here we count down from UINT64_MAX.
  // If this ever becomes a point of contention, we could sub-divide the
  // space and use CoreLocalArray.
  static std::atomic<uint64_t> counter{UINT64_MAX};
  uint64_t id = counter.fetch_sub(1, std::memory_order_relaxed);
  // Ensure we don't collide with CreateUniqueForCacheLifetime
  assert((id >> 63) == 1U);
  return CacheKey(0, id);
}

// How we generate CacheKeys and base OffsetableCacheKey, assuming that
// db_session_ids are generated from a base_session_id and
// session_id_counter (by SemiStructuredUniqueIdGen+EncodeSessionId
// in DBImpl::GenerateDbSessionId):
//
// Conceptual inputs:
//   db_id                   (unstructured, from GenerateRawUniqueId or equiv)
//                           * could be shared between cloned DBs but rare
//                           * could be constant, if session id suffices
//   base_session_id         (unstructured, from GenerateRawUniqueId)
//   session_id_counter      (structured)
//                           * usually much smaller than 2**24
//   orig_file_number        (structured)
//                           * usually smaller than 2**24
//   offset_in_file          (structured, might skip lots of values)
//                           * usually smaller than 2**32
//
// Overall approach (see https://github.com/pdillinger/unique_id for
// background):
//
// First, we have three "structured" values, up to 64 bits each, that we
// need to fit, without losses, into 128 bits. In practice, the values will
// be small enough that they should fit. For example, applications generating
// large SST files (large offsets) will naturally produce fewer files (small
// file numbers). But we don't know ahead of time what bounds the values will
// have.
//
// Second, we have unstructured inputs that enable distinct RocksDB processes
// to pick a random point in space, likely very different from others. Xoring
// the structured with the unstructured give us a cache key that is
// structurally distinct between related keys (e.g. same file or same RocksDB
// process) and distinct with high probability between unrelated keys.
//
// The problem of packing three structured values into the space for two is
// complicated by the fact that we want to derive cache keys from SST unique
// IDs, which have already combined structured and unstructured inputs in a
// practically inseparable way. And we want a base cache key that works
// with an offset of any size. So basically, we need to encode these three
// structured values, each up to 64 bits, into 128 bits without knowing any
// of their sizes. The DownwardInvolution() function gives us a mechanism to
// accomplish this. (See its properties in math.h.) Specifically, for inputs
// a, b, and c:
//   lower64 = DownwardInvolution(a) ^ ReverseBits(b);
//   upper64 = c ^ ReverseBits(a);
// The 128-bit output is unique assuming there exist some i, j, and k
// where a < 2**i, b < 2**j, c < 2**k, i <= 64, j <= 64, k <= 64, and
// i + j + k <= 128. In other words, as long as there exist some bounds
// that would allow us to pack the bits of a, b, and c into the output
// if we know the bound, we can generate unique outputs without knowing
// those bounds. To validate this claim, the inversion function (given
// the bounds) has been implemented in CacheKeyDecoder in
// db_block_cache_test.cc.
//
// With that in mind, the outputs in terms of the conceptual inputs look
// like this, using bitwise-xor of the constituent pieces, low bits on left:
//
// |------------------------- file_num_etc64 -------------------------|
// | +++++++++ base_session_id (lower 64 bits, involution) +++++++++ |
// |-----------------------------------------------------------------|
// | session_id_counter (involution) ..... |                         |
// |-----------------------------------------------------------------|
// | hash of: ++++++++++++++++++++++++++++++++++++++++++++++++++++++ |
// |  * base_session_id (upper ~39 bits)                             |
// |  * db_id (~122 bits entropy)                                    |
// |-----------------------------------------------------------------|
// |                             | ..... orig_file_number (reversed) |
// |-----------------------------------------------------------------|
//
//
// |------------------------- offset_etc64 --------------------------|
// | ++++++++++ base_session_id (lower 64 bits, reversed) ++++++++++ |
// |-----------------------------------------------------------------|
// |                           | ..... session_id_counter (reversed) |
// |-----------------------------------------------------------------|
// | offset_in_file ............... |                                |
// |-----------------------------------------------------------------|
//
// Some oddities or inconveniences of this layout are due to deriving
// the "base" cache key (without offset) from the SST unique ID (see
// GetSstInternalUniqueId). Specifically,
// * Lower 64 of base_session_id occurs in both output words (ok but
//   weird)
// * The inclusion of db_id is bad for the conditions under which we
//   can guarantee uniqueness, but could be useful in some cases with
//   few small files per process, to make up for db session id only having
//   ~103 bits of entropy.
//
// In fact, if DB ids were not involved, we would be guaranteed unique
// cache keys for files generated in a single process until total bits for
// biggest session_id_counter, orig_file_number, and offset_in_file
// reach 128 bits.
//
// With the DB id limitation, we only have nice guaranteed unique cache
// keys for files generated in a single process until biggest
// session_id_counter and offset_in_file reach combined 64 bits. This
// is quite good in practice because we can have millions of DB Opens
// with terabyte size SST files, or billions of DB Opens with gigabyte
// size SST files.
//
// One of the considerations in the translation between existing SST unique
// IDs and base cache keys is supporting better SST unique IDs in a future
// format_version. If we use a process-wide file counter instead of
// session counter and file numbers, we only need to combine two 64-bit values
// instead of three. But we don't want to track unique ID versions in the
// manifest, so we want to keep the same translation layer between SST unique
// IDs and base cache keys, even with updated SST unique IDs. If the new
// unique IDs put the file counter where the orig_file_number was, and
// use no structured field where session_id_counter was, then our translation
// layer works fine for two structured fields as well as three (for
// compatibility). The small computation for the translation (one
// DownwardInvolution(), two ReverseBits(), both ~log(64) instructions deep)
// is negligible for computing as part of SST file reader open.
//
// More on how https://github.com/pdillinger/unique_id applies here:
// Every bit of output always includes "unstructured" uniqueness bits and
// often combines with "structured" uniqueness bits. The "unstructured" bits
// change infrequently: only when we cannot guarantee our state tracking for
// "structured" uniqueness hasn't been cloned. Using a static
// SemiStructuredUniqueIdGen for db_session_ids, this means we only get an
// "all new" session id when a new process uses RocksDB. (Between processes,
// we don't know if a DB or other persistent storage has been cloned. We
// assume that if VM hot cloning is used, subsequently generated SST files
// do not interact.) Within a process, only the session_lower of the
// db_session_id changes incrementally ("structured" uniqueness).
//
// This basically means that our offsets, counters and file numbers allow us
// to do somewhat "better than random" (birthday paradox) while in the
// degenerate case of completely new session for each tiny file, we still
// have strong uniqueness properties from the birthday paradox, with ~103
// bit session IDs or up to 128 bits entropy with different DB IDs sharing a
// cache.
//
// More collision probability analysis:
// Suppose a RocksDB host generates (generously) 2 GB/s (10TB data, 17 DWPD)
// with average process/session lifetime of (pessimistically) 4 minutes.
// In 180 days (generous allowable data lifespan), we generate 31 million GB
// of data, or 2^55 bytes, and 2^16 "all new" session IDs.
//
// First, suppose this is in a single DB (lifetime 180 days):
// 128 bits cache key size
// - 55 <- ideal size for byte offsets + file numbers
// -  2 <- bits for offsets and file numbers not exactly powers of two
// +  2 <- bits saved not using byte offsets in BlockBasedTable::GetCacheKey
// ----
//   73 <- bits remaining for distinguishing session IDs
// The probability of a collision in 73 bits of session ID data is less than
// 1 in 2**(73 - (2 * 16)), or roughly 1 in a trillion. And this assumes all
// data from the last 180 days is in cache for potential collision, and that
// cache keys under each session id exhaustively cover the remaining 57 bits
// while in reality they'll only cover a small fraction of it.
//
// Although data could be transferred between hosts, each host has its own
// cache and we are already assuming a high rate of "all new" session ids.
// So this doesn't really change the collision calculation. Across a fleet
// of 1 million, each with <1 in a trillion collision possibility,
// fleetwide collision probability is <1 in a million.
//
// Now suppose we have many DBs per host, say 2**10, with same host-wide write
// rate and process/session lifetime. File numbers will be ~10 bits smaller
// and we will have 2**10 times as many session IDs because of simultaneous
// lifetimes. So now collision chance is less than 1 in 2**(83 - (2 * 26)),
// or roughly 1 in a billion.
//
// Suppose instead we generated random or hashed cache keys for each
// (compressed) block. For 1KB compressed block size, that is 2^45 cache keys
// in 180 days. Collision probability is more easily estimated at roughly
// 1 in 2**(128 - (2 * 45)) or roughly 1 in a trillion (assuming all
// data from the last 180 days is in cache, but NOT the other assumption
// for the 1 in a trillion estimate above).
//
//
// Collision probability estimation through simulation:
// A tool ./cache_bench -stress_cache_key broadly simulates host-wide cache
// activity over many months, by making some pessimistic simplifying
// assumptions. See class StressCacheKey in cache_bench_tool.cc for details.
// Here is some sample output with
// `./cache_bench -stress_cache_key -sck_keep_bits=43`:
//
//   Total cache or DBs size: 32TiB  Writing 925.926 MiB/s or 76.2939TiB/day
//   Multiply by 1.15292e+18 to correct for simulation losses (but still
//   assume whole file cached)
//
// These come from default settings of 2.5M files per day of 32 MB each, and
// `-sck_keep_bits=43` means that to represent a single file, we are only
// keeping 43 bits of the 128-bit (base) cache key.  With file size of 2**25
// contiguous keys (pessimistic), our simulation is about 2\*\*(128-43-25) or
// about 1 billion billion times more prone to collision than reality.
//
// More default assumptions, relatively pessimistic:
// * 100 DBs in same process (doesn't matter much)
// * Re-open DB in same process (new session ID related to old session ID) on
// average every 100 files generated
// * Restart process (all new session IDs unrelated to old) 24 times per day
//
// After enough data, we get a result at the end (-sck_keep_bits=43):
//
//   (keep 43 bits)  18 collisions after 2 x 90 days, est 10 days between
//                   (1.15292e+19 corrected)
//
// If we believe the (pessimistic) simulation and the mathematical
// extrapolation, we would need to run a billion machines all for 11 billion
// days to expect a cache key collision. To help verify that our extrapolation
// ("corrected") is robust, we can make our simulation more precise by
// increasing the "keep" bits, which takes more running time to get enough
// collision data:
//
//   (keep 44 bits)  16 collisions after 5 x 90 days, est 28.125 days between
//                   (1.6213e+19 corrected)
//   (keep 45 bits)  15 collisions after 7 x 90 days, est 42 days between
//                   (1.21057e+19 corrected)
//   (keep 46 bits)  15 collisions after 17 x 90 days, est 102 days between
//                   (1.46997e+19 corrected)
//   (keep 47 bits)  15 collisions after 49 x 90 days, est 294 days between
//                   (2.11849e+19 corrected)
//
// The extrapolated prediction seems to be within noise (sampling error).
//
// With the `-sck_randomize` option, we can see that typical workloads like
// above have lower collision probability than "random" cache keys (note:
// offsets still non-randomized) by a modest amount (roughly 2-3x less
// collision prone than random), which should make us reasonably comfortable
// even in "degenerate" cases (e.g. repeatedly launch a process to generate
// one file with SstFileWriter):
//
//   (rand 43 bits) 22 collisions after 1 x 90 days, est 4.09091 days between
//                  (4.7165e+18 corrected)
//
// We can see that with more frequent process restarts,
// -sck_restarts_per_day=5000, which means more all-new session IDs, we get
// closer to the "random" cache key performance:
//
// 15 collisions after 1 x 90 days, est 6 days between (6.91753e+18 corrected)
//
// And with less frequent process restarts and re-opens,
// -sck_restarts_per_day=1 -sck_reopen_nfiles=1000, we get lower collision
// probability:
//
// 18 collisions after 8 x 90 days, est 40 days between (4.61169e+19 corrected)
//
// Other tests have been run to validate other conditions behave as expected,
// never behaving "worse than random" unless we start chopping off structured
// data.
//
// Conclusion: Even in extreme cases, rapidly burning through "all new" IDs
// that only arise when a new process is started, the chance of any cache key
// collisions in a giant fleet of machines is negligible. Especially when
// processes live for hours or days, the chance of a cache key collision is
// likely more plausibly due to bad hardware than to bad luck in random
// session ID data. Software defects are surely more likely to cause corruption
// than both of those.
//
// TODO: Nevertheless / regardless, an efficient way to detect (and thus
// quantify) block cache corruptions, including collisions, should be added.
OffsetableCacheKey::OffsetableCacheKey(const std::string &db_id,
                                       const std::string &db_session_id,
                                       uint64_t file_number) {
  UniqueId64x2 internal_id;
  Status s = GetSstInternalUniqueId(db_id, db_session_id, file_number,
                                    &internal_id, /*force=*/true);
  assert(s.ok());
  *this = FromInternalUniqueId(&internal_id);
}

OffsetableCacheKey OffsetableCacheKey::FromInternalUniqueId(UniqueIdPtr id) {
  uint64_t session_lower = id.ptr[0];
  uint64_t file_num_etc = id.ptr[1];

#ifndef NDEBUG
  bool is_empty = session_lower == 0 && file_num_etc == 0;
#endif

  // Although DBImpl guarantees (in recent versions) that session_lower is not
  // zero, that's not entirely sufficient to guarantee that file_num_etc64_ is
  // not zero (so that the 0 case can be used by CacheKey::CreateUnique*)
  // However, if we are given an "empty" id as input, then we should produce
  // "empty" as output.
  // As a consequence, this function is only bijective assuming
  // id[0] == 0 only if id[1] == 0.
  if (session_lower == 0U) {
    session_lower = file_num_etc;
  }

  // See comments above for how DownwardInvolution and ReverseBits
  // make this function invertible under various assumptions.
  OffsetableCacheKey rv;
  rv.file_num_etc64_ =
      DownwardInvolution(session_lower) ^ ReverseBits(file_num_etc);
  rv.offset_etc64_ = ReverseBits(session_lower);

  // Because of these transformations and needing to allow arbitrary
  // offset (thus, second 64 bits of cache key might be 0), we need to
  // make some correction to ensure the first 64 bits is not 0.
  // Fortunately, the transformation ensures the second 64 bits is not 0
  // for non-empty base key, so we can swap in the case one is 0 without
  // breaking bijectivity (assuming condition above).
  assert(is_empty || rv.offset_etc64_ > 0);
  if (rv.file_num_etc64_ == 0) {
    std::swap(rv.file_num_etc64_, rv.offset_etc64_);
  }
  assert(is_empty || rv.file_num_etc64_ > 0);
  return rv;
}

// Inverse of FromInternalUniqueId (assuming file_num_etc64 == 0 only if
// offset_etc64 == 0)
UniqueId64x2 OffsetableCacheKey::ToInternalUniqueId() {
  uint64_t a = file_num_etc64_;
  uint64_t b = offset_etc64_;
  if (b == 0) {
    std::swap(a, b);
  }
  UniqueId64x2 rv;
  rv[0] = ReverseBits(b);
  rv[1] = ReverseBits(a ^ DownwardInvolution(rv[0]));
  return rv;
}

}  // namespace ROCKSDB_NAMESPACE