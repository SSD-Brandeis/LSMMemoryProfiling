#ifndef EVENT_LISTNER_H_
#define EVENT_LISTNER_H_

#include <rocksdb/db.h>

#include <condition_variable>

using namespace rocksdb;

extern std::mutex mtx;
extern std::condition_variable cv;
extern bool compaction_complete;

/*
 * Wait for compactions that are running (or will run) to make the
 * LSM tree in its shape. Check `CompactionListner` for more details.
 */
void WaitForCompactions(DB *db);

/*
 * The compactions can run in background even after the workload is completely
 * executed so, we have to wait for them to complete. Compaction Listener gets
 * notified by the rocksdb API for every compaction that just finishes off.
 * After every compaction we check, if more compactions are required with
 * `WaitForCompaction` function, if not then it signals to close the db
 */
class CompactionsListner : public EventListener {
public:
  explicit CompactionsListner() {}

  void OnCompactionCompleted(DB *db, const CompactionJobInfo &ci) override {
    std::lock_guard<std::mutex> lock(mtx);
    uint64_t num_running_compactions;
    uint64_t pending_compaction_bytes;
    uint64_t num_pending_compactions;
    db->GetIntProperty("rocksdb.num-running-compactions",
                       &num_running_compactions);
    db->GetIntProperty("rocksdb.estimate-pending-compaction-bytes",
                       &pending_compaction_bytes);
    db->GetIntProperty("rocksdb.compaction-pending", &num_pending_compactions);
    if (num_running_compactions == 0 && pending_compaction_bytes == 0 &&
        num_pending_compactions == 0) {
      compaction_complete = true;
    }
    cv.notify_one();
  }
};

#endif // EVENT_LISTNER_H_