#include <iostream>
#include <string>
#include <cassert>
#include "rocksdb/db.h"
#include "rocksdb/options.h"
#include "rocksdb/memtablerep.h"
#include "config_options.h"

namespace rocksdb {
  MemTableRepFactory* NewTLXBTreeRepFactory();
}

int main() {
    rocksdb::DB* db;
    rocksdb::Options options;
    options.create_if_missing = true;
    options.allow_concurrent_memtable_write = false;
    options.memtable_factory.reset(rocksdb::NewTLXBTreeRepFactory());

    std::string db_path = "/tmp/test_tlx_btree_db";
    rocksdb::DestroyDB(db_path, options);

    rocksdb::Status s = rocksdb::DB::Open(options, db_path, &db);
    if (!s.ok()) {
        std::cerr << "Open failed: " << s.ToString() << std::endl;
        return 1;
    }

    // Test Point Query (Put and Get)
    std::cout << "Testing Point Query..." << std::endl;
    s = db->Put(rocksdb::WriteOptions(), "key1", "value1");
    assert(s.ok());
    s = db->Put(rocksdb::WriteOptions(), "key2", "value2");
    assert(s.ok());
    s = db->Put(rocksdb::WriteOptions(), "key3", "value3");
    assert(s.ok());

    std::string value;
    s = db->Get(rocksdb::ReadOptions(), "key2", &value);
    assert(s.ok());
    assert(value == "value2");
    std::cout << "Point Query OK! (key2 -> " << value << ")" << std::endl;

    // Test Range Query (Iterator)
    std::cout << "Testing Range Query (Forward)..." << std::endl;
    rocksdb::Iterator* it = db->NewIterator(rocksdb::ReadOptions());
    it->SeekToFirst();
    int count = 0;
    while (it->Valid()) {
        std::cout << "Valid! key: " << it->key().ToString() << ": " << it->value().ToString() << std::endl;
        count++;
        std::cout << "Calling Next()" << std::endl;
        it->Next();
        std::cout << "Next() returned" << std::endl;
    }
    assert(it->status().ok());
    assert(count == 3);
    std::cout << "Range Query (Forward) OK!" << std::endl;

    std::cout << "Testing Range Query (Reverse)..." << std::endl;
    it->SeekToLast();
    count = 0;
    while (it->Valid()) {
        std::cout << "Valid! key: " << it->key().ToString() << ": " << it->value().ToString() << std::endl;
        count++;
        std::cout << "Calling Prev()" << std::endl;
        it->Prev();
        std::cout << "Prev() returned" << std::endl;
    }
    assert(it->status().ok());
    assert(count == 3);
    std::cout << "Range Query (Reverse) OK!" << std::endl;

    delete it;
    delete db;

    std::cout << "All tests passed!" << std::endl;
    return 0;
}
