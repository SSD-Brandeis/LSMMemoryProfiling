#include "../src/trie.cc"
#include "proto/gen/db_operation.pb.h"
#include "src/libfuzzer/libfuzzer_macro.h"

DEFINE_PROTO_FUZZER(const Operations &operations) {
  if (operations.operations().empty()) {
    return;
  }

  Trie trie;
  std::unordered_map<std::string, std::string> truth;

  for (const auto &operation : operations.operations()) {
    switch (operation.op_type_case()) {
    case Operation::kGet: {
      auto get = operation.get();
      std::string key = get.key();
      std::optional<std::string> val = trie.Get(key);
      auto it = truth.find(key);
      // trie has val, map doesnt OR trie doesnt have val, map does
      if ((it == truth.end() && val.has_value()) ||
          (it != truth.end() && !val.has_value())) {
        abort();
      }
      // trie and map have different values
      if (it != truth.end() && val.has_value()) {
        if (val.value() != it->second) {
          abort();
        }
      }
      break;
    }
    case Operation::kPut: {
      auto put = operation.put();
      std::string key = put.key();
      std::string val = put.val();
      trie.Insert(key, val);
      truth[key] = val;
      break;
    }
    case Operation::kDelete: {
      auto del = operation.delete_();
      std::string key = del.key();

      trie.Delete(key);
      truth.erase(key);
      break;
    }
    case Operation::OP_TYPE_NOT_SET: {
      break;
    }
    }
  }
}
