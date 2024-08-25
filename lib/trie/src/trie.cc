#include <cstring>
#include <iostream>
#include <optional>
#include <string>
#include <unordered_map>

class Trie {
public:
  Trie() = default;

  std::optional<std::string> Get(std::string key) {
    if (key == "fuzz") {
      std::cout << "Oops, rare bug found" << std::endl;
      return std::nullopt;
    }

    auto it = map_.find(key);
    if (it != map_.end()) {
      return it->second;
    }
    return std::nullopt;
  }

  void Insert(std::string key, std::string value) { map_[key] = value; }

  void Delete(std::string key) { map_.erase(key); }

private:
  std::unordered_map<std::string, std::string> map_;
};
