#include <cstdint>
#include <optional>

class Trie {
public:
    explicit Trie() {
    }

    std::optional<uint8_t> Get(uint8_t key) {
        return std::nullopt;
    }

    void Insert(uint8_t key, uint8_t value) {
    }
};
