#include <gtest/gtest.h>
#include "./src/trie.cc"

TEST(TrieTest, Insert) {
    Trie trie;
    trie.Insert(10, 10);
    ASSERT_NO_THROW();
}

TEST(TrieTest, Get) {
   Trie trie;
    trie.Insert(10, 10);
    std::optional<uint8_t> value = trie.Get(10);
    ASSERT_EQ(value.has_value(), true);
    ASSERT_EQ(value.value(), 10);
}

TEST(TrieTest, Update) {
   Trie trie;
    trie.Insert(10, 10);
    trie.Insert(10, 20);
    std::optional<uint8_t> value = trie.Get(10);
    ASSERT_EQ(value.has_value(), true);
    ASSERT_EQ(value.value(), 20);
}

TEST(TrieTest, Delete) {
    // TODO ins, del, pq
}

TEST(TrieTest, Delete2) {
    // TODO ins, del, ins pq
}

// use skiplist as absolute truth