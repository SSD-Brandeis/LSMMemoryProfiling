#include "./src/trie.cc"
#include <gtest/gtest.h>

TEST(TrieTest, Insert) {
  Trie trie;
  trie.Insert("key", "value");
  ASSERT_NO_THROW();
}

TEST(TrieTest, Get) {
  Trie trie;
  trie.Insert("key", "value");
  std::optional<std::string> value = trie.Get("key");
  ASSERT_TRUE(value.has_value());
  ASSERT_EQ(value.value(), "value");
}

TEST(TrieTest, Update) {
  Trie trie;
  trie.Insert("key", "initial");
  trie.Insert("key", "updated");
  std::optional<std::string> value = trie.Get("key");
  ASSERT_TRUE(value.has_value());
  ASSERT_EQ(value.value(), "updated");
}

TEST(TrieTest, Delete) {
  Trie trie;
  trie.Insert("key", "value");
  trie.Delete("key");
  std::optional<std::string> value = trie.Get("key");
  ASSERT_FALSE(value.has_value());
}

TEST(TrieTest, Delete2) {
  Trie trie;
  trie.Insert("key", "value");
  trie.Delete("key");
  trie.Insert("key", "new_value");
  std::optional<std::string> value = trie.Get("key");
  ASSERT_TRUE(value.has_value());
  ASSERT_EQ(value.value(), "new_value");
}

TEST(TrieTest, FuzzFail1) {
  std::string key = "";
  std::string value(
      "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"
      "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"
      "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"
      "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"
      "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000",
      92
  );
  Trie trie;
  trie.Insert(key, value);
  std::optional<std::string> retrieved_value = trie.Get(key);
  ASSERT_TRUE(retrieved_value.has_value());
  ASSERT_EQ(retrieved_value.value(), value);
}