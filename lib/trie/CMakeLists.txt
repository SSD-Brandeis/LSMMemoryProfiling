cmake_minimum_required(VERSION 3.10)
project(trie VERSION 1.0)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0")

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../rocksdb/include)

set(SOURCES
        ${CMAKE_CURRENT_SOURCE_DIR}/src/trie.cc
)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/bin)

add_executable(trie ${SOURCES})

# Testing

find_package(GTest REQUIRED)

enable_testing()

add_executable(
        trie_test
        trie_test.cc
        src/trie.cc
)
target_link_libraries(
        trie_test
        GTest::GTest
        GTest::Main
)
include(GoogleTest)
gtest_discover_tests(trie_test)