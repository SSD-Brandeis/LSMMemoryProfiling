cmake_minimum_required(VERSION 3.10)
project(LSMBufferAnalysis)

include(FetchContent)

FetchContent_Declare(
  cxxopts
  GIT_REPOSITORY https://github.com/jarro2783/cxxopts.git
  GIT_TAG        v3.1.1
  SOURCE_DIR     ${CMAKE_CURRENT_SOURCE_DIR}/lib/cxxopts
)

FetchContent_MakeAvailable(cxxopts)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")

find_package(Threads REQUIRED)

option(WITH_TESTS "Build with tests" OFF)
option(WITH_TOOLS "Build with tools" OFF)
option(WITH_ALL_TESTS "Build with all tests" OFF)
option(WITH_BENCHMARK_TOOLS "Build with benchmark tools" OFF)
option(WITH_CORE_TOOLS "Build with core tools" OFF)
option(WITH_TRACE_TOOLS "Build with trace tools" OFF)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

set(USE_RTTI 1 CACHE BOOL "" FORCE)

add_subdirectory(lib/rocksdb)
add_subdirectory(lib/KV-WorkloadGenerator)

# ---- Tectonic build (Rust project) ----
add_custom_target(tectonic-cli ALL
    COMMAND cargo build --release
    COMMAND ${CMAKE_COMMAND} -E copy
            ${CMAKE_CURRENT_SOURCE_DIR}/lib/Tectonic/target/release/tectonic-cli
            ${CMAKE_CURRENT_SOURCE_DIR}/bin/tectonic-cli
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lib/Tectonic
    COMMENT "Building tectonic in release mode"
)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/bin)

add_executable(working_version    
    src/aux_time.cc 
    src/buffer.cc
    src/db_env.cc
    src/event_listners.cc
    src/fluid_lsm.cc
    src/utils.cc
    src/run_workload.cc
    src/working_version.cc )
target_include_directories(working_version PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/lib/rocksdb/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(working_version PRIVATE
    rocksdb
    Threads::Threads
)