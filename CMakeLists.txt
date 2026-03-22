cmake_minimum_required(VERSION 3.10)
project(FluidLSM VERSION 1.0)

set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
set(CMAKE_CXX_STANDARD 20)

find_package(Threads REQUIRED)

option(WITH_TESTS "Build with tests" OFF)
option(WITH_TOOLS "Build with tools" OFF)
option(WITH_ALL_TESTS "Build with all tests" OFF)
option(WITH_BENCHMARK_TOOLS "Build with benchmark tools" OFF)
option(WITH_CORE_TOOLS "Build with core tools" OFF)
option(WITH_TRACE_TOOLS "Build with trace tools" OFF)

set(USE_RTTI 1 CACHE BOOL "Force RTTI on" FORCE)
add_subdirectory(lib/rocksdb)

# Build tectonic
add_custom_target(tectonic-cli ALL
    COMMAND cargo build --release
    COMMAND ${CMAKE_COMMAND} -E copy
            ${CMAKE_CURRENT_SOURCE_DIR}/lib/Tectonic/target/release/tectonic-cli
            ${CMAKE_CURRENT_SOURCE_DIR}/bin/tectonic-cli
    WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/lib/Tectonic
    COMMENT "Building tectonic in release mode"
)

set(SOURCES
    ${CMAKE_CURRENT_SOURCE_DIR}/src/buffer.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/db_env.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/event_listners.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/utils.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/run_workload.cc
    ${CMAKE_CURRENT_SOURCE_DIR}/src/working_version.cc 
)

add_executable(working_version ${SOURCES})

target_include_directories(working_version PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/lib/rocksdb/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_compile_definitions(working_version PRIVATE -DPER_OP_TIMER -DTOTAL_TIMER)

target_link_libraries(working_version PRIVATE
    rocksdb
    Threads::Threads
)

set_target_properties(working_version PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/bin
)