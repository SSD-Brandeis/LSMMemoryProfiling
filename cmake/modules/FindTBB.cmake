# - Find TBB
#
# Looks for the TBB headers/library directly via find_path/find_library
# instead of relying on a TBBConfig.cmake / tbb-config.cmake package
# config file. Older distro packages (e.g. classic TBB on Ubuntu 20.04)
# and some non-standard install prefixes (Homebrew on Apple Silicon,
# conda environments) don't ship a usable CMake config, which makes
# `find_package(TBB REQUIRED)` fail even though the library is actually
# installed. This module works either way.
#
# Being on CMAKE_MODULE_PATH ahead of find_package(TBB) makes CMake
# prefer this Module-mode lookup over Config mode, so it applies
# uniformly rather than only as a fallback after a failed Config search.
#
# Result:
#   TBB_INCLUDE_DIRS - where to find tbb.h, etc.
#   TBB_LIBRARIES    - the TBB library.
#   TBB_FOUND        - True if TBB was found.
#   TBB::tbb         - imported target, matching oneTBB's own Config-mode name.

if(NOT DEFINED TBB_ROOT_DIR)
  set(TBB_ROOT_DIR "$ENV{TBBROOT}")
endif()

find_path(TBB_INCLUDE_DIRS
  NAMES tbb/tbb.h
  HINTS ${TBB_ROOT_DIR}/include ${TBB_ROOT})

find_library(TBB_LIBRARIES
  NAMES tbb
  HINTS ${TBB_ROOT_DIR}/lib ${TBB_ROOT}/lib ENV LIBRARY_PATH)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(TBB DEFAULT_MSG TBB_LIBRARIES TBB_INCLUDE_DIRS)

mark_as_advanced(
  TBB_LIBRARIES
  TBB_INCLUDE_DIRS)

if(TBB_FOUND AND NOT TARGET TBB::tbb)
  add_library(TBB::tbb UNKNOWN IMPORTED)
  set_target_properties(TBB::tbb
    PROPERTIES
      IMPORTED_LOCATION ${TBB_LIBRARIES}
      INTERFACE_INCLUDE_DIRECTORIES ${TBB_INCLUDE_DIRS})
endif()
