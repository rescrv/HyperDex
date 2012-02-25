pkg_check_modules(LIBE QUIET libe>=0.2.4)
find_path(LIBE_INCLUDE_DIR e/slice.h
          HINTS ${LIBE_INCLUDE_DIRS}
          PATH_SUFFIXES e)

set(LIBE_INCLUDE_DIRS ${LIBE_INCLUDE_DIR})

find_library(LIBE_LIBRARY NAMES e HINTS ${LIBE_LIBRARY_DIRS})
find_package_handle_standard_args(LibE DEFAULT_MSG LIBE_LIBRARY LIBE_INCLUDE_DIRS)
include_directories(${LIBE_INCLUDE_DIRS})
