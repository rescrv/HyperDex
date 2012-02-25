pkg_check_modules(LIBCITYHASH QUIET libcityhash>=1.0.3)
find_path(LIBCITYHASH_INCLUDE_DIR city.h
  HINTS ${LIBCITYHASH_INCLUDE_DIRS})

set(LIBCITYHASH_INCLUDE_DIRS ${LIBCITYHASH_INCLUDE_DIR})

find_library(LIBCITYHASH_LIBRARY NAMES cityhash HINTS ${LIBCITYHASH_LIBRARY_DIRS})
find_package_handle_standard_args(LibCityHash DEFAULT_MSG LIBCITYHASH_LIBRARY LIBCITYHASH_INCLUDE_DIRS)
include_directories(${LIBCITYHASH_INCLUDE_DIRS})
