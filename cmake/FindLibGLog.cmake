pkg_check_modules(LIBGLOG QUIET libe>=0.2.4)
find_path(LIBGLOG_INCLUDE_DIR glog/logging.h
          HINTS ${LIBGLOG_INCLUDE_DIRS}
          PATH_SUFFIXES glog)

set(LIBGLOG_INCLUDE_DIRS ${LIBGLOG_INCLUDE_DIR})

find_library(LIBGLOG_LIBRARY NAMES glog HINTS ${LIBGLOG_LIBRARY_DIRS})
find_package_handle_standard_args(LibGLog DEFAULT_MSG LIBGLOG_LIBRARY LIBGLOG_INCLUDE_DIRS)
include_directories(${LIBGLOG_INCLUDE_DIRS})
