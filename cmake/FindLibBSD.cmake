pkg_search_module(LIBBSD REQUIRED QUIET libbsd>=0.3.0)
find_library(LIBBSD_LIBRARY NAMES bsd HINTS ${LIBBSD_LIBRARY_DIRS})
find_package_handle_standard_args(LibBSD DEFAULT_MSG LIBBSD_LIBRARY LIBBSD_INCLUDE_DIRS)
include_directories(${LIBBSD_INCLUDE_DIRS})
