cmake_minimum_required(VERSION 2.6.0)
project(hyperdex)

enable_testing()
find_package(GTest REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

include(FindPkgConfig)
include(FindPackageHandleStandardArgs)
include(cmake/FindLibBSD.cmake)
include(cmake/FindLibCityHash.cmake)
include(cmake/FindLibE.cmake)
include(cmake/FindLibGLog.cmake)

# build dynamic libraries by default
if(NOT DEFINED BUILD_SHARED_LIBS)
  set(BUILD_SHARED_LIBS YES)
endif()

set(LIBRARY_INSTALL_DIR lib)
set(INCLUDE_INSTALL_DIR include)

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
add_subdirectory(include)
add_subdirectory(src)
