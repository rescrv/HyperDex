# Copyright (c) 2012, Cornell University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of HyperDex nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

cmake_minimum_required(VERSION 2.6.0)
project(hyperdex)

set(HYPERDEX_VERSION_MAJOR 0)
set(HYPERDEX_VERSION_MINOR 2b6)
set(HYPERDEX_VERSION ${HYPERDEX_VERSION_MAJOR}.${HYPERDEX_VERSION_MINOR})

enable_testing()
find_package(GTest 1.6.0 REQUIRED)
include_directories(${GTEST_INCLUDE_DIRS})

set(Python_ADDITIONAL_VERSIONS 3.2mu 3.2 3.1mu 3.1 3.0mu 3.0)
find_package(PythonInterp 2.6.0 REQUIRED)
include(FindPkgConfig)
include(FindPackageHandleStandardArgs)

execute_process(COMMAND ${PYTHON_EXECUTABLE} -c
  "from distutils import sysconfig\nprint(sysconfig.get_python_lib())"
  OUTPUT_VARIABLE PYTHON_SITE_INSTALL_DIR
  OUTPUT_STRIP_TRAILING_WHITESPACE)

set(CMAKE_MODULE_PATH ${CMAKE_CURRENT_SOURCE_DIR}/cmake)
find_package(LibBSD      0.3.0  REQUIRED)
find_package(LibCityHash 1.0.0  REQUIRED)
find_package(LibE        0.2.4  REQUIRED)
find_package(LibPO6      0.2.2  REQUIRED)
find_package(LibGLog     0.3.0  REQUIRED)

include(cmake/UseCython.cmake)

option(ENABLE_JAVA_BINDINGS "Build Java bindings" OFF)
option(ENABLE_YCSB "Build Yahoo Cloud Serving Benchmark" OFF)

if(ENABLE_JAVA_BINDINGS)
  find_package(Java REQUIRED)
  find_package(JNI REQUIRED)
  include(UseJava)
  set(CMAKE_JAVA_TARGET_VERSION ${HYPERDEX_VERSION})
endif()

# build dynamic libraries by default
if(NOT DEFINED BUILD_SHARED_LIBS)
  set(BUILD_SHARED_LIBS YES)
endif()

set(LIBRARY_INSTALL_DIR lib)
set(INCLUDE_INSTALL_DIR include)
set(JAVA_LIB_INSTALL_DIR share/java/hyperdex/)

set(CMAKE_CXX_FLAGS_RELEASE "-g0 -O2 -s")
set(CMAKE_CXX_FLAGS_DEBUG "-ggdb -O0")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE}")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG}")

if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Type of the build" FORCE)
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
add_subdirectory(include)
add_subdirectory(src)
