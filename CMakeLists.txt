cmake_minimum_required(VERSION 2.6)

project(HyperDex)

# Flags are credit to Abhishek "G" Mukherjee
set(HyperDex_wanal "")
set(HyperDex_wanal "${HyperDex_wanal} -pedantic")
set(HyperDex_wanal "${HyperDex_wanal} -std=c++98")
set(HyperDex_wanal "${HyperDex_wanal} -Wabi")
set(HyperDex_wanal "${HyperDex_wanal} -Wall")
set(HyperDex_wanal "${HyperDex_wanal} -Wcast-align")
set(HyperDex_wanal "${HyperDex_wanal} -Wcast-qual")
set(HyperDex_wanal "${HyperDex_wanal} -Wctor-dtor-privacy")
set(HyperDex_wanal "${HyperDex_wanal} -Weffc++")
set(HyperDex_wanal "${HyperDex_wanal} -Wextra")
set(HyperDex_wanal "${HyperDex_wanal} -Wfloat-equal")
set(HyperDex_wanal "${HyperDex_wanal} -Wformat=2")
set(HyperDex_wanal "${HyperDex_wanal} -Winit-self")
set(HyperDex_wanal "${HyperDex_wanal} -Winline")
set(HyperDex_wanal "${HyperDex_wanal} -Woverloaded-virtual")
set(HyperDex_wanal "${HyperDex_wanal} -Wshadow")
set(HyperDex_wanal "${HyperDex_wanal} -Wsign-promo")
set(HyperDex_wanal "${HyperDex_wanal} -Wstrict-overflow=5")
set(HyperDex_wanal "${HyperDex_wanal} -Wswitch-default")
set(HyperDex_wanal "${HyperDex_wanal} -Wswitch-enum")
set(HyperDex_wanal "${HyperDex_wanal} -Wunsafe-loop-optimizations")
set(HyperDex_wanal "${HyperDex_wanal} -Wwrite-strings")
set(HyperDex_wanal "${HyperDex_wanal} -Wno-long-long")

set(CMAKE_CXX_FLAGS "${HyperDex_wanal} ${CMAKE_CXX_FLAGS}")

# Find dependencies
find_package(PkgConfig REQUIRED)
pkg_search_module(GLOG REQUIRED libglog)

# Setup the include paths
include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})

# Enable shared libraries
set(BUILD_SHARED_LIBS ON)

# Enable testing
ENABLE_TESTING()

add_subdirectory(hyperdisk)
add_subdirectory(hyperdex)
add_subdirectory(hyperdaemon)
add_subdirectory(hyperclient)
add_dependencies(hyperdaemon hyperdex)
add_dependencies(hyperclient hyperdex)

include_directories(hyperdaemon/include)
include_directories(hyperclient/include)

# Build hyperdexd
add_executable(hyperdexd bin/hyperdexd.cc)
target_link_libraries(hyperdexd
    glog
    hyperdisk
    hyperdex
    hyperdaemon
)

# Build count benchmark
add_executable(count bin/count.cc)
target_link_libraries(count
    hyperdex
    hyperclient
    rt
)

# Build binary benchmark
add_executable(binary bin/binary.cc)
target_link_libraries(binary
    hyperdex
    hyperclient
    rt
)

# Build cli
add_executable(hyperdex-cli bin/hyperdex-cli.cc)
target_link_libraries(hyperdex-cli
    hyperdex
    hyperclient
    rt
)

# Build replication test
add_executable(repl-test bin/repl-test.cc)
target_link_libraries(repl-test
    hyperdex
    hyperclient
    rt
)
