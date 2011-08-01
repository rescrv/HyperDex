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
include_directories(include)
include_directories(src)
include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})

# Enable shared libraries
set(BUILD_SHARED_LIBS ON)

# Enable testing
ENABLE_TESTING()

add_subdirectory(cityhash)

###############################################################################
###############################  HyperDisk  ###################################
###############################################################################

# The installed headers for HyperDisk
set (hyperdisk_headers
)

# The source files for HyperDisk
set (hyperdisk_srcs
    src/hyperdisk/disk.cc
    src/hyperdisk/log.cc
    src/hyperdisk/shard.cc
)

add_library(hyperdisk ${hyperdisk_srcs})
set_target_properties(hyperdisk
    PROPERTIES
    VERSION         0.1.0
    SOVERSION       0
    DEFINE_SYMBOL   MAKE_HYPERDEX_LIB
)
target_link_libraries(hyperdisk
    LINK_INTERFACE_LIBRARIES
    cityhash
)

set(hyperdisk_test_srcs
    test/hyperdisk/log.cc
    test/hyperdisk/shard.cc
)

add_executable(hyperdisk-tests test/runner.cc ${hyperdisk_test_srcs})
# TODO properly detect gtest.
target_link_libraries(hyperdisk-tests
    hyperdisk
    cityhash
    gtest
    pthread
)

###############################################################################
##############################  Configuration  ################################
###############################################################################

set(configuration_srcs
    src/configuration/configuration.cc
    src/configuration/coordinatorlink.cc
)

set(configuration_test_srcs
    test/configuration/configuration.cc
)

add_executable(configuration-tests test/runner.cc ${configuration_test_srcs})
# TODO properly detect gtest.
target_link_libraries(configuration-tests
    gtest
    pthread
    configuration
)

add_library(configuration STATIC ${configuration_srcs})
add_dependencies(configuration hyperdisk-tests)

###############################################################################
##################################  Old  ######################################
###############################################################################

# Setup the headers
set(hyperdex_headers
    include/hyperdex/instance.h
    include/hyperdex/search.h
)

# Setup the sources
set(hyperdex_srcs
    src/hyperdex/client.cc
    src/hyperdex/daemon.cc
    src/hyperdex/datalayer.cc
    src/hyperdex/logical.cc
    src/hyperdex/network_worker.cc
    src/hyperdex/physical.cc
    src/hyperdex/replication_manager.cc
    src/hyperdex/search.cc
    src/hyperdex/searches.cc
)

# Build libhyperdex
add_library(hyperdex ${hyperdex_srcs})
add_dependencies(hyperdex configuration)
set_target_properties(hyperdex
    PROPERTIES
    VERSION         0.1.0
    SOVERSION       0
    DEFINE_SYMBOL   MAKE_HYPERDEX_LIB
)
target_link_libraries(hyperdex
    LINK_INTERFACE_LIBRARIES
    cityhash
    ${GLOG_LIBRARIES}
    pthread
    configuration
)

# Build hyperdexd
add_executable(hyperdexd src/bin/hyperdexd.cc)
target_link_libraries(hyperdexd
    hyperdex
    cityhash
    hyperdisk
    configuration
)

# Build count benchmark
add_executable(count src/bin/count.cc)
target_link_libraries(count
    hyperdex
    cityhash
    rt
    hyperdisk
    configuration
)

# Build cli
add_executable(hyperdex-cli src/bin/hyperdex-cli.cc)
target_link_libraries(hyperdex-cli
    hyperdex
    cityhash
    rt
    hyperdisk
    configuration
)

# Build binary test
add_executable(binary src/bin/binary.cc)
target_link_libraries(binary
    hyperdex
    cityhash
    rt
    hyperdisk
    configuration
)

# Build replication test
add_executable(repl-test src/bin/repl-test.cc)
target_link_libraries(repl-test
    hyperdex
    cityhash
    rt
    hyperdisk
    configuration
)

# Tests

set(hyperdex_test_srcs
    test/hyperdex/hyperspace.cc
    test/hyperdex/ids.cc
    test/hyperdex/search.cc
)

add_executable(testrunner test/runner.cc ${hyperdex_test_srcs})
# TODO properly detect gtest.
target_link_libraries(testrunner
    hyperdex
    cityhash
    gtest
    hyperdisk
)

add_test(SearchTest.CtorAndDtor testrunner
    --gtest_filter=SearchTest.CtorAndDtor)
add_test(SearchTest.SetAndUnset testrunner
    --gtest_filter=SearchTest.SetAndUnset)
add_test(SearchTest.Clear testrunner
    --gtest_filter=SearchTest.Clear)
add_test(SearchTest.NegativeMatch testrunner
    --gtest_filter=SearchTest.NegativeMatch)
add_test(SearchTest.SetDeathTest testrunner
    --gtest_filter=SearchTest.SetDeathTest)
add_test(SearchTest.UnsetDeathTest testrunner
    --gtest_filter=SearchTest.UnsetDeathTest)
add_test(SearchTest.MatchDeathTest testrunner
    --gtest_filter=SearchTest.MatchDeathTest)

add_test(LockingqTest.CtorAndDtor testrunner
    --gtest_filter=LockingqTest.CtorAndDtor)
add_test(LockingqTest.NormalQueue testrunner
    --gtest_filter=LockingqTest.NormalQueue)

# Installation targets.
