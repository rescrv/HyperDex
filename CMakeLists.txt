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

set(CMAKE_CXX_FLAGS "${HyperDex_wanal} ${CMAKE_CXX_FLAGS}")

# Find dependencies
find_package(PkgConfig REQUIRED)
pkg_search_module(GLOG REQUIRED libglog)
pkg_search_module(EV REQUIRED libev)

# Setup the include paths
include_directories(include)
include_directories(src)
include_directories(SYSTEM ${GLOG_INCLUDE_DIRS})
include_directories(SYSTEM ${EV_INCLUDE_DIRS})

# Setup the headers
set(hyperdex_headers
    include/hyperdex/query.h
    include/hyperdex/mapper.h
)

# Setup the sources
set(hyperdex_srcs
    src/hyperdex/bloom.cc
    src/hyperdex/city.cc
    src/hyperdex/hyperdexd.cc
    src/hyperdex/log.cc
    src/hyperdex/logical.cc
    src/hyperdex/mapper.cc
    src/hyperdex/network_worker.cc
    src/hyperdex/physical.cc
    src/hyperdex/query.cc
)

# Build libhyperdex
set(BUILD_SHARED_LIBS ON)
add_library(hyperdex ${hyperdex_srcs})
set_target_properties(hyperdex
    PROPERTIES
    VERSION         0.1.0
    SOVERSION       0
    DEFINE_SYMBOL   MAKE_HYPERDEX_LIB
)
target_link_libraries(hyperdex
    LINK_INTERFACE_LIBRARIES
    ${EV_LIBRARIES}
    ${GLOG_LIBRARIES}
    pthread
)

# Build hyperdexd
add_executable(hyperdexd src/bin/hyperdexd.cc)
# TODO properly detect sctp.
target_link_libraries(hyperdexd
    hyperdex
    sctp
)

# Tests
ENABLE_TESTING()

set(hyperdex_test_srcs
    test/hyperdex/bloom.cc
    test/hyperdex/log.cc
    test/hyperdex/lockingq.cc
    test/hyperdex/mapper.cc
    test/hyperdex/query.cc
)

add_executable(testrunner test/runner.cc ${hyperdex_test_srcs})
# TODO properly detect gtest.
# TODO properly detect sctp.
target_link_libraries(testrunner
    hyperdex
    gtest
    sctp
)

add_test(QueryTest.CtorAndDtor testrunner
    --gtest_filter=QueryTest.CtorAndDtor)
add_test(QueryTest.SetAndUnset testrunner
    --gtest_filter=QueryTest.SetAndUnset)
add_test(QueryTest.Clear testrunner
    --gtest_filter=QueryTest.Clear)
add_test(QueryTest.NegativeMatch testrunner
    --gtest_filter=QueryTest.NegativeMatch)
add_test(QueryTest.SetDeathTest testrunner
    --gtest_filter=QueryTest.SetDeathTest)
add_test(QueryTest.UnsetDeathTest testrunner
    --gtest_filter=QueryTest.UnsetDeathTest)
add_test(QueryTest.MatchDeathTest testrunner
    --gtest_filter=QueryTest.MatchDeathTest)

add_test(BloomTest.CtorAndDtor testrunner
    --gtest_filter=BloomTest.CtorAndDtor)
add_test(BloomTest.AddAndCheck testrunner
    --gtest_filter=BloomTest.AddAndCheck)
add_test(BloomTest.Assignment testrunner
    --gtest_filter=BloomTest.Assignment)
add_test(BloomTest.CopyConstructor testrunner
    --gtest_filter=BloomTest.CopyConstructor)
add_test(BloomTest.Serialization testrunner
    --gtest_filter=BloomTest.Serialization)

add_test(MapperTest.CtorAndDtor testrunner
    --gtest_filter=MapperTest.CtorAndDtor)
add_test(MapperTest.ThreeColumnTableOneColumnQuery testrunner
    --gtest_filter=MapperTest.ThreeColumnTableOneColumnQuery)
add_test(MapperTest.ThreeColumnTableTwoColumnQuery testrunner
    --gtest_filter=MapperTest.ThreeColumnTableTwoColumnQuery)
add_test(MapperTest.ThreeColumnTableThreeColumnQuery testrunner
    --gtest_filter=MapperTest.ThreeColumnTableThreeColumnQuery)

add_test(LogTest.CtorAndDtor testrunner
    --gtest_filter=LogTest.CtorAndDtor)
add_test(LogTest.CtorAndDtorDeathTest testrunner
    --gtest_filter=LogTest.CtorAndDtorDeathTest)
add_test(LogTest.NonEmptyLogDeathTest testrunner
    --gtest_filter=LogTest.NonEmptyLogDeathTest)
add_test(LogTest.SimpleIteration testrunner
    --gtest_filter=LogTest.SimpleIteration)
add_test(LogTest.IterateAddIterate testrunner
    --gtest_filter=LogTest.IterateAddIterate)
add_test(LogTest.IterateDrainIterate testrunner
    --gtest_filter=LogTest.IterateDrainIterate)
add_test(LogTest.ConcurrentIteration testrunner
    --gtest_filter=LogTest.ConcurrentIteration)

add_test(LockingqTest.CtorAndDtor testrunner
    --gtest_filter=LockingqTest.CtorAndDtor)
add_test(LockingqTest.NormalQueue testrunner
    --gtest_filter=LockingqTest.NormalQueue)

# Installation targets.
