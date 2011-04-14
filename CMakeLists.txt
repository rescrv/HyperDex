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

# Setup the include paths
include_directories(include)

# Setup the headers
set(hyperdex_headers
    include/hyperdex/query.h
)

# Setup the sources
set(hyperdex_srcs
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
)

# Tests
ENABLE_TESTING()

set(hyperdex_test_srcs
    test/hyperdex/query.cc
)

add_executable(testrunner test/runner.cc ${hyperdex_test_srcs})
# TODO properly detect gtest.
target_link_libraries(testrunner
    hyperdex
    gtest
    pthread
)

add_test(QueryTest.CtorAndDtor testrunner
    --gtest_filter=QueryTest.CtorAndDtor)
add_test(QueryTest.SetAndUnset testrunner
    --gtest_filter=QueryTest.SetAndUnset)
add_test(QueryTest.NegativeMatch testrunner
    --gtest_filter=QueryTest.NegativeMatch)
add_test(QueryTest.SetDeathTest testrunner
    --gtest_filter=QueryTest.SetDeathTest)
add_test(QueryTest.UnsetDeathTest testrunner
    --gtest_filter=QueryTest.UnsetDeathTest)
add_test(QueryTest.MatchDeathTest testrunner
    --gtest_filter=QueryTest.MatchDeathTest)

# Installation targets.
