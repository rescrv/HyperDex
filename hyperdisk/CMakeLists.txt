project(hyperdisk)
include_directories(include)
include_directories(../hyperdex/include) # XXX Remove this.

# Installed headers.
set (hyperdisk_headers
    include/hyperdisk/disk.h
    include/hyperdisk/returncode.h
)

set (hyperdisk_srcs
    src/disk.cc
    src/log.cc
    src/shard.cc
    src/shard_snapshot.cc
)

add_library(hyperdisk ${hyperdisk_srcs})
set_target_properties(hyperdisk
    PROPERTIES
    VERSION         0.1.0
    SOVERSION       0
    DEFINE_SYMBOL   MAKE_HYPERDISK_LIB
)

target_link_libraries(hyperdisk
    LINK_INTERFACE_LIBRARIES
    cityhash
    pthread
)

set (hyperdisk_tests
    test/log.cc
    test/shard.cc
)

## XXX TODO properly detect gtest.
add_executable(hyperdisk-tests ../test-runner.cc ${hyperdisk_tests})
target_link_libraries(hyperdisk-tests
    hyperdisk
    cityhash
    pthread
    gtest
)
