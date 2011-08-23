project(hyperdex)
include_directories(include)

# Installed headers.
set (hyperdex_headers
    include/hyperdex/configuration.h
    include/hyperdex/coordinatorlink.h
    include/hyperdex/hyperspace.h
    include/hyperdex/ids.h
    include/hyperdex/instance.h
    include/hyperdex/network_constants.h
    include/hyperdex/search.h
)

set (hyperdex_srcs
    src/configuration.cc
    src/search.cc
    src/coordinatorlink.cc
)

add_library(hyperdex ${hyperdex_srcs})
set_target_properties(hyperdex
    PROPERTIES
    VERSION         0.1.0
    SOVERSION       0
    DEFINE_SYMBOL   MAKE_HYPERDEX_LIB
)

target_link_libraries(hyperdex
    LINK_INTERFACE_LIBRARIES
    cityhash
)

#set (hyperdex_tests
#    test/log.cc
#    test/shard.cc
#)
#
### XXX TODO properly detect gtest.
#add_executable(hyperdex-tests ../test-runner.cc ${hyperdex_tests})
#target_link_libraries(hyperdex-tests
#    hyperdisk
#    cityhash
#    pthread
#    gtest
#)
