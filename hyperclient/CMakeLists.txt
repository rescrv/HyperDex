project(hyperclient)
include_directories(../cityhash/include)
include_directories(../include) # XXX Remove this.
include_directories(../src) # XXX Remove this.

# Installed headers.
set (hyperclient_headers
    include/hyperclient/client.h
)
#    include/hyperclient/returncode.h

set (hyperclient_srcs
    src/client.cc
)

add_library(hyperclient ${hyperclient_srcs})
set_target_properties(hyperclient
    PROPERTIES
    VERSION         0.1.0
    SOVERSION       0
    DEFINE_SYMBOL   MAKE_HYPERCLIENT_LIB
)

target_link_libraries(hyperclient
    LINK_INTERFACE_LIBRARIES
    cityhash
    pthread
)

set (hyperclient_tests
)

## XXX TODO properly detect gtest.
add_executable(hyperclient-tests ../test-runner.cc ${hyperclient_tests})
target_link_libraries(hyperclient-tests
    hyperdisk
    cityhash
    pthread
    gtest
)
