project(hyperdaemon)
include_directories(include)
include_directories(../hyperdisk/include)
include_directories(../hyperdex/include)

# Installed headers.
set (hyperdaemon_headers
    include/hyperdaemon/daemon.h
)

set (hyperdaemon_srcs
    src/daemon.cc
    src/datalayer.cc
    src/logical.cc
    src/network_worker.cc
    src/physical.cc
    src/replication_manager.cc
    src/searches.cc
)

add_library(hyperdaemon ${hyperdaemon_srcs})
set_target_properties(hyperdaemon
    PROPERTIES
    VERSION         0.1.0
    SOVERSION       0
    DEFINE_SYMBOL   MAKE_HYPERDAEMON_LIB
)

target_link_libraries(hyperdaemon
    LINK_INTERFACE_LIBRARIES
    cityhash
    pthread
)
