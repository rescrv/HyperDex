set(src
  daemon.cc
  datalayer.cc
  datalayer.h
  logical.cc
  logical.h
  network_worker.cc
  network_worker.h
  ongoing_state_transfers.cc
  ongoing_state_transfers.h
  physical.cc
  physical.h
  replication/clientop.h
  replication/keypair.h
  replication_manager.cc
  replication_manager.h
  runtimeconfig.cc
  runtimeconfig.h
  searches.cc
  searches.h)

add_library(hyperdaemon ${src})
target_link_libraries(hyperdaemon
  rt hyperdex hyperspacehashing ${LIBE_LIBRARY})
install(TARGETS hyperdaemon DESTINATION ${LIBRARY_INSTALL_DIR})
