set(src
  disk.cc
  log_entry.h
  offset_update.h
  reference.cc
  shard.cc
  shard_constants.h
  shard.h
  shard_snapshot.cc
  shard_snapshot.h
  shard_vector.cc
  shard_vector.h
  snapshot.cc)

add_library(hyperdisk ${src})
install(TARGETS hyperdisk DESTINATION ${LIBRARY_INSTALL_DIR})
