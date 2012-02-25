set(src
  bithacks.h
  cfloat.cc
  cfloat.h
  hashes.cc
  hashes_internal.h
  mask.cc
  prefix.cc
  range_match.cc
  range_match.h
  search.cc)

add_library(hyperspacehashing ${src})
target_link_libraries(hyperspacehashing
  ${LIBE_LIBRARY} ${LIBCITYHASH_LIBRARY})
install(TARGETS hyperspacehashing DESTINATION ${LIBRARY_INSTALL_DIR})
