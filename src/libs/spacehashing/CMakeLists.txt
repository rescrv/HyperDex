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

create_test(bithacks test/bithacks.cc)
create_test(cfloat test/cfloat.cc)
create_test(mask test/mask.cc)
create_test(prefix test/prefix.cc)
create_test(search test/search.cc)
