set(src
  configuration.cc
  coordinatorlink.cc)

add_library(hyperdex ${src})
install(TARGETS hyperdex DESTINATION ${LIBRARY_INSTALL_DIR})

#create_test(configuration test/configuration.cc)
create_test(ids test/ids.cc)
#create_test(search test/search.cc)
