set(src
  configuration.cc
  coordinatorlink.cc)

add_library(hyperdex ${src})
install(TARGETS hyperdex DESTINATION ${LIBRARY_INSTALL_DIR})
