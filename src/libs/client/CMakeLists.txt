set(src
  hyperclient.cc)

add_library(hyperclient ${src})
target_link_libraries(hyperclient rt ${LIBE_LIBRARY})
install(TARGETS hyperclient DESTINATION ${LIBRARY_INSTALL_DIR})
