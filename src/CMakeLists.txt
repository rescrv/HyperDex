add_subdirectory(libs)

set(COMMON_LIBS
  hyperclient
  hyperdex
  hyperspacehashing
  ${LIBBSD_LIBRARY})

add_executable(hyperdex-binary hyperdex-binary.cc)
target_link_libraries(hyperdex-binary ${COMMON_LIBS})

add_executable(hyperdex-count hyperdex-count.cc)
target_link_libraries(hyperdex-count ${COMMON_LIBS})

add_executable(hyperdex-trace-player hyperdex-trace-player.cc)
target_link_libraries(hyperdex-trace-player ${COMMON_LIBS})

add_executable(hyperdex-repl-tester hyperdex-repl-tester.cc)
target_link_libraries(hyperdex-repl-tester ${COMMON_LIBS})

add_executable(hyperdexd hyperdexd.cc)
target_link_libraries(hyperdexd ${COMMON_LIBS} hyperdaemon hyperdisk ${LIBGLOG_LIBRARY})

install(TARGETS
  hyperdex-binary 
  hyperdex-count
  hyperdex-trace-player
  hyperdex-repl-tester
  hyperdexd
  DESTINATION bin)
