macro(create_test name)
  add_executable(test_${name} ${ARGN})
  target_link_libraries(test_${name}
    hyperdex hyperdisk hyperspacehashing ${GTEST_BOTH_LIBRARIES})
  add_test(${name} test_${name})
endmacro()

add_subdirectory(client)
add_subdirectory(dex)
add_subdirectory(disk)
add_subdirectory(spacehashing)
add_subdirectory(daemon)
