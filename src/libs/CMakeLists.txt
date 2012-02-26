# Copyright (c) 2012, Cornell University
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of HyperDex nor the names of its contributors may be
#       used to endorse or promote products derived from this software without
#       specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
# ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
# LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
# CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
# SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
# INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
# CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
# ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

macro(create_test name)
  add_executable(test_${name} ${ARGN})
  target_link_libraries(test_${name}
    hyperdex hyperdisk hyperspacehashing ${GTEST_BOTH_LIBRARIES})
  add_test(${name} test_${name})
endmacro()

macro(create_library name)
  add_library(${name} ${ARGN})
  target_link_libraries(${name} rt ${LIBE_LIBRARY})
  set_target_properties(${name} PROPERTIES VERSION ${HYPERDEX_VERSION})
  set_target_properties(${name} PROPERTIES SOVERSION ${HYPERDEX_VERSION_MAJOR})
  install(TARGETS ${name} DESTINATION ${LIBRARY_INSTALL_DIR})
endmacro()

add_subdirectory(client)
add_subdirectory(dex)
add_subdirectory(disk)
add_subdirectory(spacehashing)
add_subdirectory(daemon)
