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

add_subdirectory(libs)
add_subdirectory(ycsb)

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

set(bindir "${CMAKE_CURRENT_BINARY_DIR}")

set(hdcoordinator_src
  hdcoordinator/client.py
  hdcoordinator/coordinator.py
  hdcoordinator/__init__.py
  hdcoordinator/parser.py)

# use "python setup.py"-like command to build the module
add_custom_command(OUTPUT "${bindir}/hdcoordinator"
  COMMAND ${PYTHON_EXECUTABLE}
          hdcoordinator-setup.py
          build -b "${bindir}/py.build"
          install -O2 --prefix=. --install-lib=hdcoordinator --root="${bindir}"
  DEPENDS hdcoordinator-setup.py ${hdcoordinator_src}
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}")

add_custom_target(hdcoordinator ALL
  COMMAND
  DEPENDS "${bindir}/hdcoordinator")

configure_file(hyperdex-coordinator.in "${bindir}/hyperdex-coordinator")
configure_file(hyperdex-coordinator-ctl.in "${bindir}/hyperdex-coordinator-ctl")

install(DIRECTORY "${bindir}/hdcoordinator/"
  DESTINATION "${PYTHON_SITE_INSTALL_DIR}")
install(PROGRAMS
  "${bindir}/hyperdex-coordinator" "${bindir}/hyperdex-coordinator-ctl"
  DESTINATION bin)

add_test(NAME hdcoordinator_parser
  WORKING_DIRECTORY "${bindir}/hdcoordinator"
  COMMAND "${PYTHON_EXECUTABLE}" hdcoordinator/parser.py)
