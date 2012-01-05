## Copyright (c) 2012, Cornell University
## All rights reserved.
##
## Redistribution and use in source and binary forms, with or without
## modification, are permitted provided that the following conditions are met:
##
##     * Redistributions of source code must retain the above copyright notice,
##       this list of conditions and the following disclaimer.
##     * Redistributions in binary form must reproduce the above copyright
##       notice, this list of conditions and the following disclaimer in the
##       documentation and/or other materials provided with the distribution.
##     * Neither the name of HyperDex nor the names of its contributors may be
##       used to endorse or promote products derived from this software without
##       specific prior written permission.
##
## THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
## AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
## IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
## ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
## LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
## CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
## SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
## INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
## CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
## ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
## POSSIBILITY OF SUCH DAMAGE.

ACLOCAL_AMFLAGS = -I m4 ${ACLOCAL_FLAGS}
AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)

EXTRA_DIST = \
			LICENSE \
			$(libhyperclient_extra_dist)

BUILT_SOURCES = \
			$(python_bindings_built_sources)

lib_LTLIBRARIES = \
			libhyperspacehashing.la \
			libhyperdisk.la \
			libhyperclient.la

pyexec_LTLIBRARIES = \
			hyperclient/python/hyperclient.la

noinst_LTLIBRARIES = \
			libhyperdex.la \
			libhyperdaemon.la

noinst_HEADERS = \
			$(libhyperspacehashing_noinst_headers) \
			$(libhyperdisk_noinst_headers) \
			$(libhyperdex_noinst_headers) \
			$(libhyperdaemon_noinst_headers)

bin_PROGRAMS = \
			daemon

noinst_PROGRAMS = \
			$(libhyperspacehashing_noinst_programs) \
			$(libhyperdisk_noinst_programs) \
			count-test

check_PROGRAMS = \
			$(libhyperspacehashing_check_programs) \
			$(libhyperdisk_check_programs)

TESTS = \
			$(libhyperspacehashing_tests) \
			$(libhyperdisk_tests)

nobase_python_PYTHON = \
			hdcoordinator/__init__.py \
			hdcoordinator/coordinator.py \
			hdcoordinator/parser.py

.PHONY: coverage

################################################################################
############################## Hyperspace Hashing ##############################
################################################################################

#################################### Library ###################################

libhyperspacehashing_includedir = $(includedir)/hyperspacehashing
libhyperspacehashing_include_HEADERS = \
			hyperspacehashing/hyperspacehashing/hashes.h \
			hyperspacehashing/hyperspacehashing/mask.h \
			hyperspacehashing/hyperspacehashing/prefix.h \
			hyperspacehashing/hyperspacehashing/search.h

libhyperspacehashing_noinst_headers = \
			hyperspacehashing/bithacks.h \
			hyperspacehashing/cfloat.h \
			hyperspacehashing/hashes_internal.h \
			hyperspacehashing/range_match.h

libhyperspacehashing_la_SOURCES = \
			hyperspacehashing/cfloat.cc \
			hyperspacehashing/hashes.cc \
			hyperspacehashing/mask.cc \
			hyperspacehashing/prefix.cc \
			hyperspacehashing/range_match.cc \
			hyperspacehashing/search.cc
libhyperspacehashing_la_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(CPPFLAGS)

##################################### Tests ####################################

if HAVE_GTEST
libhyperspacehashing_check_programs = \
			hyperspacehashing/test/bithacks \
			hyperspacehashing/test/cfloat \
			hyperspacehashing/test/mask \
			hyperspacehashing/test/prefix \
			hyperspacehashing/test/search
libhyperspacehashing_tests = $(libhyperspacehashing_check_programs)

hyperspacehashing_test_bithacks_SOURCES = \
			runner.cc \
			hyperspacehashing/test/bithacks.cc
hyperspacehashing_test_bithacks_LDADD = \
			libhyperspacehashing.la \
			$(HYPERSPACEHASHING_LDADD) \
			$(COVERAGE_LDADD) \
			$(GTEST_LIBS)
hyperspacehashing_test_bithacks_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(CPPFLAGS)

hyperspacehashing_test_cfloat_SOURCES = \
			hyperspacehashing/test/cfloat.cc
hyperspacehashing_test_cfloat_LDADD = \
			libhyperspacehashing.la \
			$(HYPERSPACEHASHING_LDADD) \
			$(COVERAGE_LDADD)
hyperspacehashing_test_cfloat_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(CPPFLAGS)

hyperspacehashing_test_mask_SOURCES = \
			runner.cc \
			hyperspacehashing/test/mask.cc
hyperspacehashing_test_mask_LDADD = \
			libhyperspacehashing.la \
			$(HYPERSPACEHASHING_LDADD) \
			$(COVERAGE_LDADD) \
			$(GTEST_LIBS)
hyperspacehashing_test_mask_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(CPPFLAGS)

hyperspacehashing_test_prefix_SOURCES = \
			runner.cc \
			hyperspacehashing/test/prefix.cc
hyperspacehashing_test_prefix_LDADD = \
			libhyperspacehashing.la \
			$(HYPERSPACEHASHING_LDADD) \
			$(COVERAGE_LDADD) \
			$(GTEST_LIBS)
hyperspacehashing_test_prefix_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(CPPFLAGS)

hyperspacehashing_test_search_SOURCES = \
			hyperspacehashing/test/search.cc
hyperspacehashing_test_search_LDADD = \
			libhyperspacehashing.la \
			$(HYPERSPACEHASHING_LDADD) \
			$(COVERAGE_LDADD)
hyperspacehashing_test_search_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(CPPFLAGS)
endif

##################################### Utils ####################################

libhyperspacehashing_noinst_programs = \
			hyperspacehashing/utils/cfloat

hyperspacehashing_utils_cfloat_SOURCES = \
			hyperspacehashing/utils/cfloat.cc
hyperspacehashing_utils_cfloat_LDADD = \
			libhyperspacehashing.la \
			$(HYPERSPACEHASHING_LDADD) \
			$(COVERAGE_LDADD)

################################################################################
################################### HyperDisk ##################################
################################################################################

#################################### Library ###################################

libhyperdisk_includedir = $(includedir)/hyperdisk
libhyperdisk_include_HEADERS = \
			hyperdisk/hyperdisk/disk.h \
			hyperdisk/hyperdisk/returncode.h \
			hyperdisk/hyperdisk/snapshot.h

libhyperdisk_noinst_headers = \
			hyperdisk/log_entry.h \
			hyperdisk/offset_update.h \
			hyperdisk/shard.h \
			hyperdisk/shard_constants.h \
			hyperdisk/shard_snapshot.h \
			hyperdisk/shard_vector.h

libhyperdisk_la_SOURCES = \
			hyperdisk/disk.cc \
			hyperdisk/reference.cc \
			hyperdisk/shard.cc \
			hyperdisk/shard_snapshot.cc \
			hyperdisk/shard_vector.cc \
			hyperdisk/snapshot.cc
libhyperdisk_la_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			-I$(abs_top_srcdir)/hyperdisk \
			$(CPPFLAGS)

##################################### Tests ####################################

if HAVE_GTEST
libhyperdisk_check_programs = \
			hyperdisk/test/shard
libhyperdisk_tests = $(libhyperdisk_check_programs)

hyperdisk_test_shard_SOURCES = \
			runner.cc \
			hyperdisk/test/shard.cc
hyperdisk_test_shard_LDADD = \
			$(COVERAGE_LDADD) \
			$(GTEST_LIBS)
hyperdisk_test_shard_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(CPPFLAGS)
endif

##################################### Utils ####################################

libhyperdisk_noinst_programs = \
			hyperdisk/utils/shard-dumphashes \
			hyperdisk/utils/shard-fsck

hyperdisk_utils_shard_dumphashes_SOURCES = \
			hyperdisk/utils/shard-dumphashes.cc
hyperdisk_utils_shard_dumphashes_LDADD = \
			libhyperspacehashing.la \
			libhyperdisk.la \
			$(HYPERDISK_LDADD) \
			$(COVERAGE_LDADD)
hyperdisk_utils_shard_dumphashes_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(CPPFLAGS)

hyperdisk_utils_shard_fsck_SOURCES = \
			hyperdisk/utils/shard-fsck.cc
hyperdisk_utils_shard_fsck_LDADD = \
			libhyperspacehashing.la \
			libhyperdisk.la \
			$(HYPERDISK_LDADD) \
			$(COVERAGE_LDADD)
hyperdisk_utils_shard_fsck_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(CPPFLAGS)

################################################################################
################################### HyperDex ###################################
################################################################################

#################################### Library ###################################

libhyperdex_noinst_headers = \
			hyperdex/hyperdex/configuration.h \
			hyperdex/hyperdex/coordinatorlink.h \
			hyperdex/hyperdex/ids.h \
			hyperdex/hyperdex/instance.h \
			hyperdex/hyperdex/network_constants.h

libhyperdex_la_SOURCES = \
			hyperdex/configuration.cc \
			hyperdex/coordinatorlink.cc
libhyperdex_la_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			-I$(abs_top_srcdir)/hyperdex \
			$(CPPFLAGS)

################################################################################
################################## HyperDaemon #################################
################################################################################

#################################### Library ###################################

libhyperdaemon_noinst_headers = \
			hyperdaemon/hyperdaemon/daemon.h \
			hyperdaemon/datalayer.h \
			hyperdaemon/logical.h \
			hyperdaemon/network_worker.h \
			hyperdaemon/physical.h \
			hyperdaemon/replication/clientop.h \
			hyperdaemon/replication/deferred.h \
			hyperdaemon/replication/keyholder.h \
			hyperdaemon/replication/keypair.h \
			hyperdaemon/replication_manager.cc \
			hyperdaemon/replication_manager.h \
			hyperdaemon/replication/pending.h \
			hyperdaemon/replication/transfer_in.h \
			hyperdaemon/replication/transfer_out.h \
			hyperdaemon/runtimeconfig.h \
			hyperdaemon/searches.h

libhyperdaemon_la_SOURCES = \
			hyperdaemon/daemon.cc \
			hyperdaemon/datalayer.cc \
			hyperdaemon/logical.cc \
			hyperdaemon/network_worker.cc \
			hyperdaemon/ongoing_state_transfers.cc \
			hyperdaemon/physical.cc \
			hyperdaemon/replication_manager.cc \
			hyperdaemon/runtimeconfig.cc \
			hyperdaemon/searches.cc
libhyperdaemon_la_LIBADD = \
			libhyperdex.la
libhyperdaemon_la_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			-I$(abs_top_srcdir)/hyperdisk \
			-I$(abs_top_srcdir)/hyperdex \
			$(CPPFLAGS)

################################################################################
################################## HyperClient #################################
################################################################################

#################################### Library ###################################

libhyperclient_extra_dist = \
			hyperclient/hyperclient.i

libhyperclient_includedir = $(includedir)
libhyperclient_include_HEADERS = \
			hyperclient/hyperclient.h

libhyperclient_la_SOURCES = \
			hyperclient/hyperclient.cc
libhyperclient_la_LIBADD = \
			libhyperdex.la
libhyperclient_la_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			-I$(abs_top_srcdir)/hyperdex \
			$(CPPFLAGS)

################################ Python Bindings ###############################

python_bindings_built_sources = \
			hyperclient/python/hyperclient.c

hyperclient/python/hyperclient.c: hyperclient/python/hyperclient.pyx
	cython hyperclient/python/hyperclient.pyx

hyperclient_python_hyperclient_la_SOURCES = \
			hyperclient/python/hyperclient.c
hyperclient_python_hyperclient_la_CPPFLAGS = \
			$(PYTHON_CPPFLAGS)
hyperclient_python_hyperclient_la_LIBADD = \
			libhyperspacehashing.la \
			libhyperclient.la \
			$(HYPERCLIENT_LDADD) \
			$(PYTHON_LDFLAGS) \
			$(COVERAGE_LDADD)
hyperclient_python_hyperclient_la_LDFLAGS = \
			-module \
			-avoid-version \
			-export-symbols-regex inithyperclient

################################################################################
################################### Binaries ###################################
################################################################################

daemon_SOURCES = \
			daemon.cc
daemon_LDADD = \
			libhyperspacehashing.la \
			libhyperdisk.la \
			libhyperdaemon.la \
			$(HYPERDISK_LDADD) \
			$(COVERAGE_LDADD) \
			-lglog -lrt

#binary_test_SOURCES = \
#			binary-test.cc
#binary_test_LDADD = \
#			libhyperclient.la

count_test_SOURCES = \
			count-test.cc
count_test_LDADD = \
			libhyperspacehashing.la \
			libhyperclient.la \
			$(HYPERCLIENT_LDADD) \
			$(COVERAGE_LDADD)

#repl_test_SOURCES = \
#			repl-test.cc
#repl_test_LDADD = \
#			libhyperclient.la

################################################################################
################################### Coverage ###################################
################################################################################

if ENABLE_COVERAGE
coverage: coverage.dat
	mkdir -p coverage
	genhtml --function-coverage --branch-coverage --demangle-cpp --output coverage $(abs_top_builddir)/coverage.dat

coverage.dat: check
	lcov --directory $(abs_top_builddir) --compat-libtool --capture --output $(abs_top_builddir)/coverage.dat
	lcov --extract $(abs_top_builddir)/coverage.dat $(abs_top_builddir)/'*' --output $(abs_top_builddir)/coverage.dat

clean-coverage:
	rm -f coverage.dat
	rm -rf coverage/
	find $(abs_top_builddir) -type f -iname '*.gcda' -delete
	find $(abs_top_builddir) -type f -iname '*.gcno' -delete

clean-local: clean-coverage
else
coverage:
	@echo Coverage not configured.
endif
