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
AM_CXXFLAGS = $(WANAL_CXXFLAGS) $(COVERAGE_CXXFLAGS)

EXTRA_DIST = \
			LICENSE \
			doc \
			doc/HyperDex-$(VERSION).pdf \
			hyperdex-coordinator \
			hyperdex-coordinator-control \
			$(libhyperclient_extra_dist) \
			$(ycsb_extra_dist) \
			hyperclient/nodejs/sync_test.js \
			hyperclient/nodejs/async_test.js \
			hyperclient/nodejs/hyperclient.cc \
			wscript

lib_LTLIBRARIES = \
			libhyperspacehashing.la \
			libhyperdisk.la \
			libhyperclient.la

pyexec_LTLIBRARIES =

include_HEADERS = \
			hyperdex.h

noinst_HEADERS = \
			$(libhyperspacehashing_noinst_headers) \
			$(libhyperdisk_noinst_headers) \
			$(libhyperdex_noinst_headers) \
			$(libhyperclient_noinst_headers) \
			$(java_bindings_noinst_headers) \
			util/atomicfile.h

bin_PROGRAMS = \
			hyperdex-binary-test \
			hyperdex-daemon \
			hyperdex-replication-stress-test \
			hyperdex-simple-consistency-stress-test

noinst_PROGRAMS = \
			$(libhyperspacehashing_noinst_programs) \
			$(libhyperdisk_noinst_programs) \
			hyperclient/c/testcompile

check_PROGRAMS = \
			$(libhyperspacehashing_check_programs) \
			$(libhyperdisk_check_programs)

bin_SCRIPTS = \
			hyperdex-coordinator \
			hyperdex-coordinator-control

TESTS = \
			$(libhyperspacehashing_tests) \
			$(libhyperdisk_tests)

nobase_python_PYTHON = \
			hypercoordinator/__init__.py \
			hypercoordinator/client.py \
			hypercoordinator/coordinator.py \
			hypercoordinator/hdtypes.py \
			hypercoordinator/hdjson.py \
			hypercoordinator/parser.py

CLEANFILES = \
			$(java_bindings_cleanfiles) \
			$(ycsb_cleanfiles)

JAVAROOT = $(abs_top_srcdir)

.PHONY: coverage

dist-hook:
	rm -rf $(distdir)/doc/_build

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
			$(E_CFLAGS) \
			$(CPPFLAGS)
libhyperspacehashing_la_LIBADD = \
			$(E_LIBS) \
			-lcityhash

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
			$(GTEST_LIBS)
hyperspacehashing_test_bithacks_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(CPPFLAGS)

hyperspacehashing_test_cfloat_SOURCES = \
			hyperspacehashing/test/cfloat.cc
hyperspacehashing_test_cfloat_LDADD = \
			libhyperspacehashing.la \
			$(COVERAGE_LDADD)
hyperspacehashing_test_cfloat_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(CPPFLAGS)

hyperspacehashing_test_mask_SOURCES = \
			runner.cc \
			hyperspacehashing/test/mask.cc
hyperspacehashing_test_mask_LDADD = \
			libhyperspacehashing.la \
			$(COVERAGE_LDADD) \
			$(GTEST_LIBS)
hyperspacehashing_test_mask_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(E_CFLAGS) \
			$(CPPFLAGS)

hyperspacehashing_test_prefix_SOURCES = \
			runner.cc \
			hyperspacehashing/test/prefix.cc
hyperspacehashing_test_prefix_LDADD = \
			libhyperspacehashing.la \
			$(COVERAGE_LDADD) \
			$(GTEST_LIBS)
hyperspacehashing_test_prefix_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(E_CFLAGS) \
			$(CPPFLAGS)

hyperspacehashing_test_search_SOURCES = \
			hyperspacehashing/test/search.cc
hyperspacehashing_test_search_LDADD = \
			libhyperspacehashing.la \
			$(COVERAGE_LDADD)
hyperspacehashing_test_search_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(E_CFLAGS) \
			$(CPPFLAGS)
endif

##################################### Utils ####################################

libhyperspacehashing_noinst_programs = \
			hyperspacehashing/utils/cfloat

hyperspacehashing_utils_cfloat_SOURCES = \
			hyperspacehashing/utils/cfloat.cc
hyperspacehashing_utils_cfloat_LDADD = \
			libhyperspacehashing.la \
			$(COVERAGE_LDADD)
hyperspacehashing_utils_cfloat_CPPFLAGS = \
			$(E_CFLAGS) \
			$(CPPFLAGS)

################################################################################
################################### HyperDisk ##################################
################################################################################

#################################### Library ###################################

libhyperdisk_includedir = $(includedir)/hyperdisk
libhyperdisk_include_HEADERS = \
			hyperdisk/hyperdisk/disk.h \
			hyperdisk/hyperdisk/reference.h \
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
libhyperdisk_la_LIBADD = \
			libhyperspacehashing.la \
			-lpthread \
			$(COVERAGE_LDADD)
libhyperdisk_la_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			-I$(abs_top_srcdir)/hyperdisk \
			$(E_CFLAGS) \
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
			libhyperspacehashing.la \
			libhyperdisk.la \
			$(COVERAGE_LDADD) \
			$(GTEST_LIBS)
hyperdisk_test_shard_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(E_CFLAGS) \
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
			$(COVERAGE_LDADD)
hyperdisk_utils_shard_dumphashes_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(E_CFLAGS) \
			$(CPPFLAGS)

hyperdisk_utils_shard_fsck_SOURCES = \
			hyperdisk/utils/shard-fsck.cc
hyperdisk_utils_shard_fsck_LDADD = \
			libhyperspacehashing.la \
			libhyperdisk.la \
			$(COVERAGE_LDADD)
hyperdisk_utils_shard_fsck_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(E_CFLAGS) \
			$(CPPFLAGS)

################################################################################
################################### HyperDex ###################################
################################################################################

#################################### Library ###################################

libhyperdex_noinst_headers = \
			hyperdex/hyperdex/attribute.h \
			hyperdex/hyperdex/configuration.h \
			hyperdex/hyperdex/configuration_parser.h \
			hyperdex/hyperdex/coordinatorlink.h \
			hyperdex/hyperdex/ids.h \
			hyperdex/hyperdex/instance.h \
			hyperdex/hyperdex/microop.h \
			hyperdex/hyperdex/network_constants.h \
			hyperdex/hyperdex/packing.h

libhyperdex_sources = \
			hyperdex/configuration.cc \
			hyperdex/configuration_parser.cc \
			hyperdex/coordinatorlink.cc \
			hyperdex/microop.cc

################################################################################
################################## HyperDaemon #################################
################################################################################

#################################### Library ###################################

hyperdex_daemon_SOURCES = \
			daemon.cc \
			hyperdaemon/daemon.h \
			hyperdaemon/daemon.cc \
			hyperdaemon/datalayer.h \
			hyperdaemon/datalayer.cc \
			hyperdaemon/datatypes.h \
			hyperdaemon/datatypes.cc \
			hyperdaemon/logical.h \
			hyperdaemon/logical.cc \
			hyperdaemon/network_worker.h \
			hyperdaemon/network_worker.cc \
			hyperdaemon/ongoing_state_transfers.h \
			hyperdaemon/ongoing_state_transfers.cc \
			hyperdaemon/replication/clientop.h \
			hyperdaemon/replication/keypair.h \
			hyperdaemon/replication_manager.h \
			hyperdaemon/replication_manager.cc \
			hyperdaemon/replication_manager_deferred.h \
			hyperdaemon/replication_manager_keyholder.h \
			hyperdaemon/replication_manager_pending.h \
			hyperdaemon/runtimeconfig.h \
			hyperdaemon/runtimeconfig.cc \
			hyperdaemon/searches.h \
			hyperdaemon/searches.cc \
			${libhyperdex_sources}
hyperdex_daemon_LDADD = \
			libhyperdisk.la \
			libhyperspacehashing.la \
			$(E_LIBS) \
			$(BUSYBEE_LIBS) \
			-lbusybee-mta \
			$(COVERAGE_LDADD) \
			-lcityhash -lpopt -lglog -lrt -lpthread
hyperdex_daemon_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			-I$(abs_top_srcdir)/hyperdisk \
			$(E_CFLAGS) \
			$(CPPFLAGS)

################################################################################
################################## HyperClient #################################
################################################################################

#################################### Library ###################################

libhyperclient_extra_dist = \
			hyperclient/java/hyperclient.i \
			hyperclient/python/hyperclient.pyx

libhyperclient_includedir = $(includedir)
libhyperclient_include_HEADERS = \
			hyperclient/hyperclient.h

libhyperclient_noinst_headers = \
			hyperclient/constants.h \
			hyperclient/hyperclient_completedop.h \
			hyperclient/hyperclient_pending.h \
			hyperclient/hyperclient_pending_get.h \
			hyperclient/hyperclient_pending_search.h \
			hyperclient/hyperclient_pending_statusonly.h \
			hyperclient/util.h

libhyperclient_la_SOURCES = \
			hyperclient/hyperclient.cc \
			hyperclient/hyperclient_c_wrappers.cc \
			hyperclient/hyperclient_pending.cc \
			hyperclient/hyperclient_pending_get.cc \
			hyperclient/hyperclient_pending_search.cc \
			hyperclient/hyperclient_pending_statusonly.cc \
			hyperclient/util.cc \
			${libhyperdex_sources}
libhyperclient_la_LIBADD = \
			libhyperspacehashing.la \
			$(E_LIBS) \
			$(BUSYBEE_LIBS) \
			-lbusybee-st \
			$(COVERAGE_LDADD)
libhyperclient_la_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			-I$(abs_top_srcdir)/hyperdex \
			$(E_CFLAGS) \
			$(CPPFLAGS)

################################# Java Bindings ################################

java_bindings_noinst_headers = \
			hyperclient/java/syncclient.h
java_bindings_cleanfiles = \
			$(hyperclient_jarfile) \
			hyperclient/java/hyperclient_wrap.cxx \
			hyperclient/java/hyperclient/*.java \
			hyperclient/java/hyperclient/*.class \
			hyperclient-java.stamp

libhyperclient_java_la_SOURCES = hyperclient/java/syncclient.cc
nodist_libhyperclient_java_la_SOURCES = hyperclient/java/hyperclient_wrap.cxx
libhyperclient_java_la_CPPFLAGS = \
			$(JNI_CPPFLAGS) \
			$(CPPFLAGS)
libhyperclient_java_la_CXXFLAGS = \
			-fno-strict-aliasing
libhyperclient_java_la_LIBADD = \
			libhyperspacehashing.la \
			libhyperclient.la \
			$(PYTHON_LDFLAGS) \
			$(COVERAGE_LDADD)

hyperclient/java/hyperclient_wrap.cxx: hyperclient/java/hyperclient.i hyperclient/hyperclient.h hyperclient/java/syncclient.h
	mkdir -p hyperclient/java/hyperclient
	${SWIG} -java -package hyperclient -outdir $(abs_builddir)/hyperclient/java/hyperclient \
		-o $(abs_builddir)/hyperclient/java/hyperclient_wrap.cxx -w518 $(abs_top_srcdir)/hyperclient/java/hyperclient.i

hyperclient_jarfile = hyperclient-$(VERSION).jar

hyperclient-java.stamp: hyperclient/java/hyperclient_wrap.cxx
	javac hyperclient/java/hyperclient/*.java
	date > hyperclient-java.stamp

$(hyperclient_jarfile): hyperclient-java.stamp
	cd $(abs_top_builddir)/hyperclient/java ; $(JAR) cvf $(JARFLAGS) $(abs_top_builddir)/$(hyperclient_jarfile) hyperclient/*.class

if ENABLE_JAVA_BINDINGS
lib_LTLIBRARIES += libhyperclient-java.la
hyperclientjardir = $(datadir)/java
hyperclientjar_DATA = $(hyperclient_jarfile)
endif

################################ Python Bindings ###############################

if ENABLE_PYTHON_BINDINGS
pyexec_LTLIBRARIES += \
			hyperclient/python/hyperclient.la
endif

hyperclient/python/hyperclient.c: hyperclient/python/hyperclient.pyx
	cython hyperclient/python/hyperclient.pyx

hyperclient_python_hyperclient_la_SOURCES = \
			hyperclient/python/hyperclient.c
hyperclient_python_hyperclient_la_CPPFLAGS = \
			$(PYTHON_CPPFLAGS) \
			$(CPPFLAGS)
hyperclient_python_hyperclient_la_LIBADD = \
			libhyperspacehashing.la \
			libhyperclient.la \
			$(PYTHON_LDFLAGS) \
			$(COVERAGE_LDADD)
hyperclient_python_hyperclient_la_LDFLAGS = \
			-module \
			-avoid-version \
			-export-symbols-regex inithyperclient

################################## C Bindings ##################################

hyperclient_c_testcompile_SOURCES = hyperclient/c/testcompile.c
hyperclient_c_testcompile_CPPFLAGS = -I$(abs_top_srcdir)/hyperclient

################################################################################
################################## Benchmarks ##################################
################################################################################

##################################### YCSB #####################################

ycsb_extra_dist = \
			ycsb/hyperclient/HyperClientYCSB.java

ycsb_cleanfiles = \
			$(ycsb_jarfile) \
			ycsb/hyperclient/HyperClientYCSB.class

ycsb_jarfile = hyperclient-ycsb-$(VERSION).jar

ycsb/hyperclient/HyperClientYCSB.class: $(hyperclient_jarfile) ycsb/hyperclient/HyperClientYCSB.java
	mkdir -p $(abs_top_builddir)/ycsb/hyperclient
	javac -cp $(abs_top_builddir)/$(hyperclient_jarfile):$(CLASSPATH) -d $(abs_top_builddir)/ycsb $(abs_top_srcdir)/ycsb/hyperclient/HyperClientYCSB.java

$(ycsb_jarfile): ycsb/hyperclient/HyperClientYCSB.class
	cd $(abs_top_builddir)/ycsb; $(JAR) cvf $(JARFLAGS) $(abs_top_builddir)/$(ycsb_jarfile) hyperclient/HyperClientYCSB.class

if ENABLE_JAVA_BINDINGS
ycsbjardir = $(datadir)/java
ycsbjar_DATA = $(ycsb_jarfile)
endif

################################################################################
################################### Binaries ###################################
################################################################################

hyperdex_binary_test_SOURCES = \
			binary-test.cc
hyperdex_binary_test_LDADD = \
			libhyperclient.la \
			-lrt
hyperdex_binary_test_CPPFLAGS = \
			$(E_CFLAGS) \
			$(CPPFLAGS)

hyperdex_simple_consistency_stress_test_SOURCES = \
			simple-consistency-stress-test.cc
hyperdex_simple_consistency_stress_test_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperclient \
			$(E_CFLAGS) \
			$(CPPFLAGS)
hyperdex_simple_consistency_stress_test_LDADD = \
			libhyperclient.la \
			-lpopt -lpthread

hyperdex_replication_stress_test_SOURCES = \
			replication-stress-test.cc
hyperdex_replication_stress_test_CPPFLAGS = \
			-I$(abs_top_srcdir)/hyperspacehashing \
			$(E_CFLAGS) \
			$(CPPFLAGS)
hyperdex_replication_stress_test_LDADD = \
			libhyperspacehashing.la \
			libhyperclient.la \
			-lpopt

if ENABLE_TRACEPLAYER
bin_PROGRAMS += hyperdex-trace-player
hyperdex_trace_player_SOURCES = \
			trace-player.cc
hyperdex_trace_player_LDADD = \
			libhyperclient.la \
			-lbsd \
			-lrt
hyperdex_trace_player_CPPFLAGS = \
			$(E_CFLAGS) \
			$(CPPFLAGS)
endif

################################################################################
################################# Documentation ################################
################################################################################

man_MANS = \
			doc/man/hyperdex-daemon.1 \
			doc/man/hyperdex-replication-stress-test.1 \
			doc/man/hyperdex-simple-consistency-stress-test.1

# These need to be chained so that they'll only build once, and will not rely
# upon a PHONY rule.
doc/man/hyperdex-replication-stress-test.1: doc/man/hyperdex-replication-stress-test.rst doc/man/hyperdex-simple-consistency-stress-test.1
doc/man/hyperdex-simple-consistency-stress-test.1: doc/man/hyperdex-simple-consistency-stress-test.rst doc/man/hyperdex-daemon.1
doc/man/hyperdex-daemon.1: doc/man/hyperdex-daemon.rst
	sphinx-build -W -d ${abs_top_builddir}/doc/_build -b man doc doc/man

doc/HyperDex-$(VERSION).pdf:
	sphinx-build -W -d ${abs_top_builddir}/doc/_build -b latex ${abs_top_srcdir}/doc ${abs_top_builddir}/doc/_build/latex
	make -C ${abs_top_builddir}/doc/_build/latex all-pdf
	cp ${abs_top_builddir}/doc/_build/latex/HyperDex.pdf doc/HyperDex-$(VERSION).pdf

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
