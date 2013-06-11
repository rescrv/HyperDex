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
AM_CXXFLAGS = $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(WANAL_CXXFLAGS)
AM_MAKEFLAGS = --no-print-directory
AM_YFLAGS = -d

BUILT_SOURCES = client/parse_space_y.h

EXTRA_DIST =
EXTRA_DIST += LICENSE
EXTRA_DIST += doc
EXTRA_DIST += doc/HyperDex-$(VERSION).pdf
EXTRA_DIST += $(libhyperclient_extra_dist)
EXTRA_DIST += $(ycsb_extra_dist)
EXTRA_DIST += hyperclient/nodejs/sync_test.js
EXTRA_DIST += hyperclient/nodejs/async_test.js
EXTRA_DIST += hyperclient/nodejs/hyperclient.cc
EXTRA_DIST += wscript
EXTRA_DIST += windows/hyperclientclr.cpp
EXTRA_DIST += windows/marshal.cpp

lib_LTLIBRARIES = libhyperclient.la

pyexec_LTLIBRARIES =

include_HEADERS =
include_HEADERS += hyperdex.h

bin_PROGRAMS =
bin_PROGRAMS += hyperdex
bin_PROGRAMS += hyperdex-search-stress-test
bin_PROGRAMS += hyperdex-replication-stress-test
bin_PROGRAMS += hyperdex-simple-consistency-stress-test

TESTS = $(check_PROGRAMS)
check_PROGRAMS =

hyperdexexecdir = $(exec_prefix)/libexec/$(PACKAGE)-$(VERSION)
hyperdexexec_PROGRAMS =
hyperdexexec_PROGRAMS += hyperdex-initialize-cluster
hyperdexexec_PROGRAMS += hyperdex-daemon
hyperdexexec_PROGRAMS += hyperdex-coordinator
hyperdexexec_PROGRAMS += hyperdex-add-space
hyperdexexec_PROGRAMS += hyperdex-validate-space
hyperdexexec_PROGRAMS += hyperdex-rm-space
hyperdexexec_PROGRAMS += hyperdex-show-config
hyperdexexec_PROGRAMS += hyperdex-async-benchmark
hyperdexexec_PROGRAMS += hyperdex-benchmark
hyperdexexec_PROGRAMS += hyperdex-initiate-transfer
hyperdexexec_LTLIBRARIES = libhypercoordinator.la

noinst_PROGRAMS =
noinst_PROGRAMS += client/c/testcompile
noinst_PROGRAMS += client/cc/testcompile

CONFIG_CLEAN_FILES = hyperclient.pc

CLEANFILES =
CLEANFILES += $(java_bindings_cleanfiles)
CLEANFILES += doc/HyperDex-$(VERSION).pdf
CLEANFILES += $(ycsb_cleanfiles)

JAVAROOT = $(abs_top_srcdir)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = hyperclient.pc

if ENABLE_DOCS
.PHONY: coverage doc/HyperDex-$(VERSION).pdf
else
.PHONY: coverage
endif

dist-hook:
	-rm -rf $(distdir)/doc/_build

clean-local:
	-rm -rf $(abs_top_builddir)/doc/_build

noinst_HEADERS =
noinst_HEADERS += common/attribute_check.h
noinst_HEADERS += common/attribute.h
noinst_HEADERS += common/capture.h
noinst_HEADERS += common/configuration.h
noinst_HEADERS += common/coordinator_returncode.h
noinst_HEADERS += common/counter_map.h
noinst_HEADERS += common/datatypes.h
noinst_HEADERS += common/datatype_string.h
noinst_HEADERS += common/datatype_int64.h
noinst_HEADERS += common/datatype_float.h
noinst_HEADERS += common/datatype_map.h
noinst_HEADERS += common/datatype_set.h
noinst_HEADERS += common/datatype_list.h
noinst_HEADERS += common/funcall.h
noinst_HEADERS += common/hash.h
noinst_HEADERS += common/hyperspace.h
noinst_HEADERS += common/ids.h
noinst_HEADERS += common/macros.h
noinst_HEADERS += common/mapper.h
noinst_HEADERS += common/network_msgtype.h
noinst_HEADERS += common/network_returncode.h
noinst_HEADERS += common/range.h
noinst_HEADERS += common/range_searches.h
noinst_HEADERS += common/regex_match.h
noinst_HEADERS += common/schema.h
noinst_HEADERS += common/serialization.h
noinst_HEADERS += common/transfer.h
noinst_HEADERS += coordinator/coordinator.h
noinst_HEADERS += coordinator/missing_acks.h
noinst_HEADERS += coordinator/server_state.h
noinst_HEADERS += coordinator/transitions.h
noinst_HEADERS += daemon/communication.h
noinst_HEADERS += daemon/coordinator_link.h
noinst_HEADERS += daemon/daemon.h
noinst_HEADERS += daemon/datalayer.h
noinst_HEADERS += daemon/datalayer_encodings.h
noinst_HEADERS += daemon/leveldb.h
noinst_HEADERS += daemon/reconfigure_returncode.h
noinst_HEADERS += daemon/replication_manager.h
noinst_HEADERS += daemon/replication_manager_key_state.h
noinst_HEADERS += daemon/replication_manager_key_state_reference.h
noinst_HEADERS += daemon/replication_manager_key_region.h
noinst_HEADERS += daemon/replication_manager_pending.h
noinst_HEADERS += daemon/search_manager.h
noinst_HEADERS += daemon/state_transfer_manager.h
noinst_HEADERS += daemon/state_transfer_manager_pending.h
noinst_HEADERS += daemon/state_transfer_manager_transfer_in_state.h
noinst_HEADERS += daemon/state_transfer_manager_transfer_out_state.h
noinst_HEADERS += client/client.h
noinst_HEADERS += client/constants.h
noinst_HEADERS += client/coordinator_link.h
noinst_HEADERS += client/hyperclient.h
noinst_HEADERS += client/hyperclient.hpp
noinst_HEADERS += client/keyop_info.h
noinst_HEADERS += client/partition.h
noinst_HEADERS += client/pending.h
noinst_HEADERS += client/pending_count.h
noinst_HEADERS += client/pending_get.h
noinst_HEADERS += client/pending_group_del.h
noinst_HEADERS += client/pending_search.h
noinst_HEADERS += client/pending_search_describe.h
noinst_HEADERS += client/pending_sorted_search.h
noinst_HEADERS += client/pending_aggregation.h
noinst_HEADERS += client/pending_atomic.h
noinst_HEADERS += client/util.h
noinst_HEADERS += osx/ieee754.h
noinst_HEADERS += tools/common.h
noinst_HEADERS += windows/hyperclientclr.h
noinst_HEADERS += windows/ieee754.h
noinst_HEADERS += windows/marshal.h
noinst_HEADERS += windows/unistd.h

################################################################################
#################################### Daemon ####################################
################################################################################

hyperdex_daemon_SOURCES =
hyperdex_daemon_SOURCES += common/attribute.cc
hyperdex_daemon_SOURCES += common/attribute_check.cc
hyperdex_daemon_SOURCES += common/capture.cc
hyperdex_daemon_SOURCES += common/configuration.cc
hyperdex_daemon_SOURCES += common/counter_map.cc
hyperdex_daemon_SOURCES += common/datatypes.cc
hyperdex_daemon_SOURCES += common/datatype_string.cc
hyperdex_daemon_SOURCES += common/datatype_int64.cc
hyperdex_daemon_SOURCES += common/datatype_float.cc
hyperdex_daemon_SOURCES += common/datatype_list.cc
hyperdex_daemon_SOURCES += common/datatype_set.cc
hyperdex_daemon_SOURCES += common/datatype_map.cc
hyperdex_daemon_SOURCES += common/funcall.cc
hyperdex_daemon_SOURCES += common/hash.cc
hyperdex_daemon_SOURCES += common/hyperdex.cc
hyperdex_daemon_SOURCES += common/hyperspace.cc
hyperdex_daemon_SOURCES += common/network_msgtype.cc
hyperdex_daemon_SOURCES += common/mapper.cc
hyperdex_daemon_SOURCES += common/ordered_encoding.cc
hyperdex_daemon_SOURCES += common/range.cc
hyperdex_daemon_SOURCES += common/range_searches.cc
hyperdex_daemon_SOURCES += common/regex_match.cc
hyperdex_daemon_SOURCES += common/schema.cc
hyperdex_daemon_SOURCES += common/serialization.cc
hyperdex_daemon_SOURCES += common/transfer.cc
hyperdex_daemon_SOURCES += daemon/communication.cc
hyperdex_daemon_SOURCES += daemon/coordinator_link.cc
hyperdex_daemon_SOURCES += daemon/daemon.cc
hyperdex_daemon_SOURCES += daemon/datalayer.cc
hyperdex_daemon_SOURCES += daemon/datalayer_encodings.cc
hyperdex_daemon_SOURCES += daemon/datalayer_iterator.cc
hyperdex_daemon_SOURCES += daemon/index_container.cc
hyperdex_daemon_SOURCES += daemon/index_float.cc
hyperdex_daemon_SOURCES += daemon/index_info.cc
hyperdex_daemon_SOURCES += daemon/index_int64.cc
hyperdex_daemon_SOURCES += daemon/index_list.cc
hyperdex_daemon_SOURCES += daemon/index_map.cc
hyperdex_daemon_SOURCES += daemon/index_primitive.cc
hyperdex_daemon_SOURCES += daemon/index_set.cc
hyperdex_daemon_SOURCES += daemon/index_string.cc
hyperdex_daemon_SOURCES += daemon/main.cc
hyperdex_daemon_SOURCES += daemon/replication_manager.cc
hyperdex_daemon_SOURCES += daemon/replication_manager_key_state.cc
hyperdex_daemon_SOURCES += daemon/replication_manager_key_state_reference.cc
hyperdex_daemon_SOURCES += daemon/replication_manager_key_region.cc
hyperdex_daemon_SOURCES += daemon/replication_manager_pending.cc
hyperdex_daemon_SOURCES += daemon/search_manager.cc
hyperdex_daemon_SOURCES += daemon/state_transfer_manager.cc
hyperdex_daemon_SOURCES += daemon/state_transfer_manager_pending.cc
hyperdex_daemon_SOURCES += daemon/state_transfer_manager_transfer_in_state.cc
hyperdex_daemon_SOURCES += daemon/state_transfer_manager_transfer_out_state.cc
hyperdex_daemon_LDADD = \
			$(E_LIBS) \
			$(BUSYBEE_LIBS) -lhyperleveldb \
			$(REPLICANT_LIBS) -lcityhash -lpopt -lglog -lpthread
hyperdex_daemon_CPPFLAGS = $(CPPFLAGS)

################################################################################
################################## Coordinator #################################
################################################################################

libhypercoordinator_la_SOURCES =
libhypercoordinator_la_SOURCES += common/attribute.cc
libhypercoordinator_la_SOURCES += common/capture.cc
libhypercoordinator_la_SOURCES += common/hyperdex.cc
libhypercoordinator_la_SOURCES += common/hyperspace.cc
libhypercoordinator_la_SOURCES += common/schema.cc
libhypercoordinator_la_SOURCES += common/serialization.cc
libhypercoordinator_la_SOURCES += common/transfer.cc
libhypercoordinator_la_SOURCES += coordinator/coordinator.cc
libhypercoordinator_la_SOURCES += coordinator/symtable.c
libhypercoordinator_la_LIBADD = $(E_LIBS)
libhypercoordinator_la_CPPFLAGS = $(REPLICANT_CFLAGS)

################################################################################
#################################### Client ####################################
################################################################################

#################################### Library ###################################

libhyperclient_extra_dist =
libhyperclient_extra_dist += client/keyop_info.gperf
libhyperclient_extra_dist += client/java/examples/HyperBinaryTest.java
libhyperclient_extra_dist += client/java/examples/HyperSearchFloatRange.java
libhyperclient_extra_dist += client/java/examples/HyperTest.java
libhyperclient_extra_dist += client/java/examples/README.txt
libhyperclient_extra_dist += client/java/examples/source_me
libhyperclient_extra_dist += client/java/extra_src/AttributeError.java
libhyperclient_extra_dist += client/java/extra_src/ByteArray.java
libhyperclient_extra_dist += client/java/extra_src/ByteArrayKeyedMap.java
libhyperclient_extra_dist += client/java/extra_src/ByteArrayKeyedSortedMap.java
libhyperclient_extra_dist += client/java/extra_src/ByteArraySortedSet.java
libhyperclient_extra_dist += client/java/extra_src/ByteArrayVector.java
libhyperclient_extra_dist += client/java/extra_src/DeferredCondPut.java
libhyperclient_extra_dist += client/java/extra_src/DeferredCount.java
libhyperclient_extra_dist += client/java/extra_src/DeferredDelete.java
libhyperclient_extra_dist += client/java/extra_src/DeferredFromAttrs.java
libhyperclient_extra_dist += client/java/extra_src/DeferredGet.java
libhyperclient_extra_dist += client/java/extra_src/DeferredGroupDel.java
libhyperclient_extra_dist += client/java/extra_src/Deferred.java
libhyperclient_extra_dist += client/java/extra_src/DeferredMapOp.java
libhyperclient_extra_dist += client/java/extra_src/GreaterEqual.java
libhyperclient_extra_dist += client/java/extra_src/HyperClientException.java
libhyperclient_extra_dist += client/java/extra_src/LessEqual.java
libhyperclient_extra_dist += client/java/extra_src/MapOp.java
libhyperclient_extra_dist += client/java/extra_src/MemoryError.java
libhyperclient_extra_dist += client/java/extra_src/Pending.java
libhyperclient_extra_dist += client/java/extra_src/Predicate.java
libhyperclient_extra_dist += client/java/extra_src/Range.java
libhyperclient_extra_dist += client/java/extra_src/SearchBase.java
libhyperclient_extra_dist += client/java/extra_src/Search.java
libhyperclient_extra_dist += client/java/extra_src/SimpleOp.java
libhyperclient_extra_dist += client/java/extra_src/SortedSearch.java
libhyperclient_extra_dist += client/java/extra_src/TypeError.java
libhyperclient_extra_dist += client/java/extra_src/ValueError.java
libhyperclient_extra_dist += client/java/hyperclient.i
libhyperclient_extra_dist += client/java/proxies/hyperclient_attribute_check.i
libhyperclient_extra_dist += client/java/proxies/hyperclient_attribute.i
libhyperclient_extra_dist += client/java/proxies/HyperClient.i
libhyperclient_extra_dist += client/java/proxies/hyperclient_map_attribute.i
libhyperclient_extra_dist += client/python/hyperclient.pyx
libhyperclient_extra_dist += client/ruby/type_conversion.h
libhyperclient_extra_dist += hyperclient.pc

include_HEADERS += client/hyperclient.h

libhyperclient_la_SOURCES =
libhyperclient_la_SOURCES += common/attribute.cc
libhyperclient_la_SOURCES += common/attribute_check.cc
libhyperclient_la_SOURCES += common/capture.cc
libhyperclient_la_SOURCES += common/configuration.cc
libhyperclient_la_SOURCES += common/datatypes.cc
libhyperclient_la_SOURCES += common/datatype_string.cc
libhyperclient_la_SOURCES += common/datatype_int64.cc
libhyperclient_la_SOURCES += common/datatype_float.cc
libhyperclient_la_SOURCES += common/datatype_list.cc
libhyperclient_la_SOURCES += common/datatype_set.cc
libhyperclient_la_SOURCES += common/datatype_map.cc
libhyperclient_la_SOURCES += common/funcall.cc
libhyperclient_la_SOURCES += common/hash.cc
libhyperclient_la_SOURCES += common/hyperdex.cc
libhyperclient_la_SOURCES += common/hyperspace.cc
libhyperclient_la_SOURCES += common/mapper.cc
libhyperclient_la_SOURCES += common/ordered_encoding.cc
libhyperclient_la_SOURCES += common/network_msgtype.cc
libhyperclient_la_SOURCES += common/range.cc
libhyperclient_la_SOURCES += common/range_searches.cc
libhyperclient_la_SOURCES += common/regex_match.cc
libhyperclient_la_SOURCES += common/schema.cc
libhyperclient_la_SOURCES += common/serialization.cc
libhyperclient_la_SOURCES += common/transfer.cc
libhyperclient_la_SOURCES += client/client.cc
libhyperclient_la_SOURCES += client/coordinator_link.cc
libhyperclient_la_SOURCES += client/hyperclient.cc
libhyperclient_la_SOURCES += client/hyperspace_builder.cc
libhyperclient_la_SOURCES += client/keyop_info.cc
libhyperclient_la_SOURCES += client/parse_space_l.l
libhyperclient_la_SOURCES += client/parse_space_y.y
libhyperclient_la_SOURCES += client/partition.cc
libhyperclient_la_SOURCES += client/pending.cc
libhyperclient_la_SOURCES += client/pending_aggregation.cc
libhyperclient_la_SOURCES += client/pending_atomic.cc
libhyperclient_la_SOURCES += client/pending_count.cc
libhyperclient_la_SOURCES += client/pending_get.cc
libhyperclient_la_SOURCES += client/pending_group_del.cc
libhyperclient_la_SOURCES += client/pending_search.cc
libhyperclient_la_SOURCES += client/pending_search_describe.cc
libhyperclient_la_SOURCES += client/pending_sorted_search.cc
libhyperclient_la_SOURCES += client/util.cc
libhyperclient_la_CPPFLAGS = -I${abs_top_srcdir}/client
libhyperclient_la_LIBADD =
libhyperclient_la_LIBADD += $(E_LIBS)
libhyperclient_la_LIBADD += $(BUSYBEE_LIBS)
libhyperclient_la_LIBADD += $(REPLICANT_LIBS)
libhyperclient_la_LIBADD += -lcityhash

gperf_verbose = $(gperf_verbose_$(V))
gperf_verbose_ = $(gperf_verbose_$(AM_DEFAULT_VERBOSITY))
gperf_verbose_0 = @echo "  GPERF " $@;

client/keyop_info.cc: client/keyop_info.gperf
	$(gperf_verbose)gperf -m 100 $(abs_top_srcdir)/client/keyop_info.gperf --output-file=$(abs_top_builddir)/client/keyop_info.cc

################################# Java Bindings ################################

java_bindings_cleanfiles = \
			$(hyperclient_jarfile) \
			client/java/hyperclient_wrap.cxx \
			client/java/hyperclient/*.java \
			client/java/hyperclient/*.class \
			hyperclient-java.stamp

nodist_libhyperclient_java_la_SOURCES = client/java/hyperclient_wrap.cxx
libhyperclient_java_la_CPPFLAGS = \
			$(JNI_CPPFLAGS) \
			$(CPPFLAGS)
libhyperclient_java_la_CXXFLAGS = \
			-I$(abs_top_srcdir)/client \
			-fno-strict-aliasing
libhyperclient_java_la_LIBADD = \
			libhyperclient.la \
			$(PYTHON_LDFLAGS)

swig_verbose = $(swig_verbose_$(V))
swig_verbose_ = $(swig_verbose_$(AM_DEFAULT_VERBOSITY))
swig_verbose_0 = @echo "  SWIG  " $@;

client/java/hyperclient_wrap.cxx: client/java/hyperclient.i client/hyperclient.h
	$(swig_verbose)mkdir -p $(abs_top_builddir)/client/java/hyperclient &&  \
	${SWIG} -I${abs_srcdir} -java -package hyperclient -outdir $(abs_builddir)/client/java/hyperclient \
		-o $(abs_builddir)/client/java/hyperclient_wrap.cxx -w518 $(abs_top_srcdir)/client/java/hyperclient.i

hyperclient_jarfile = hyperclient-$(VERSION).jar

hyperclient-java.stamp: client/java/hyperclient_wrap.cxx
	cp ${abs_srcdir}/client/java/extra_src/*.java ${abs_builddir}/client/java/hyperclient/.
	javac ${abs_builddir}/client/java/hyperclient/*.java
	date > ${abs_builddir}/hyperclient-java.stamp

$(hyperclient_jarfile): hyperclient-java.stamp
	cd $(abs_top_builddir)/client/java ; $(JAR) cvf $(JARFLAGS) $(abs_top_builddir)/$(hyperclient_jarfile) hyperclient/*.class

if ENABLE_JAVA_BINDINGS
lib_LTLIBRARIES += libhyperclient-java.la
hyperclientjardir = $(datadir)/java
hyperclientjar_DATA = $(hyperclient_jarfile)
endif

################################ Python Bindings ###############################

if ENABLE_PYTHON_BINDINGS
pyexec_LTLIBRARIES += client/python/hyperclient.la
endif

pyx_verbose = $(pyx_verbose_$(V))
pyx_verbose_ = $(pyx_verbose_$(AM_DEFAULT_VERBOSITY))
pyx_verbose_0 = @echo "  PYX   " $@;

client/python/hyperclient.c: client/python/hyperclient.pyx
	$(pyx_verbose)cython client/python/hyperclient.pyx

client_python_hyperclient_la_SOURCES = client/python/hyperclient.c
client_python_hyperclient_la_CPPFLAGS = $(PYTHON_CPPFLAGS) $(CPPFLAGS)
client_python_hyperclient_la_LIBADD = libhyperclient.la $(PYTHON_LDFLAGS)
client_python_hyperclient_la_LDFLAGS = -module -avoid-version -export-symbols-regex inithyperclient

################################# Ruby Bindings ################################

if ENABLE_RUBY_BINDINGS
rubydir = $(DESTDIR)$(RUBY_EXT_LIB)
ruby_LTLIBRARIES = client/ruby/hyperclient.la
endif

client_ruby_hyperclient_la_SOURCES = client/ruby/hyperclient.c client/ruby/type_conversion.cc
client_ruby_hyperclient_la_CPPFLAGS = -I$(RUBY_EXT_INC) -I$(RUBY_EXT_ARCHINC) $(RUBY_EXT_CPPFLAGS) $(CPPFLAGS)
client_ruby_hyperclient_la_LIBADD = libhyperclient.la $(RUBY_EXT_LDFLAGS)
client_ruby_hyperclient_la_LDFLAGS = -module -avoid-version

################################ C/C++ Bindings ################################

client_c_testcompile_SOURCES = client/c/testcompile.c
client_c_testcompile_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
client_c_testcompile_LDADD = libhyperclient.la
client_cc_testcompile_SOURCES = client/cc/testcompile.cc
client_cc_testcompile_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
client_cc_testcompile_LDADD = libhyperclient.la

################################################################################
##################################### Tests ####################################
################################################################################

th_sources = test/runner.cc th.cc th.h

check_PROGRAMS += common/test/ordered_encoding
common_test_ordered_encoding_SOURCES = common/test/ordered_encoding.cc common/ordered_encoding.cc $(th_sources)

################################################################################
##################################### Tools ####################################
################################################################################

hyperdex_SOURCES = hyperdex.cc
hyperdex_CPPFLAGS = -DHYPERDEX_EXEC_DIR=\""$(hyperdexexecdir)\"" $(CPPFLAGS)
hyperdex_LDADD = -lpopt

hyperdex_initialize_cluster_SOURCES = tools/initialize-cluster.cc
hyperdex_initialize_cluster_CPPFLAGS = -DHYPERDEX_COORD_LIB=\""$(hyperdexexecdir)/libhypercoordinator\"" -I${abs_top_srcdir}/client $(CPPFLAGS)
hyperdex_initialize_cluster_LDADD = libhyperclient.la -lpopt

hyperdex_coordinator_SOURCES = tools/coordinator.cc
hyperdex_coordinator_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
hyperdex_coordinator_LDADD = libhyperclient.la $(E_LIBS) $(BUSYBEE_LIBS) -lpopt

hyperdex_add_space_SOURCES = tools/add-space.cc
hyperdex_add_space_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
hyperdex_add_space_LDADD = libhyperclient.la -lpopt

hyperdex_validate_space_SOURCES = tools/validate-space.cc
hyperdex_validate_space_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
hyperdex_validate_space_LDADD = libhyperclient.la

hyperdex_rm_space_SOURCES = tools/rm-space.cc
hyperdex_rm_space_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
hyperdex_rm_space_LDADD = libhyperclient.la -lpopt

hyperdex_show_config_SOURCES = tools/show-config.cc
hyperdex_show_config_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
hyperdex_show_config_LDADD = libhyperclient.la -lpopt

hyperdex_async_benchmark_SOURCES = tools/async-benchmark.cc
hyperdex_async_benchmark_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
hyperdex_async_benchmark_LDADD = libhyperclient.la -lhyperleveldb $(E_LIBS) -lpopt

hyperdex_benchmark_SOURCES = tools/benchmark.cc
hyperdex_benchmark_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
hyperdex_benchmark_LDADD = libhyperclient.la -lhyperleveldb $(E_LIBS) -lpopt

hyperdex_initiate_transfer_SOURCES = tools/initiate-transfer.cc
hyperdex_initiate_transfer_CPPFLAGS = -I${abs_top_srcdir}/client $(CPPFLAGS)
hyperdex_initiate_transfer_LDADD = libhyperclient.la -lpopt

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

if ENABLE_YCSB_DRIVER
ycsbjardir = $(datadir)/java
ycsbjar_DATA = $(ycsb_jarfile)
endif

################################################################################
################################### Binaries ###################################
################################################################################

hyperdex_replication_stress_test_SOURCES = replication-stress-test.cc
hyperdex_replication_stress_test_CPPFLAGS = -I${abs_top_srcdir}/client
hyperdex_replication_stress_test_LDADD = libhyperclient.la -lpopt $(E_LIBS)

hyperdex_search_stress_test_SOURCES = search-stress-test.cc
hyperdex_search_stress_test_CPPFLAGS = -I${abs_top_srcdir}/client
hyperdex_search_stress_test_LDADD = libhyperclient.la -lpopt $(E_LIBS)

hyperdex_simple_consistency_stress_test_SOURCES = simple-consistency-stress-test.cc
hyperdex_simple_consistency_stress_test_CPPFLAGS = -I${abs_top_srcdir}/client
hyperdex_simple_consistency_stress_test_LDADD = libhyperclient.la -lpopt -lpthread

################################################################################
################################# Documentation ################################
################################################################################

# These need to be chained so that they'll only build once, and will not rely
# upon a PHONY rule.
if ENABLE_DOCS
doc/man/hyperdex-daemon.1: doc/man/hyperdex-daemon.rst
	sphinx-build -W -d ${abs_top_builddir}/doc/_build -b man doc doc/man

doc/HyperDex-$(VERSION).pdf:
	sphinx-build -W -d ${abs_top_builddir}/doc/_build -b latex ${abs_top_srcdir}/doc ${abs_top_builddir}/doc/_build/latex
	make -C ${abs_top_builddir}/doc/_build/latex all-pdf
	cp ${abs_top_builddir}/doc/_build/latex/HyperDex.pdf doc/HyperDex-$(VERSION).pdf
endif

################################################################################
################################### Man Pages ##################################
################################################################################

pandoc_verbose = $(pandoc_verbose_$(V))
pandoc_verbose_ = $(pandoc_verbose_$(AM_DEFAULT_VERBOSITY))
pandoc_verbose_0 = @echo "  PANDOC" $@;
sed_verbose = $(sed_verbose_$(V))
sed_verbose_ = $(sed_verbose_$(AM_DEFAULT_VERBOSITY))
sed_verbose_0 = @echo "  SED   " $@;
help2man_verbose = $(help2man_verbose_$(V))
help2man_verbose_ = $(help2man_verbose_$(AM_DEFAULT_VERBOSITY))
help2man_verbose_0 = @echo "  MAN   " $@;

EXTRA_DIST += man/hyperdex.1.md man/hyperdex.1.h2m
dist_man_MANS = man/hyperdex.1

%.h2m: %.md
	$(pandoc_verbose)pandoc -s -w man -o $@ $<
	$(sed_verbose)sed -i -e 's/^\.SH \(.*\)$$/[\1]/' $@

man/hyperdex.1: man/hyperdex.1.h2m hyperdex.cc $(top_srcdir)/configure.ac
	@$(MAKE) --no-print-directory $(AM_MAKEFLAGS) hyperdex$(EXEEXT)
	$(help2man_verbose)help2man --libtool --no-info --section 1 --output $@ --version-string=$(VERSION) --include man/hyperdex.1.h2m ./hyperdex$(EXEEXT)
