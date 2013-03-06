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
AM_CXXFLAGS = $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(WANAL_CXXFLAGS) -fpermissive
AM_YFLAGS = -d

BUILT_SOURCES = client/parse_space_y.h

EXTRA_DIST = \
			LICENSE \
			doc \
			doc/HyperDex-$(VERSION).pdf \
			$(libhyperclient_extra_dist) \
			$(ycsb_extra_dist) \
			hyperclient/nodejs/sync_test.js \
			hyperclient/nodejs/async_test.js \
			hyperclient/nodejs/hyperclient.cc \
			wscript \
			windows/hyperclientclr.cpp \
			windows/marshal.cpp

lib_LTLIBRARIES = libhyperclient.la

pyexec_LTLIBRARIES =

include_HEADERS = \
			hyperdex.h

bin_PROGRAMS = hyperdex \
			hyperdex-search-stress-test \
			hyperdex-replication-stress-test \
			hyperdex-simple-consistency-stress-test

hyperdexexecdir = $(exec_prefix)/libexec/$(PACKAGE)-$(VERSION)
hyperdexexec_PROGRAMS = \
			hyperdex-initialize-cluster \
			hyperdex-daemon \
			hyperdex-coordinator \
			hyperdex-add-space \
			hyperdex-rm-space \
			hyperdex-show-config \
			hyperdex-async-benchmark \
			hyperdex-benchmark \
			hyperdex-initiate-transfer
hyperdexexec_LTLIBRARIES = libhypercoordinator.la

noinst_PROGRAMS = \
			client/c/testcompile \
			client/cc/testcompile

CONFIG_CLEAN_FILES = hyperclient.pc

CLEANFILES = \
			$(java_bindings_cleanfiles) \
			doc/HyperDex-$(VERSION).pdf \
			$(ycsb_cleanfiles)

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

noinst_HEADERS = \
			common/attribute_check.h \
			common/attribute.h \
			common/capture.h \
			common/configuration.h \
			common/coordinator_returncode.h \
			common/counter_map.h \
			common/float_encode.h \
			common/funcall.h \
			common/hash.h \
			common/hyperspace.h \
			common/ids.h \
			common/macros.h \
			common/mapper.h \
			common/network_msgtype.h \
			common/network_returncode.h \
			common/range_searches.h \
			common/schema.h \
			common/serialization.h \
			common/transfer.h \
			datatypes/alltypes.h \
			datatypes/apply.h \
			datatypes/coercion.h \
			datatypes/compare.h \
			datatypes/float.h \
			datatypes/int64.h \
			datatypes/list.h \
			datatypes/map.h \
			datatypes/microerror.h \
			datatypes/set.h \
			datatypes/sort.h \
			datatypes/step.h \
			datatypes/string.h \
			datatypes/validate.h \
			datatypes/write.h \
			coordinator/coordinator.h \
			coordinator/missing_acks.h \
			coordinator/server_state.h \
			coordinator/transitions.h \
			daemon/communication.h \
			daemon/coordinator_link.h \
			daemon/daemon.h \
			daemon/datalayer.h \
			daemon/datalayer_encodings.h \
			daemon/index_encode.h \
			daemon/leveldb.h \
			daemon/reconfigure_returncode.h \
			daemon/replication_manager.h \
			daemon/replication_manager_keyholder.h \
			daemon/replication_manager_keypair.h \
			daemon/replication_manager_pending.h \
			daemon/search_manager.h \
			daemon/state_transfer_manager.h \
			daemon/state_transfer_manager_pending.h \
			daemon/state_transfer_manager_transfer_in_state.h \
			daemon/state_transfer_manager_transfer_out_state.h \
			client/complete.h \
			client/constants.h \
			client/coordinator_link.h \
			client/description.h \
			client/hyperclient.h \
			client/keyop_info.h \
			client/parse_space_aux.h \
			client/partition.h \
			client/pending_count.h \
			client/pending_get.h \
			client/pending_group_del.h \
			client/pending.h \
			client/pending_search.h \
			client/pending_search_description.h \
			client/pending_sorted_search.h \
			client/pending_statusonly.h \
			client/refcount.h \
			client/space_description.h \
			client/tool_wrapper.h \
			client/util.h \
			client/wrap.h \
			osx/ieee754.h \
			test/common.h \
			tools/common.h \
			windows/hyperclientclr.h  \
			windows/ieee754.h \
			windows/marshal.h \
			windows/unistd.h

################################################################################
#################################### Daemon ####################################
################################################################################

hyperdex_daemon_SOURCES = \
			common/attribute.cc \
			common/attribute_check.cc \
			common/capture.cc \
			common/configuration.cc \
			common/counter_map.cc \
			common/float_encode.cc \
			common/funcall.cc \
			common/hash.cc \
			common/hyperdex.cc \
			common/hyperspace.cc \
			common/network_msgtype.cc \
			common/mapper.cc \
			common/range_searches.cc \
			common/schema.cc \
			common/serialization.cc \
			common/transfer.cc \
			daemon/communication.cc \
			daemon/coordinator_link.cc \
			daemon/daemon.cc \
			daemon/datalayer.cc \
			daemon/datalayer_encodings.cc \
			daemon/index_encode.cc \
			daemon/main.cc \
			daemon/replication_manager.cc \
			daemon/replication_manager_keyholder.cc \
			daemon/replication_manager_keypair.cc \
			daemon/replication_manager_pending.cc \
			daemon/search_manager.cc \
			daemon/state_transfer_manager.cc \
			daemon/state_transfer_manager_pending.cc \
			daemon/state_transfer_manager_transfer_in_state.cc \
			daemon/state_transfer_manager_transfer_out_state.cc \
			datatypes/apply.cc \
			datatypes/compare.cc \
			datatypes/float.cc \
			datatypes/int64.cc \
			datatypes/list.cc \
			datatypes/map.cc \
			datatypes/set.cc \
			datatypes/step.cc \
			datatypes/string.cc \
			datatypes/validate.cc \
			datatypes/write.cc
hyperdex_daemon_LDADD = \
			$(E_LIBS) \
			$(BUSYBEE_LIBS) -lleveldb \
			$(REPLICANT_LIBS) -lcityhash -lpopt -lglog -lpthread
hyperdex_daemon_CPPFLAGS = $(CPPFLAGS)

#daemon_test_index_encode_SOURCES = runner.cc daemon/test/index_encode.cc daemon/index_encode.cc common/float_encode.cc
#daemon_test_index_encode_CPPFLAGS = $(GTEST_CPPFLAGS) $(CPPFLAGS)
#daemon_test_index_encode_LDADD = $(GTEST_LDFLAGS) -lgtest -lpthread

################################################################################
################################## Coordinator #################################
################################################################################

libhypercoordinator_la_SOURCES = \
			common/attribute.cc \
			common/capture.cc \
			common/hyperdex.cc \
			common/hyperspace.cc \
			common/schema.cc \
			common/serialization.cc \
			common/transfer.cc \
			coordinator/coordinator.cc \
			coordinator/symtable.c
libhypercoordinator_la_LIBADD = $(E_LIBS) 
libhypercoordinator_la_CPPFLAGS = $(REPLICANT_CFLAGS)

################################################################################
#################################### Client ####################################
################################################################################

#################################### Library ###################################

libhyperclient_extra_dist = \
			client/keyop_info.gperf \
			client/java/examples/HyperBinaryTest.java \
			client/java/examples/HyperSearchFloatRange.java \
			client/java/examples/HyperTest.java \
			client/java/examples/README.txt \
			client/java/examples/source_me \
			client/java/extra_src/AttributeError.java \
			client/java/extra_src/ByteArray.java \
			client/java/extra_src/ByteArrayKeyedMap.java \
			client/java/extra_src/ByteArrayKeyedSortedMap.java \
			client/java/extra_src/ByteArraySortedSet.java \
			client/java/extra_src/ByteArrayVector.java \
			client/java/extra_src/DeferredCondPut.java \
			client/java/extra_src/DeferredCount.java \
			client/java/extra_src/DeferredDelete.java \
			client/java/extra_src/DeferredFromAttrs.java \
			client/java/extra_src/DeferredGet.java \
			client/java/extra_src/DeferredGroupDel.java \
			client/java/extra_src/Deferred.java \
			client/java/extra_src/DeferredMapOp.java \
			client/java/extra_src/GreaterEqual.java \
			client/java/extra_src/HyperClientException.java \
			client/java/extra_src/LessEqual.java \
			client/java/extra_src/MapOp.java \
			client/java/extra_src/MemoryError.java \
			client/java/extra_src/Pending.java \
			client/java/extra_src/Predicate.java \
			client/java/extra_src/Range.java \
			client/java/extra_src/SearchBase.java \
			client/java/extra_src/Search.java \
			client/java/extra_src/SimpleOp.java \
			client/java/extra_src/SortedSearch.java \
			client/java/extra_src/TypeError.java \
			client/java/extra_src/ValueError.java \
			client/java/hyperclient.i \
			client/java/proxies/hyperclient_attribute_check.i \
			client/java/proxies/hyperclient_attribute.i \
			client/java/proxies/HyperClient.i \
			client/java/proxies/hyperclient_map_attribute.i \
			client/python/hyperclient.pyx \
			hyperclient.pc

libhyperclient_includedir = $(includedir)
libhyperclient_include_HEADERS = \
			client/hyperclient.h

libhyperclient_la_SOURCES = \
			common/attribute.cc \
			common/attribute_check.cc \
			common/capture.cc \
			common/configuration.cc \
			common/float_encode.cc \
			common/funcall.cc \
			common/hash.cc \
			common/hyperdex.cc \
			common/hyperspace.cc \
			common/mapper.cc \
			common/network_msgtype.cc \
			common/range_searches.cc \
			common/schema.cc \
			common/serialization.cc \
			common/transfer.cc \
			datatypes/coercion.cc \
			datatypes/compare.cc \
			datatypes/float.cc \
			datatypes/int64.cc \
			datatypes/list.cc \
			datatypes/map.cc \
			datatypes/set.cc \
			datatypes/step.cc \
			datatypes/string.cc \
			datatypes/validate.cc \
			datatypes/write.cc \
			client/c_wrappers.cc \
			client/complete.cc \
			client/coordinator_link.cc \
			client/description.cc \
			client/hyperclient.cc \
			client/keyop_info.cc \
			client/parse_space_y.y \
			client/parse_space_l.l \
			client/parse_space_aux.cc \
			client/partition.cc \
			client/pending.cc \
			client/pending_count.cc \
			client/pending_get.cc \
			client/pending_group_del.cc \
			client/pending_search.cc \
			client/pending_search_description.cc \
			client/pending_sorted_search.cc \
			client/pending_statusonly.cc \
			client/refcount.cc \
			client/space_description.cc \
			client/util.cc
libhyperclient_la_LIBADD = \
			$(E_LIBS) \
			$(BUSYBEE_LIBS) \
			$(REPLICANT_LIBS) -lcityhash

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
			-fno-strict-aliasing
libhyperclient_java_la_LIBADD = \
			libhyperclient.la \
			$(PYTHON_LDFLAGS)

client/java/hyperclient_wrap.cxx: client/java/hyperclient.i client/hyperclient.h
	mkdir -p client/java/hyperclient
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
rubydir = $(RUBY_EXT_LIB)
ruby_LTLIBRARIES = client/ruby/hyperclient.la
endif

client_ruby_hyperclient_la_SOURCES = client/ruby/hyperclient.c client/ruby/type_conversion.cc
client_ruby_hyperclient_la_CPPFLAGS = -I$(RUBY_EXT_INC) -I$(RUBY_EXT_ARCHINC) $(RUBY_EXT_CPPFLAGS) $(CPPFLAGS)
client_ruby_hyperclient_la_LIBADD = libhyperclient.la $(RUBY_EXT_LDFLAGS)
client_ruby_hyperclient_la_LDFLAGS = -module -avoid-version

################################ C/C++ Bindings ################################

client_c_testcompile_SOURCES = client/c/testcompile.c
client_cc_testcompile_SOURCES = client/cc/testcompile.cc

################################################################################
##################################### Tools ####################################
################################################################################

hyperdex_SOURCES = hyperdex.cc
hyperdex_CPPFLAGS = -DHYPERDEX_EXEC_DIR=\""$(hyperdexexecdir)\"" $(CPPFLAGS)

hyperdex_initialize_cluster_SOURCES = tools/initialize-cluster.cc

hyperdex_initialize_cluster_CPPFLAGS = -DHYPERDEX_COORD_LIB=\""$(hyperdexexecdir)/libhypercoordinator\"" $(CPPFLAGS)
hyperdex_initialize_cluster_LDADD = libhyperclient.la

hyperdex_coordinator_SOURCES = tools/coordinator.cc
hyperdex_coordinator_LDADD = libhyperclient.la $(E_LIBS) $(BUSYBEE_LIBS)

hyperdex_add_space_SOURCES = tools/add-space.cc
hyperdex_add_space_LDADD = libhyperclient.la

hyperdex_rm_space_SOURCES = tools/rm-space.cc
hyperdex_rm_space_LDADD = libhyperclient.la

hyperdex_show_config_SOURCES = tools/show-config.cc
hyperdex_show_config_LDADD = libhyperclient.la

hyperdex_async_benchmark_SOURCES = tools/async-benchmark.cc
hyperdex_async_benchmark_LDADD = libhyperclient.la -lleveldb

hyperdex_benchmark_SOURCES = tools/benchmark.cc
hyperdex_benchmark_LDADD = libhyperclient.la -lleveldb

hyperdex_initiate_transfer_SOURCES = tools/initiate-transfer.cc
hyperdex_initiate_transfer_LDADD = libhyperclient.la

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
hyperdex_replication_stress_test_LDADD = libhyperclient.la -lpopt $(E_LIBS) 

hyperdex_search_stress_test_SOURCES = search-stress-test.cc test/common.cc
hyperdex_search_stress_test_LDADD = libhyperclient.la -lpopt $(E_LIBS)

hyperdex_simple_consistency_stress_test_SOURCES = simple-consistency-stress-test.cc
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
