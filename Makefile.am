## Copyright (c) 2012-2013, Cornell University
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

AM_CPPFLAGS  = -I${abs_top_srcdir}/include $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(HYPERLEVELDB_CFLAGS) $(REPLICANT_CFLAGS) $(SPARSEHASH_CFLAGS)
AM_CFLAGS    = -fvisibility=hidden $(WANAL_CFLAGS)
AM_CXXFLAGS  = -fvisibility=hidden -fvisibility-inlines-hidden $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(HYPERLEVELDB_CFLAGS) $(REPLICANT_CFLAGS) $(SPARSEHASH_CFLAGS) $(WANAL_CXXFLAGS)
AM_MAKEFLAGS = --no-print-directory
AM_YFLAGS = -d
HELP2MAN_FLAGS = --libtool --no-info --version-string=$(VERSION) --manual="HyperDex User Manual"

hyperdexexecdir = $(libexecdir)/$(PACKAGE)-$(VERSION)
hyperdexheaderdir = $(includedir)/hyperdex
pkgconfigdir = $(libdir)/pkgconfig

BUILT_SOURCES =

EXTRA_DIST =

CLEANFILES =

noinst_HEADERS =
hyperdexheader_HEADERS =

lib_LTLIBRARIES =

bin_PROGRAMS =
noinst_PROGRAMS =
check_PROGRAMS =

hyperdexexec_PROGRAMS =
hyperdexexec_LTLIBRARIES =

dist_man_MANS =

pkgconfig_DATA =

jardir = $(datadir)/java
jar_DATA =

pythonmoddir = $(pyexecdir)/hyperdex
pythonmod_DATA =
pythonmod_LTLIBRARIES =

rubydir = $(DESTDIR)$(RUBY_EXT_LIB)
ruby_LTLIBRARIES =

TESTS =

################################################################################
#################################### Common ####################################
################################################################################

%.h2m: %.md
	$(pandoc_verbose)pandoc -s -w man -o $@ $<
	$(sed_verbose)sed -i -e 's/^\.SH \(.*\)$$/[\1]/' -e 's/^.PP$$//' $@

gperf_verbose = $(gperf_verbose_$(V))
gperf_verbose_ = $(gperf_verbose_$(AM_DEFAULT_VERBOSITY))
gperf_verbose_0 = @echo "  GPERF " $@;
help2man_verbose = $(help2man_verbose_$(V))
help2man_verbose_ = $(help2man_verbose_$(AM_DEFAULT_VERBOSITY))
help2man_verbose_0 = @echo "  MAN   " $@;
pandoc_verbose = $(pandoc_verbose_$(V))
pandoc_verbose_ = $(pandoc_verbose_$(AM_DEFAULT_VERBOSITY))
pandoc_verbose_0 = @echo "  PANDOC" $@;
pyx_verbose = $(pyx_verbose_$(V))
pyx_verbose_ = $(pyx_verbose_$(AM_DEFAULT_VERBOSITY))
pyx_verbose_0 = @echo "  PYX   " $@;
sed_verbose = $(sed_verbose_$(V))
sed_verbose_ = $(sed_verbose_$(AM_DEFAULT_VERBOSITY))
sed_verbose_0 = @echo "  SED   " $@;
swig_verbose = $(swig_verbose_$(V))
swig_verbose_ = $(swig_verbose_$(AM_DEFAULT_VERBOSITY))
swig_verbose_0 = @echo "  SWIG  " $@;

th_sources = test/th_main.cc test/th.cc test/th.h

EXTRA_DIST += LICENSE
EXTRA_DIST += SIGNED-OFF-BY

include_HEADERS = include/hyperdex.h

noinst_HEADERS += include/hyperdex.h
noinst_HEADERS += namespace.h
noinst_HEADERS += visibility.h
noinst_HEADERS += common/attribute_check.h
noinst_HEADERS += common/attribute.h
noinst_HEADERS += common/configuration.h
noinst_HEADERS += common/configuration_flags.h
noinst_HEADERS += common/coordinator_link.h
noinst_HEADERS += common/coordinator_returncode.h
noinst_HEADERS += common/datatype_float.h
noinst_HEADERS += common/datatype_int64.h
noinst_HEADERS += common/datatype_list.h
noinst_HEADERS += common/datatype_map.h
noinst_HEADERS += common/datatype_set.h
noinst_HEADERS += common/datatypes.h
noinst_HEADERS += common/datatype_string.h
noinst_HEADERS += common/funcall.h
noinst_HEADERS += common/hash.h
noinst_HEADERS += common/hyperspace.h
noinst_HEADERS += common/ordered_encoding.h
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
noinst_HEADERS += common/server.h
noinst_HEADERS += common/transfer.h
noinst_HEADERS += tools/common.h

check_PROGRAMS += common/test/ordered_encoding
TESTS += common/test/ordered_encoding

common_test_ordered_encoding_SOURCES = common/test/ordered_encoding.cc common/ordered_encoding.cc $(th_sources)
common_test_ordered_encoding_CXXFLAGS = $(AM_CXXFLAGS) $(CXXFLAGS)

################################################################################
#################################### Daemon ####################################
################################################################################

if ENABLE_DAEMON
hyperdexexec_PROGRAMS += hyperdex-daemon
dist_man_MANS += man/hyperdex-daemon.1
endif

noinst_HEADERS += daemon/communication.h
noinst_HEADERS += daemon/coordinator_link_wrapper.h
noinst_HEADERS += daemon/daemon.h
noinst_HEADERS += daemon/datalayer_encodings.h
noinst_HEADERS += daemon/datalayer.h
noinst_HEADERS += daemon/datalayer_iterator.h
noinst_HEADERS += daemon/identifier_collector.h
noinst_HEADERS += daemon/identifier_generator.h
noinst_HEADERS += daemon/index_container.h
noinst_HEADERS += daemon/index_float.h
noinst_HEADERS += daemon/index_info.h
noinst_HEADERS += daemon/index_int64.h
noinst_HEADERS += daemon/index_list.h
noinst_HEADERS += daemon/index_map.h
noinst_HEADERS += daemon/index_primitive.h
noinst_HEADERS += daemon/index_set.h
noinst_HEADERS += daemon/index_string.h
noinst_HEADERS += daemon/leveldb.h
noinst_HEADERS += daemon/performance_counter.h
noinst_HEADERS += daemon/reconfigure_returncode.h
noinst_HEADERS += daemon/region_timestamp.h
noinst_HEADERS += daemon/replication_manager.h
noinst_HEADERS += daemon/replication_manager_key_region.h
noinst_HEADERS += daemon/replication_manager_key_state.h
noinst_HEADERS += daemon/replication_manager_pending.h
noinst_HEADERS += daemon/search_manager.h
noinst_HEADERS += daemon/state_hash_table.h
noinst_HEADERS += daemon/state_transfer_manager.h
noinst_HEADERS += daemon/state_transfer_manager_pending.h
noinst_HEADERS += daemon/state_transfer_manager_transfer_in_state.h
noinst_HEADERS += daemon/state_transfer_manager_transfer_out_state.h

EXTRA_DIST += man/hyperdex-daemon.1.md
EXTRA_DIST += man/hyperdex-daemon.1.h2m
hyperdex_daemon_SOURCES =
hyperdex_daemon_SOURCES += common/attribute.cc
hyperdex_daemon_SOURCES += common/attribute_check.cc
hyperdex_daemon_SOURCES += common/configuration.cc
hyperdex_daemon_SOURCES += common/coordinator_link.cc
hyperdex_daemon_SOURCES += common/coordinator_returncode.cc
hyperdex_daemon_SOURCES += common/datatype_float.cc
hyperdex_daemon_SOURCES += common/datatype_int64.cc
hyperdex_daemon_SOURCES += common/datatype_list.cc
hyperdex_daemon_SOURCES += common/datatype_map.cc
hyperdex_daemon_SOURCES += common/datatypes.cc
hyperdex_daemon_SOURCES += common/datatype_set.cc
hyperdex_daemon_SOURCES += common/datatype_string.cc
hyperdex_daemon_SOURCES += common/funcall.cc
hyperdex_daemon_SOURCES += common/hash.cc
hyperdex_daemon_SOURCES += common/hyperdex.cc
hyperdex_daemon_SOURCES += common/hyperspace.cc
hyperdex_daemon_SOURCES += common/ids.cc
hyperdex_daemon_SOURCES += common/mapper.cc
hyperdex_daemon_SOURCES += common/network_msgtype.cc
hyperdex_daemon_SOURCES += common/ordered_encoding.cc
hyperdex_daemon_SOURCES += common/range.cc
hyperdex_daemon_SOURCES += common/range_searches.cc
hyperdex_daemon_SOURCES += common/regex_match.cc
hyperdex_daemon_SOURCES += common/schema.cc
hyperdex_daemon_SOURCES += common/serialization.cc
hyperdex_daemon_SOURCES += common/server.cc
hyperdex_daemon_SOURCES += common/transfer.cc
hyperdex_daemon_SOURCES += daemon/communication.cc
hyperdex_daemon_SOURCES += daemon/coordinator_link_wrapper.cc
hyperdex_daemon_SOURCES += daemon/daemon.cc
hyperdex_daemon_SOURCES += daemon/datalayer.cc
hyperdex_daemon_SOURCES += daemon/datalayer_encodings.cc
hyperdex_daemon_SOURCES += daemon/datalayer_iterator.cc
hyperdex_daemon_SOURCES += daemon/identifier_collector.cc
hyperdex_daemon_SOURCES += daemon/identifier_generator.cc
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
hyperdex_daemon_SOURCES += daemon/replication_manager_key_region.cc
hyperdex_daemon_SOURCES += daemon/replication_manager_key_state.cc
hyperdex_daemon_SOURCES += daemon/replication_manager_pending.cc
hyperdex_daemon_SOURCES += daemon/search_manager.cc
hyperdex_daemon_SOURCES += daemon/state_transfer_manager.cc
hyperdex_daemon_SOURCES += daemon/state_transfer_manager_pending.cc
hyperdex_daemon_SOURCES += daemon/state_transfer_manager_transfer_in_state.cc
hyperdex_daemon_SOURCES += daemon/state_transfer_manager_transfer_out_state.cc
hyperdex_daemon_CXXFLAGS = $(AM_CXXFLAGS) $(CXXFLAGS)
hyperdex_daemon_LDADD =
hyperdex_daemon_LDADD += $(E_LIBS)
hyperdex_daemon_LDADD += $(BUSYBEE_LIBS)
hyperdex_daemon_LDADD += $(HYPERLEVELDB_LIBS)
hyperdex_daemon_LDADD += $(REPLICANT_LIBS)
hyperdex_daemon_LDADD += -lcityhash -lpopt -lglog -lpthread
man/hyperdex-daemon.1: man/hyperdex-daemon.1.h2m daemon/main.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-daemon$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-daemon$(EXEEXT)

check_PROGRAMS += daemon/test/identifier_collector
check_PROGRAMS += daemon/test/identifier_generator
TESTS += daemon/test/identifier_collector
TESTS += daemon/test/identifier_generator

daemon_test_identifier_collector_SOURCES = daemon/test/identifier_collector.cc daemon/identifier_collector.cc $(th_sources)
daemon_test_identifier_collector_CXXFLAGS = $(AM_CXXFLAGS) $(CXXFLAGS)

daemon_test_identifier_generator_SOURCES = daemon/test/identifier_generator.cc daemon/identifier_generator.cc $(th_sources)
daemon_test_identifier_generator_CXXFLAGS = $(AM_CXXFLAGS) $(CXXFLAGS)

################################################################################
################################## Coordinator #################################
################################################################################

if ENABLE_COORDINATOR
hyperdexexec_LTLIBRARIES += libhyperdex-coordinator.la
hyperdexexec_PROGRAMS += hyperdex-coordinator
dist_man_MANS += man/hyperdex-coordinator.1
endif

noinst_HEADERS += coordinator/coordinator.h
noinst_HEADERS += coordinator/offline_server.h
noinst_HEADERS += coordinator/region_intent.h
noinst_HEADERS += coordinator/replica_sets.h
noinst_HEADERS += coordinator/server_barrier.h
noinst_HEADERS += coordinator/transitions.h
noinst_HEADERS += coordinator/util.h

libhyperdex_coordinator_la_SOURCES =
libhyperdex_coordinator_la_SOURCES += common/attribute.cc
libhyperdex_coordinator_la_SOURCES += common/hyperspace.cc
libhyperdex_coordinator_la_SOURCES += common/ids.cc
libhyperdex_coordinator_la_SOURCES += common/schema.cc
libhyperdex_coordinator_la_SOURCES += common/serialization.cc
libhyperdex_coordinator_la_SOURCES += common/server.cc
libhyperdex_coordinator_la_SOURCES += common/transfer.cc
libhyperdex_coordinator_la_SOURCES += coordinator/coordinator.cc
libhyperdex_coordinator_la_SOURCES += coordinator/replica_sets.cc
libhyperdex_coordinator_la_SOURCES += coordinator/server_barrier.cc
libhyperdex_coordinator_la_SOURCES += coordinator/symtable.c
libhyperdex_coordinator_la_SOURCES += coordinator/transitions.cc
# libhyperdex_coordinator_la_LIBADD =
# libhyperdex_coordinator_la_LIBADD += $(E_LIBS)

EXTRA_DIST += man/hyperdex-coordinator.1.md
EXTRA_DIST += man/hyperdex-coordinator.1.h2m
hyperdex_coordinator_SOURCES = tools/coordinator.cc
hyperdex_coordinator_CPPFLAGS = -DHYPERDEX_EXEC_DIR=\""$(hyperdexexecdir)\"" $(AM_CPPFLAGS) $(CPPFLAGS)
hyperdex_coordinator_LDADD =
hyperdex_coordinator_LDADD += $(E_LIBS)
hyperdex_coordinator_LDADD += $(BUSYBEE_LIBS)
hyperdex_coordinator_LDADD += -lpopt
man/hyperdex-coordinator.1: man/hyperdex-coordinator.1.h2m tools/coordinator.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-coordinator$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-coordinator$(EXEEXT)

################################################################################
#################################### Client ####################################
################################################################################

if ENABLE_CLIENT
lib_LTLIBRARIES += libhyperdex-client.la
BUILT_SOURCES += client/keyop_info.cc
hyperdexheader_HEADERS += include/hyperdex/client.h
hyperdexheader_HEADERS += include/hyperdex/client.hpp
hyperdexheader_HEADERS += include/hyperdex/datastructures.h
pkgconfig_DATA += hyperdex-client.pc
endif

EXTRA_DIST += client/keyop_info.gperf

noinst_HEADERS += include/hyperdex/client.h
noinst_HEADERS += include/hyperdex/client.hpp
noinst_HEADERS += include/hyperdex/datastructures.h
noinst_HEADERS += client/client.h
noinst_HEADERS += client/constants.h
noinst_HEADERS += client/keyop_info.h
noinst_HEADERS += client/pending_aggregation.h
noinst_HEADERS += client/pending_atomic.h
noinst_HEADERS += client/pending_count.h
noinst_HEADERS += client/pending_get.h
noinst_HEADERS += client/pending_group_del.h
noinst_HEADERS += client/pending.h
noinst_HEADERS += client/pending_search_describe.h
noinst_HEADERS += client/pending_search.h
noinst_HEADERS += client/pending_sorted_search.h
noinst_HEADERS += client/util.h

libhyperdex_client_la_SOURCES =
libhyperdex_client_la_SOURCES += common/attribute.cc
libhyperdex_client_la_SOURCES += common/attribute_check.cc
libhyperdex_client_la_SOURCES += common/configuration.cc
libhyperdex_client_la_SOURCES += common/coordinator_link.cc
libhyperdex_client_la_SOURCES += common/datatype_float.cc
libhyperdex_client_la_SOURCES += common/datatype_int64.cc
libhyperdex_client_la_SOURCES += common/datatype_list.cc
libhyperdex_client_la_SOURCES += common/datatype_map.cc
libhyperdex_client_la_SOURCES += common/datatypes.cc
libhyperdex_client_la_SOURCES += common/datatype_set.cc
libhyperdex_client_la_SOURCES += common/datatype_string.cc
libhyperdex_client_la_SOURCES += common/funcall.cc
libhyperdex_client_la_SOURCES += common/hash.cc
libhyperdex_client_la_SOURCES += common/hyperdex.cc
libhyperdex_client_la_SOURCES += common/hyperspace.cc
libhyperdex_client_la_SOURCES += common/ids.cc
libhyperdex_client_la_SOURCES += common/mapper.cc
libhyperdex_client_la_SOURCES += common/network_msgtype.cc
libhyperdex_client_la_SOURCES += common/ordered_encoding.cc
libhyperdex_client_la_SOURCES += common/range.cc
libhyperdex_client_la_SOURCES += common/range_searches.cc
libhyperdex_client_la_SOURCES += common/regex_match.cc
libhyperdex_client_la_SOURCES += common/schema.cc
libhyperdex_client_la_SOURCES += common/server.cc
libhyperdex_client_la_SOURCES += common/serialization.cc
libhyperdex_client_la_SOURCES += common/transfer.cc
libhyperdex_client_la_SOURCES += client/c.cc
libhyperdex_client_la_SOURCES += client/client.cc
libhyperdex_client_la_SOURCES += client/datastructures.cc
libhyperdex_client_la_SOURCES += client/keyop_info.cc
libhyperdex_client_la_SOURCES += client/pending_aggregation.cc
libhyperdex_client_la_SOURCES += client/pending_atomic.cc
libhyperdex_client_la_SOURCES += client/pending.cc
libhyperdex_client_la_SOURCES += client/pending_count.cc
libhyperdex_client_la_SOURCES += client/pending_get.cc
libhyperdex_client_la_SOURCES += client/pending_group_del.cc
libhyperdex_client_la_SOURCES += client/pending_search.cc
libhyperdex_client_la_SOURCES += client/pending_search_describe.cc
libhyperdex_client_la_SOURCES += client/pending_sorted_search.cc
libhyperdex_client_la_SOURCES += client/util.cc
libhyperdex_client_la_LIBADD =
libhyperdex_client_la_LIBADD += $(E_LIBS)
libhyperdex_client_la_LIBADD += $(BUSYBEE_LIBS)
libhyperdex_client_la_LIBADD += $(REPLICANT_LIBS)
libhyperdex_client_la_LIBADD += -lcityhash

client/keyop_info.cc: client/keyop_info.gperf client/keyop_info.h
	$(gperf_verbose)gperf -m 100 $(abs_top_srcdir)/client/keyop_info.gperf --output-file=$(abs_top_builddir)/client/keyop_info.cc

check_PROGRAMS += client/test/datastructures
TESTS += client/test/datastructures

client_test_datastructures_SOURCES = client/test/datastructures.cc $(th_sources)
client_test_datastructures_LDADD = libhyperdex-client.la

################################################################################
##################################### Admin ####################################
################################################################################

if ENABLE_ADMIN
lib_LTLIBRARIES += libhyperdex-admin.la
BUILT_SOURCES += admin/parse_space_y.h
hyperdexheader_HEADERS += include/hyperdex/admin.h
hyperdexheader_HEADERS += include/hyperdex/admin.hpp
hyperdexheader_HEADERS += include/hyperdex/hyperspace_builder.h
pkgconfig_DATA += hyperdex-admin.pc
endif

noinst_HEADERS += include/hyperdex/admin.h
noinst_HEADERS += include/hyperdex/admin.hpp
noinst_HEADERS += include/hyperdex/hyperspace_builder.h
noinst_HEADERS += admin/admin.h
noinst_HEADERS += admin/constants.h
noinst_HEADERS += admin/coord_rpc.h
noinst_HEADERS += admin/coord_rpc_generic.h
noinst_HEADERS += admin/hyperspace_builder_internal.h
noinst_HEADERS += admin/partition.h
noinst_HEADERS += admin/pending.h
noinst_HEADERS += admin/pending_perf_counters.h
noinst_HEADERS += admin/pending_string.h
noinst_HEADERS += admin/yieldable.h

libhyperdex_admin_la_SOURCES =
libhyperdex_admin_la_SOURCES += common/attribute.cc
libhyperdex_admin_la_SOURCES += common/configuration.cc
libhyperdex_admin_la_SOURCES += common/coordinator_link.cc
libhyperdex_admin_la_SOURCES += common/datatype_float.cc
libhyperdex_admin_la_SOURCES += common/datatype_int64.cc
libhyperdex_admin_la_SOURCES += common/datatype_list.cc
libhyperdex_admin_la_SOURCES += common/datatype_map.cc
libhyperdex_admin_la_SOURCES += common/datatypes.cc
libhyperdex_admin_la_SOURCES += common/datatype_set.cc
libhyperdex_admin_la_SOURCES += common/datatype_string.cc
libhyperdex_admin_la_SOURCES += common/hash.cc
libhyperdex_admin_la_SOURCES += common/hyperdex.cc
libhyperdex_admin_la_SOURCES += common/hyperspace.cc
libhyperdex_admin_la_SOURCES += common/ids.cc
libhyperdex_admin_la_SOURCES += common/mapper.cc
libhyperdex_admin_la_SOURCES += common/ordered_encoding.cc
libhyperdex_admin_la_SOURCES += common/range.cc
libhyperdex_admin_la_SOURCES += common/range_searches.cc
libhyperdex_admin_la_SOURCES += common/regex_match.cc
libhyperdex_admin_la_SOURCES += common/schema.cc
libhyperdex_admin_la_SOURCES += common/serialization.cc
libhyperdex_admin_la_SOURCES += common/server.cc
libhyperdex_admin_la_SOURCES += common/transfer.cc
libhyperdex_admin_la_SOURCES += admin/admin.cc
libhyperdex_admin_la_SOURCES += admin/c.cc
libhyperdex_admin_la_SOURCES += admin/coord_rpc.cc
libhyperdex_admin_la_SOURCES += admin/coord_rpc_generic.cc
libhyperdex_admin_la_SOURCES += admin/hyperspace_builder.cc
libhyperdex_admin_la_SOURCES += admin/parse_space_l.l
libhyperdex_admin_la_SOURCES += admin/parse_space_y.y
libhyperdex_admin_la_SOURCES += admin/partition.cc
libhyperdex_admin_la_SOURCES += admin/pending.cc
libhyperdex_admin_la_SOURCES += admin/pending_perf_counters.cc
libhyperdex_admin_la_SOURCES += admin/pending_string.cc
libhyperdex_admin_la_SOURCES += admin/raw_backup.cc
libhyperdex_admin_la_SOURCES += admin/yieldable.cc
libhyperdex_admin_la_LIBADD =
libhyperdex_admin_la_LIBADD += $(E_LIBS)
libhyperdex_admin_la_LIBADD += $(BUSYBEE_LIBS)
libhyperdex_admin_la_LIBADD += $(REPLICANT_LIBS)
libhyperdex_admin_la_LIBADD += -lcityhash

################################################################################
################################### Bindings ###################################
################################################################################

####################################### C ######################################

if ENABLE_CLIENT
noinst_PROGRAMS += bindings/c/client_testcompile
endif
if ENABLE_ADMIN
noinst_PROGRAMS += bindings/c/admin_testcompile
endif

EXTRA_DIST += bindings/c/gen_client_header.py
EXTRA_DIST += bindings/c/gen_doc.py
EXTRA_DIST += bindings/c/gen_wrappers.py

bindings_c_client_testcompile_SOURCES = bindings/c/client-testcompile.c
bindings_c_client_testcompile_LDADD = libhyperdex-client.la

bindings_c_admin_testcompile_SOURCES = bindings/c/admin-testcompile.c
bindings_c_admin_testcompile_LDADD = libhyperdex-admin.la

###################################### C++ #####################################

if ENABLE_CLIENT
noinst_PROGRAMS += bindings/cc/client_testcompile
endif
if ENABLE_ADMIN
noinst_PROGRAMS += bindings/cc/admin_testcompile
endif

bindings_cc_client_testcompile_SOURCES = bindings/cc/client-testcompile.cc
bindings_cc_client_testcompile_LDADD = libhyperdex-client.la

bindings_cc_admin_testcompile_SOURCES = bindings/cc/admin-testcompile.cc
bindings_cc_admin_testcompile_LDADD = libhyperdex-admin.la

###################################### Go ######################################

EXTRA_DIST += bindings/go/api.go
EXTRA_DIST += bindings/go/hypergo.go
EXTRA_DIST += bindings/go/hypergo_test.go
EXTRA_DIST += bindings/go/ops.go
EXTRA_DIST += bindings/go/util.go

##################################### Java #####################################

hyperdex_client_jarfile = bindings/java/org.hyperdex.client-$(VERSION).jar
hyperdex_ycsb_jarfile = bindings/java/org.hyperdex.ycsb-$(VERSION).jar

if ENABLE_JAVA_BINDINGS
if ENABLE_CLIENT
lib_LTLIBRARIES += libhyperdex-client-java.la
jar_DATA += $(hyperdex_client_jarfile)
endif

if ENABLE_YCSB
jar_DATA += $(hyperdex_ycsb_jarfile)
endif
endif

CLEANFILES += $(hyperdex_client_jarfile)
CLEANFILES += $(hyperdex_ycsb_jarfile)

EXTRA_DIST += bindings/java/examples/HyperBinaryTest.java
EXTRA_DIST += bindings/java/examples/HyperSearchFloatRange.java
EXTRA_DIST += bindings/java/examples/HyperTest.java
EXTRA_DIST += bindings/java/examples/README.txt
EXTRA_DIST += bindings/java/examples/source_me
EXTRA_DIST += bindings/java/gen_code/MapOp.template.java
EXTRA_DIST += bindings/java/gen_code/SimpleOp.template.java
EXTRA_DIST += bindings/java/gen_code/gen_op_files
EXTRA_DIST += bindings/java/gen_code/op_list.txt
EXTRA_DIST += bindings/java/hyperdex_client.i
EXTRA_DIST += bindings/java/org/hyperdex/client/AttributeError.java
EXTRA_DIST += bindings/java/org/hyperdex/client/ByteArray.java
EXTRA_DIST += bindings/java/org/hyperdex/client/ByteArrayKeyedMap.java
EXTRA_DIST += bindings/java/org/hyperdex/client/ByteArrayKeyedSortedMap.java
EXTRA_DIST += bindings/java/org/hyperdex/client/ByteArraySortedSet.java
EXTRA_DIST += bindings/java/org/hyperdex/client/ByteArrayVector.java
EXTRA_DIST += bindings/java/org/hyperdex/client/Deferred.java
EXTRA_DIST += bindings/java/org/hyperdex/client/DeferredCondPut.java
EXTRA_DIST += bindings/java/org/hyperdex/client/DeferredCount.java
EXTRA_DIST += bindings/java/org/hyperdex/client/DeferredDelete.java
EXTRA_DIST += bindings/java/org/hyperdex/client/DeferredFromAttrs.java
EXTRA_DIST += bindings/java/org/hyperdex/client/DeferredGet.java
EXTRA_DIST += bindings/java/org/hyperdex/client/DeferredGroupDel.java
EXTRA_DIST += bindings/java/org/hyperdex/client/DeferredMapOp.java
EXTRA_DIST += bindings/java/org/hyperdex/client/GreaterEqual.java
EXTRA_DIST += bindings/java/org/hyperdex/client/HyperDexClientException.java
EXTRA_DIST += bindings/java/org/hyperdex/client/LessEqual.java
EXTRA_DIST += bindings/java/org/hyperdex/client/MapOp.java
EXTRA_DIST += bindings/java/org/hyperdex/client/MemoryError.java
EXTRA_DIST += bindings/java/org/hyperdex/client/Pending.java
EXTRA_DIST += bindings/java/org/hyperdex/client/Predicate.java
EXTRA_DIST += bindings/java/org/hyperdex/client/Range.java
EXTRA_DIST += bindings/java/org/hyperdex/client/Search.java
EXTRA_DIST += bindings/java/org/hyperdex/client/SearchBase.java
EXTRA_DIST += bindings/java/org/hyperdex/client/SimpleOp.java
EXTRA_DIST += bindings/java/org/hyperdex/client/SortedSearch.java
EXTRA_DIST += bindings/java/org/hyperdex/client/TypeError.java
EXTRA_DIST += bindings/java/org/hyperdex/client/ValueError.java
EXTRA_DIST += bindings/java/org/hyperdex/ycsb/HyperDex.java
EXTRA_DIST += bindings/java/proxies/Client.i
EXTRA_DIST += bindings/java/proxies/hyperdex_client_attribute.i
EXTRA_DIST += bindings/java/proxies/hyperdex_client_attribute_check.i
EXTRA_DIST += bindings/java/proxies/hyperdex_client_map_attribute.i

# Client
nodist_libhyperdex_client_java_la_SOURCES = bindings/java/hyperdex_client_wrap.cxx
libhyperdex_client_java_la_CPPFLAGS = $(JNI_CPPFLAGS) $(AM_CPPFLAGS) $(CPPFLAGS)
libhyperdex_client_java_la_CXXFLAGS = -fno-strict-aliasing $(AM_CXXFLAGS) -fvisibility=default $(CXXFLAGS)
libhyperdex_client_java_la_LIBADD = libhyperdex-client.la
bindings/java/hyperdex_client_wrap.cxx: bindings/java/hyperdex_client.i include/hyperdex/client.h
	$(swig_verbose)mkdir -p $(abs_top_builddir)/bindings/java/org/hyperdex/client &&  \
	${SWIG} -I${abs_srcdir} -java -package org.hyperdex.client -outdir $(abs_builddir)/bindings/java/org/hyperdex/client \
		-o $(abs_builddir)/$@ -w518 $(abs_top_srcdir)/$<

$(hyperdex_client_jarfile): bindings/java/hyperdex_client_wrap.cxx
	javac bindings/java/org/hyperdex/client/*.java
	cd bindings/java ; $(JAR) cvf $(JARFLAGS) ../../$(hyperdex_client_jarfile) org/hyperdex/client/*.class

# YCSB
$(hyperdex_ycsb_jarfile): bindings/java/org/hyperdex/ycsb/HyperDex.java
	javac -cp $(abs_top_builddir)/$(hyperdex_client_jarfile):$(CLASSPATH) bindings/java/org/hyperdex/ycsb/*.java
	cd bindings/java ; $(JAR) cvf $(JARFLAGS) ../../$(hyperdex_ycsb_jarfile) org/hyperdex/ycsb/*.class

#################################### Python ####################################

if ENABLE_PYTHON_BINDINGS
pythonmod_DATA += bindings/python/hyperdex/__init__.py
pythonmod_DATA += bindings/python/hyperdex/admin.py
pythonmod_DATA += bindings/python/hyperdex/client.py
if ENABLE_CLIENT
pythonmod_LTLIBRARIES += bindings/python/_hyperdex_client.la
endif
if ENABLE_ADMIN
pythonmod_LTLIBRARIES += bindings/python/_hyperdex_admin.la
endif
endif

EXTRA_DIST += bindings/python/client.pyx
EXTRA_DIST += bindings/python/hyperdex/admin.py
EXTRA_DIST += bindings/python/hyperdex/client.py
EXTRA_DIST += bindings/python/hyperdex/__init__.py
bindings_python__hyperdex_client_la_SOURCES = bindings/python/_hyperdex_client.c
bindings_python__hyperdex_client_la_CPPFLAGS =
bindings_python__hyperdex_client_la_CPPFLAGS += $(PYTHON_CPPFLAGS)
bindings_python__hyperdex_client_la_CPPFLAGS += $(AM_CPPFLAGS)
bindings_python__hyperdex_client_la_CPPFLAGS += $(CPPFLAGS)
bindings_python__hyperdex_client_la_CFLAGS =
bindings_python__hyperdex_client_la_CFLAGS += $(AM_CFLAGS)
bindings_python__hyperdex_client_la_CFLAGS += -fvisibility=default
bindings_python__hyperdex_client_la_CFLAGS += $(CFLAGS)
bindings_python__hyperdex_client_la_LIBADD =
bindings_python__hyperdex_client_la_LIBADD += libhyperdex-client.la
bindings_python__hyperdex_client_la_LIBADD += $(PYTHON_LDFLAGS)
bindings_python__hyperdex_client_la_LDFLAGS = -module -avoid-version -export-symbols-regex init_hyperdex_client $(AM_LDFLAGS) $(LDFLAGS)
bindings/python/_hyperdex_client.c: bindings/python/client.pyx
	cp bindings/python/client.pyx bindings/python/_hyperdex_client.pyx
	$(pyx_verbose)cython bindings/python/_hyperdex_client.pyx
	rm bindings/python/_hyperdex_client.pyx

EXTRA_DIST += bindings/python/admin.pyx
bindings_python__hyperdex_admin_la_SOURCES = bindings/python/_hyperdex_admin.c
bindings_python__hyperdex_admin_la_CPPFLAGS =
bindings_python__hyperdex_admin_la_CPPFLAGS += $(PYTHON_CPPFLAGS)
bindings_python__hyperdex_admin_la_CPPFLAGS += $(AM_CPPFLAGS)
bindings_python__hyperdex_admin_la_CPPFLAGS += $(CPPFLAGS)
bindings_python__hyperdex_admin_la_CFLAGS =
bindings_python__hyperdex_admin_la_CFLAGS += $(AM_CFLAGS)
bindings_python__hyperdex_admin_la_CFLAGS += -fvisibility=default
bindings_python__hyperdex_admin_la_CFLAGS += $(CFLAGS)
bindings_python__hyperdex_admin_la_LIBADD =
bindings_python__hyperdex_admin_la_LIBADD += libhyperdex-admin.la
bindings_python__hyperdex_admin_la_LIBADD += $(PYTHON_LDFLAGS)
bindings_python__hyperdex_admin_la_LDFLAGS = -module -avoid-version -export-symbols-regex init_hyperdex_admin $(AM_LDFLAGS) $(LDFLAGS)
bindings/python/_hyperdex_admin.c: bindings/python/admin.pyx
	cp bindings/python/admin.pyx bindings/python/_hyperdex_admin.pyx
	$(pyx_verbose)cython bindings/python/_hyperdex_admin.pyx
	rm bindings/python/_hyperdex_admin.pyx

##################################### Ruby #####################################

if ENABLE_RUBY_BINDINGS
ruby_LTLIBRARIES += bindings/ruby/hyperdex.la
endif

EXTRA_DIST += bindings/ruby/gen_ruby.py
EXTRA_DIST += bindings/ruby/definitions.c
EXTRA_DIST += bindings/ruby/prototypes.c

bindings_ruby_hyperdex_la_SOURCES = bindings/ruby/hyperdex.c
bindings_ruby_hyperdex_la_CPPFLAGS =
bindings_ruby_hyperdex_la_CPPFLAGS += -I$(RUBY_EXT_INC)
bindings_ruby_hyperdex_la_CPPFLAGS += -I$(RUBY_EXT_ARCHINC)
bindings_ruby_hyperdex_la_CPPFLAGS += -I$(RUBY_EXT_CPPFLAGS)
bindings_ruby_hyperdex_la_CPPFLAGS += $(AM_CPPFLAGS)
bindings_ruby_hyperdex_la_CPPFLAGS += $(CPPFLAGS)
bindings_ruby_hyperdex_la_CFLAGS =
bindings_ruby_hyperdex_la_CFLAGS += $(AM_CFLAGS)
bindings_ruby_hyperdex_la_CFLAGS += $(CFLAGS)
bindings_ruby_hyperdex_la_LDFLAGS = -module -avoid-version $(AM_LDFLAGS) $(LDFLAGS)
bindings_ruby_hyperdex_la_LIBADD = $(RUBY_EXT_LDFLAGS)

if ENABLE_CLIENT
bindings_ruby_hyperdex_la_SOURCES += bindings/ruby/client.c
bindings_ruby_hyperdex_la_CFLAGS += -DHYPERDEX_CLIENT
bindings_ruby_hyperdex_la_LIBADD += libhyperdex-client.la
endif

if ENABLE_ADMIN
bindings_ruby_hyperdex_la_SOURCES += bindings/ruby/admin.c
bindings_ruby_hyperdex_la_CFLAGS += -DHYPERDEX_ADMIN
bindings_ruby_hyperdex_la_LIBADD += libhyperdex-admin.la
endif

################################################################################
##################################### Tests ####################################
################################################################################

check_PROGRAMS += test/replication-stress-test
check_PROGRAMS += test/search-stress-test
shellwrappers =
shellwrappers += test/sh/doc.quick-start.sh
shellwrappers += test/sh/doc.data-types.sh
shellwrappers += test/sh/doc.async-ops.sh
shellwrappers += test/sh/doc.atomic-ops.sh
shellwrappers += test/sh/replication.simple.daemons=1.fault-tolerance=0.sh
shellwrappers += test/sh/replication.simple.daemons=8.fault-tolerance=0.sh
shellwrappers += test/sh/replication.simple.daemons=8.fault-tolerance=1.sh
shellwrappers += test/sh/replication.simple.daemons=8.fault-tolerance=2.sh
shellwrappers += test/sh/replication.reverse.daemons=1.fault-tolerance=0.sh
shellwrappers += test/sh/replication.reverse.daemons=8.fault-tolerance=0.sh
shellwrappers += test/sh/replication.reverse.daemons=8.fault-tolerance=1.sh
shellwrappers += test/sh/replication.reverse.daemons=8.fault-tolerance=2.sh
shellwrappers += test/sh/replication.composite.daemons=1.fault-tolerance=0.sh
shellwrappers += test/sh/replication.composite.daemons=8.fault-tolerance=0.sh
shellwrappers += test/sh/replication.composite.daemons=8.fault-tolerance=1.sh
shellwrappers += test/sh/replication.composite.daemons=8.fault-tolerance=2.sh
shellwrappers += test/sh/search.simple.keytype=int,daemons=1.fault-tolerance=0.sh
shellwrappers += test/sh/search.simple.keytype=int,daemons=8.fault-tolerance=0.sh
shellwrappers += test/sh/search.simple.keytype=int,daemons=8.fault-tolerance=1.sh
shellwrappers += test/sh/search.simple.keytype=string,daemons=1.fault-tolerance=0.sh
shellwrappers += test/sh/search.simple.keytype=string,daemons=8.fault-tolerance=0.sh
shellwrappers += test/sh/search.simple.keytype=string,daemons=8.fault-tolerance=1.sh
shellwrappers += test/sh/search.index.keytype=int,daemons=1.fault-tolerance=0.sh
shellwrappers += test/sh/search.index.keytype=int,daemons=8.fault-tolerance=0.sh
shellwrappers += test/sh/search.index.keytype=int,daemons=8.fault-tolerance=1.sh
shellwrappers += test/sh/search.index.keytype=string,daemons=1.fault-tolerance=0.sh
shellwrappers += test/sh/search.index.keytype=string,daemons=8.fault-tolerance=0.sh
shellwrappers += test/sh/search.index.keytype=string,daemons=8.fault-tolerance=1.sh
shellwrappers += test/sh/search.subspace.keytype=int,daemons=1.fault-tolerance=0.sh
shellwrappers += test/sh/search.subspace.keytype=int,daemons=8.fault-tolerance=0.sh
shellwrappers += test/sh/search.subspace.keytype=int,daemons=8.fault-tolerance=1.sh
shellwrappers += test/sh/search.subspace.keytype=string,daemons=1.fault-tolerance=0.sh
shellwrappers += test/sh/search.subspace.keytype=string,daemons=8.fault-tolerance=0.sh
shellwrappers += test/sh/search.subspace.keytype=string,daemons=8.fault-tolerance=1.sh
shellwrappers += test/sh/search.combination.keytype=int,daemons=1.fault-tolerance=0.sh
shellwrappers += test/sh/search.combination.keytype=int,daemons=8.fault-tolerance=0.sh
shellwrappers += test/sh/search.combination.keytype=int,daemons=8.fault-tolerance=1.sh
shellwrappers += test/sh/search.combination.keytype=string,daemons=1.fault-tolerance=0.sh
shellwrappers += test/sh/search.combination.keytype=string,daemons=8.fault-tolerance=0.sh
shellwrappers += test/sh/search.combination.keytype=string,daemons=8.fault-tolerance=1.sh
TESTS += $(shellwrappers)
EXTRA_DIST += $(shellwrappers)

test_replication_stress_test_SOURCES = test/replication-stress-test.cc
test_replication_stress_test_LDADD = libhyperdex-client.la

test_search_stress_test_SOURCES = test/search-stress-test.cc
test_search_stress_test_LDADD = libhyperdex-client.la

################################################################################
##################################### Tools ####################################
################################################################################

if ENABLE_TOOLS
bin_PROGRAMS += hyperdex-add-space
bin_PROGRAMS += hyperdex-rm-space
bin_PROGRAMS += hyperdex-list-spaces
bin_PROGRAMS += hyperdex-validate-space
bin_PROGRAMS += hyperdex-show-config
bin_PROGRAMS += hyperdex-server-register
bin_PROGRAMS += hyperdex-server-online
bin_PROGRAMS += hyperdex-server-offline
bin_PROGRAMS += hyperdex-server-kill
bin_PROGRAMS += hyperdex-server-forget
bin_PROGRAMS += hyperdex-perf-counters
bin_PROGRAMS += hyperdex-set-read-only
bin_PROGRAMS += hyperdex-set-read-write
bin_PROGRAMS += hyperdex-raw-backup
dist_man_MANS += man/hyperdex-add-space.1
dist_man_MANS += man/hyperdex-rm-space.1
dist_man_MANS += man/hyperdex-list-spaces.1
dist_man_MANS += man/hyperdex-validate-space.1
dist_man_MANS += man/hyperdex-show-config.1
dist_man_MANS += man/hyperdex-server-register.1
dist_man_MANS += man/hyperdex-server-online.1
dist_man_MANS += man/hyperdex-server-offline.1
dist_man_MANS += man/hyperdex-server-kill.1
dist_man_MANS += man/hyperdex-server-forget.1
dist_man_MANS += man/hyperdex-perf-counters.1
dist_man_MANS += man/hyperdex-set-read-only.1
dist_man_MANS += man/hyperdex-set-read-write.1
dist_man_MANS += man/hyperdex-raw-backup.1
endif

# hyperdex
bin_PROGRAMS += hyperdex
dist_man_MANS += man/hyperdex.1
EXTRA_DIST += man/hyperdex.1.md
EXTRA_DIST += man/hyperdex.1.h2m
hyperdex_SOURCES = hyperdex.cc
hyperdex_CPPFLAGS = -DHYPERDEX_EXEC_DIR=\""$(hyperdexexecdir)\"" $(AM_CPPFLAGS) $(CPPFLAGS)
hyperdex_LDADD = -lpopt
man/hyperdex.1: man/hyperdex.1.h2m hyperdex.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex$(EXEEXT)

# hyperdex-add-space
EXTRA_DIST += man/hyperdex-add-space.1.md
EXTRA_DIST += man/hyperdex-add-space.1.h2m
hyperdex_add_space_SOURCES = tools/add-space.cc
hyperdex_add_space_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-add-space.1: man/hyperdex-add-space.1.h2m tools/add-space.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-add-space$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-add-space$(EXEEXT)

# hyperdex-rm-space
EXTRA_DIST += man/hyperdex-rm-space.1.md
EXTRA_DIST += man/hyperdex-rm-space.1.h2m
hyperdex_rm_space_SOURCES = tools/rm-space.cc
hyperdex_rm_space_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-rm-space.1: man/hyperdex-rm-space.1.h2m tools/rm-space.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-rm-space$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-rm-space$(EXEEXT)

# hyperdex-list-spaces
EXTRA_DIST += man/hyperdex-list-spaces.1.md
EXTRA_DIST += man/hyperdex-list-spaces.1.h2m
hyperdex_list_spaces_SOURCES = tools/list-spaces.cc
hyperdex_list_spaces_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-list-spaces.1: man/hyperdex-list-spaces.1.h2m tools/list-spaces.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-list-spaces$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-list-spaces$(EXEEXT)

# hyperdex-validate-space
EXTRA_DIST += man/hyperdex-validate-space.1.md
EXTRA_DIST += man/hyperdex-validate-space.1.h2m
hyperdex_validate_space_SOURCES = tools/validate-space.cc
hyperdex_validate_space_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-validate-space.1: man/hyperdex-validate-space.1.h2m tools/validate-space.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-validate-space$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-validate-space$(EXEEXT)

# hyperdex-show-config
EXTRA_DIST += man/hyperdex-show-config.1.md
EXTRA_DIST += man/hyperdex-show-config.1.h2m
hyperdex_show_config_SOURCES = tools/show-config.cc
hyperdex_show_config_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-show-config.1: man/hyperdex-show-config.1.h2m tools/show-config.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-show-config$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-show-config$(EXEEXT)

# hyperdex-server-register
EXTRA_DIST += man/hyperdex-server-register.1.md
EXTRA_DIST += man/hyperdex-server-register.1.h2m
hyperdex_server_register_SOURCES = tools/server-register.cc
hyperdex_server_register_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-server-register.1: man/hyperdex-server-register.1.h2m tools/server-register.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-server-register$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-server-register$(EXEEXT)

# hyperdex-server-online
EXTRA_DIST += man/hyperdex-server-online.1.md
EXTRA_DIST += man/hyperdex-server-online.1.h2m
hyperdex_server_online_SOURCES = tools/server-online.cc
hyperdex_server_online_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-server-online.1: man/hyperdex-server-online.1.h2m tools/server-online.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-server-online$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-server-online$(EXEEXT)

# hyperdex-server-offline
EXTRA_DIST += man/hyperdex-server-offline.1.md
EXTRA_DIST += man/hyperdex-server-offline.1.h2m
hyperdex_server_offline_SOURCES = tools/server-offline.cc
hyperdex_server_offline_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-server-offline.1: man/hyperdex-server-offline.1.h2m tools/server-offline.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-server-offline$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-server-offline$(EXEEXT)

# hyperdex-server-forget
EXTRA_DIST += man/hyperdex-server-forget.1.md
EXTRA_DIST += man/hyperdex-server-forget.1.h2m
hyperdex_server_forget_SOURCES = tools/server-forget.cc
hyperdex_server_forget_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-server-forget.1: man/hyperdex-server-forget.1.h2m tools/server-forget.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-server-forget$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-server-forget$(EXEEXT)

# hyperdex-server-kill
EXTRA_DIST += man/hyperdex-server-kill.1.md
EXTRA_DIST += man/hyperdex-server-kill.1.h2m
hyperdex_server_kill_SOURCES = tools/server-kill.cc
hyperdex_server_kill_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-server-kill.1: man/hyperdex-server-kill.1.h2m tools/server-kill.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-server-kill$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-server-kill$(EXEEXT)

# hyperdex-perf-counters
EXTRA_DIST += man/hyperdex-perf-counters.1.md
EXTRA_DIST += man/hyperdex-perf-counters.1.h2m
hyperdex_perf_counters_SOURCES = tools/perf-counters.cc
hyperdex_perf_counters_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-perf-counters.1: man/hyperdex-perf-counters.1.h2m tools/perf-counters.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-perf-counters$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-perf-counters$(EXEEXT)

# hyperdex-set-read-only
EXTRA_DIST += man/hyperdex-set-read-only.1.md
EXTRA_DIST += man/hyperdex-set-read-only.1.h2m
hyperdex_set_read_only_SOURCES = tools/set-read-only.cc
hyperdex_set_read_only_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-set-read-only.1: man/hyperdex-set-read-only.1.h2m tools/set-read-only.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-set-read-only$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-set-read-only$(EXEEXT)

# hyperdex-set-read-write
EXTRA_DIST += man/hyperdex-set-read-write.1.md
EXTRA_DIST += man/hyperdex-set-read-write.1.h2m
hyperdex_set_read_write_SOURCES = tools/set-read-write.cc
hyperdex_set_read_write_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-set-read-write.1: man/hyperdex-set-read-write.1.h2m tools/set-read-write.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-set-read-write$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-set-read-write$(EXEEXT)

# hyperdex-raw-backup
EXTRA_DIST += man/hyperdex-raw-backup.1.md
EXTRA_DIST += man/hyperdex-raw-backup.1.h2m
hyperdex_raw_backup_SOURCES = tools/raw-backup.cc
hyperdex_raw_backup_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-raw-backup.1: man/hyperdex-raw-backup.1.h2m tools/raw-backup.cc
	@$(MAKE) --silent $(AM_MAKEFLAGS) hyperdex-raw-backup$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-raw-backup$(EXEEXT)

################################################################################
################################# Documentation ################################
################################################################################

EXTRA_DIST += $(wildcard doc/*.tex)
EXTRA_DIST += $(wildcard doc/*.xtx)
EXTRA_DIST += $(wildcard doc/*/*.tex)
EXTRA_DIST += $(wildcard doc/*/*/*.tex)
EXTRA_DIST += doc/Makefile
EXTRA_DIST += doc/latex.py
EXTRA_DIST += doc/api/c/iterate.c
EXTRA_DIST += doc/api/c/iterate.c

CLEANFILES += doc/HyperDex-$(VERSION).pdf

doc/HyperDex-$(VERSION).pdf:
	sphinx-build -d ${abs_top_builddir}/doc/_build -b latex ${abs_top_srcdir}/doc ${abs_top_builddir}/doc/_build/latex
	make -C ${abs_top_builddir}/doc/_build/latex all-pdf
	cp ${abs_top_builddir}/doc/_build/latex/HyperDex.pdf doc/HyperDex-$(VERSION).pdf
	rm -r ${abs_top_builddir}/doc/_build

################################################################################
################################## Maintenance #################################
################################################################################

if ENABLE_MAINTAINER_TOOLS
noinst_PROGRAMS += maint/lib-check
endif

EXTRA_DIST += bindings/generator.py

maint_lib_check_SOURCES = maint/lib-check.c
maint_lib_check_LDADD = -ldl

maintainer-generate:
	python bindings/c/gen_client_header.py
	python bindings/c/gen_doc.py
	python bindings/c/gen_wrappers.py
	python bindings/ruby/gen_ruby.py
	python maint/generate-replication-shell-stubs.py
	python maint/generate-search-shell-stubs.py
	python test/doc-extract.py python doc/quick-start.tex test/doc.quick-start.py
	python test/doc-extract.py python doc/data-types.tex test/doc.data-types.py
	python test/doc-extract.py python doc/async-ops.tex test/doc.async-ops.py
	python test/doc-extract.py python doc/atomic-ops.tex test/doc.atomic-ops.py
