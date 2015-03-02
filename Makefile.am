## Copyright (c) 2012-2014, Cornell University
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

AM_CPPFLAGS  = -I${abs_top_srcdir}/include $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(HYPERLEVELDB_CFLAGS) $(REPLICANT_CFLAGS) $(MACAROONS_CFLAGS) $(TREADSTONE_CFLAGS)
AM_CFLAGS    = -fvisibility=hidden $(WANAL_CFLAGS)
AM_CXXFLAGS  = -fvisibility=hidden -fvisibility-inlines-hidden $(PO6_CFLAGS) $(E_CFLAGS) $(BUSYBEE_CFLAGS) $(HYPERLEVELDB_CFLAGS) $(REPLICANT_CFLAGS) $(MACAROONS_CFLAGS) $(TREADSTONE_CFLAGS) $(WANAL_CXXFLAGS)
AM_MAKEFLAGS = --no-print-directory
AM_YFLAGS = -d
HELP2MAN_FLAGS = --no-discard-stderr --libtool --no-info --version-string=$(VERSION) --manual="HyperDex User Manual"
TESTS_ENVIRONMENT = . $(abs_top_srcdir)/test/env.sh "${abs_top_srcdir}" "${abs_top_builddir}" "${VERSION}";
AM_DISTCHECK_CONFIGURE_FLAGS = --enable-python-bindings --enable-java-bindings --enable-ruby-bindings

hyperdexexecdir = $(libexecdir)/$(PACKAGE)-$(VERSION)
hyperdexheaderdir = $(includedir)/hyperdex
pkgconfigdir = $(libdir)/pkgconfig

BUILT_SOURCES =

EXTRA_DIST =
EXTRA_DIST += THIRD-PARTY

CLEANFILES =

noinst_HEADERS =
hyperdexheader_HEADERS =

lib_LTLIBRARIES =

bin_PROGRAMS =
noinst_PROGRAMS =
check_PROGRAMS =

hyperdexexec_SCRIPTS =
hyperdexexec_PROGRAMS =
hyperdexexec_LTLIBRARIES =

dist_man_MANS =

pkgconfig_DATA =

jardir = $(datadir)/java
jar_DATA =

pkgpyexecdir = $(pyexecdir)/hyperdex
pkgpyexec_DATA =
pkgpyexec_LTLIBRARIES =

rubyexecdir = $(RUBY_EXT_LIB)
rubyexec_LTLIBRARIES =

TESTS =

################################################################################
#################################### Common ####################################
################################################################################

%.h2m: %.md
	$(pandoc_verbose)pandoc -s -w man -o $@ $<
	$(sed_verbose)sed -e 's/^\.SH \(.*\)$$/[\1]/' -e 's/^.PP$$//' $@ > $@.tmp
	@mv $@.tmp $@

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

th_sources = test/th_main.cc test/th.cc test/th.h

EXTRA_DIST += LICENSE
EXTRA_DIST += SIGNED-OFF-BY

include_HEADERS = include/hyperdex.h

noinst_HEADERS += include/hyperdex.h
noinst_HEADERS += namespace.h
noinst_HEADERS += visibility.h
noinst_HEADERS += common/attribute_check.h
noinst_HEADERS += common/attribute.h
noinst_HEADERS += common/auth_wallet.h
noinst_HEADERS += common/configuration_flags.h
noinst_HEADERS += common/configuration.h
noinst_HEADERS += common/coordinator_link.h
noinst_HEADERS += common/coordinator_returncode.h
noinst_HEADERS += common/datatype_document.h
noinst_HEADERS += common/datatype_float.h
noinst_HEADERS += common/datatype_info.h
noinst_HEADERS += common/datatype_int64.h
noinst_HEADERS += common/datatype_list.h
noinst_HEADERS += common/datatype_macaroon_secret.h
noinst_HEADERS += common/datatype_map.h
noinst_HEADERS += common/datatype_set.h
noinst_HEADERS += common/datatype_string.h
noinst_HEADERS += common/datatype_timestamp.h
noinst_HEADERS += common/documents.h
noinst_HEADERS += common/funcall.h
noinst_HEADERS += common/hash.h
noinst_HEADERS += common/hyperspace.h
noinst_HEADERS += common/ids.h
noinst_HEADERS += common/index.h
noinst_HEADERS += common/json_path.h
noinst_HEADERS += common/key_change.h
noinst_HEADERS += common/macros.h
noinst_HEADERS += common/mapper.h
noinst_HEADERS += common/network_msgtype.h
noinst_HEADERS += common/network_returncode.h
noinst_HEADERS += common/ordered_encoding.h
noinst_HEADERS += common/range.h
noinst_HEADERS += common/range_searches.h
noinst_HEADERS += common/regex_match.h
noinst_HEADERS += common/schema.h
noinst_HEADERS += common/serialization.h
noinst_HEADERS += common/server.h
noinst_HEADERS += common/transfer.h
noinst_HEADERS += tools/common.h
noinst_HEADERS += osx/ieee754.h

check_PROGRAMS += common/test/ordered_encoding
TESTS += common/test/ordered_encoding

common_test_ordered_encoding_SOURCES = common/test/ordered_encoding.cc common/ordered_encoding.cc $(th_sources)
common_test_ordered_encoding_CXXFLAGS = $(AM_CXXFLAGS) $(CXXFLAGS)

################################################################################
################################### City Hash ##################################
################################################################################

EXTRA_DIST += cityhash/COPYING

noinst_HEADERS += cityhash/city.h

check_PROGRAMS += cityhash/test/city
TESTS += cityhash/test/city

cityhash_test_city_SOURCES = cityhash/test/city.cc cityhash/city.cc
cityhash_test_city_CXXFLAGS = $(AM_CXXFLAGS) $(CXXFLAGS)

################################################################################
#################################### Daemon ####################################
################################################################################

if ENABLE_DAEMON
hyperdexexec_PROGRAMS += hyperdex-daemon
dist_man_MANS += man/hyperdex-daemon.1
endif

EXTRA_DIST += initscripts/sysv/default/hyperdex-daemon
EXTRA_DIST += initscripts/sysv/init.d/hyperdex-daemon

noinst_HEADERS += daemon/auth.h
noinst_HEADERS += daemon/background_thread.h
noinst_HEADERS += daemon/communication.h
noinst_HEADERS += daemon/coordinator_link_wrapper.h
noinst_HEADERS += daemon/daemon.h
noinst_HEADERS += daemon/datalayer_checkpointer_thread.h
noinst_HEADERS += daemon/datalayer_encodings.h
noinst_HEADERS += daemon/datalayer.h
noinst_HEADERS += daemon/datalayer_indexer_thread.h
noinst_HEADERS += daemon/datalayer_index_state.h
noinst_HEADERS += daemon/datalayer_iterator.h
noinst_HEADERS += daemon/datalayer_wiper_indexer_mediator.h
noinst_HEADERS += daemon/datalayer_wiper_thread.h
noinst_HEADERS += daemon/identifier_collector.h
noinst_HEADERS += daemon/identifier_generator.h
noinst_HEADERS += daemon/index_container.h
noinst_HEADERS += daemon/index_document.h
noinst_HEADERS += daemon/index_float.h
noinst_HEADERS += daemon/index_info.h
noinst_HEADERS += daemon/index_int64.h
noinst_HEADERS += daemon/index_list.h
noinst_HEADERS += daemon/index_map.h
noinst_HEADERS += daemon/index_primitive.h
noinst_HEADERS += daemon/index_set.h
noinst_HEADERS += daemon/index_string.h
noinst_HEADERS += daemon/index_timestamp.h
noinst_HEADERS += daemon/key_operation.h
noinst_HEADERS += daemon/key_region.h
noinst_HEADERS += daemon/key_state.h
noinst_HEADERS += daemon/leveldb.h
noinst_HEADERS += daemon/performance_counter.h
noinst_HEADERS += daemon/reconfigure_returncode.h
noinst_HEADERS += daemon/region_timestamp.h
noinst_HEADERS += daemon/replication_manager.h
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
hyperdex_daemon_SOURCES += common/auth_wallet.cc
hyperdex_daemon_SOURCES += common/configuration.cc
hyperdex_daemon_SOURCES += common/coordinator_link.cc
hyperdex_daemon_SOURCES += common/coordinator_returncode.cc
hyperdex_daemon_SOURCES += common/datatype_document.cc
hyperdex_daemon_SOURCES += common/datatype_float.cc
hyperdex_daemon_SOURCES += common/datatype_info.cc
hyperdex_daemon_SOURCES += common/datatype_int64.cc
hyperdex_daemon_SOURCES += common/datatype_list.cc
hyperdex_daemon_SOURCES += common/datatype_timestamp.cc
hyperdex_daemon_SOURCES += common/datatype_macaroon_secret.cc
hyperdex_daemon_SOURCES += common/datatype_map.cc
hyperdex_daemon_SOURCES += common/datatype_set.cc
hyperdex_daemon_SOURCES += common/datatype_string.cc
hyperdex_daemon_SOURCES += common/documents.cc
hyperdex_daemon_SOURCES += common/funcall.cc
hyperdex_daemon_SOURCES += common/hash.cc
hyperdex_daemon_SOURCES += common/hyperdex.cc
hyperdex_daemon_SOURCES += common/hyperspace.cc
hyperdex_daemon_SOURCES += common/ids.cc
hyperdex_daemon_SOURCES += common/index.cc
hyperdex_daemon_SOURCES += common/key_change.cc
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
hyperdex_daemon_SOURCES += cityhash/city.cc
hyperdex_daemon_SOURCES += daemon/auth.cc
hyperdex_daemon_SOURCES += daemon/background_thread.cc
hyperdex_daemon_SOURCES += daemon/communication.cc
hyperdex_daemon_SOURCES += daemon/coordinator_link_wrapper.cc
hyperdex_daemon_SOURCES += daemon/daemon.cc
hyperdex_daemon_SOURCES += daemon/datalayer.cc
hyperdex_daemon_SOURCES += daemon/datalayer_checkpointer_thread.cc
hyperdex_daemon_SOURCES += daemon/datalayer_encodings.cc
hyperdex_daemon_SOURCES += daemon/datalayer_indexer_thread.cc
hyperdex_daemon_SOURCES += daemon/datalayer_iterator.cc
hyperdex_daemon_SOURCES += daemon/datalayer_wiper_thread.cc
hyperdex_daemon_SOURCES += daemon/identifier_collector.cc
hyperdex_daemon_SOURCES += daemon/identifier_generator.cc
hyperdex_daemon_SOURCES += daemon/index_container.cc
hyperdex_daemon_SOURCES += daemon/index_document.cc
hyperdex_daemon_SOURCES += daemon/index_float.cc
hyperdex_daemon_SOURCES += daemon/index_info.cc
hyperdex_daemon_SOURCES += daemon/index_int64.cc
hyperdex_daemon_SOURCES += daemon/index_list.cc
hyperdex_daemon_SOURCES += daemon/index_timestamp.cc
hyperdex_daemon_SOURCES += daemon/index_map.cc
hyperdex_daemon_SOURCES += daemon/index_primitive.cc
hyperdex_daemon_SOURCES += daemon/index_set.cc
hyperdex_daemon_SOURCES += daemon/index_string.cc
hyperdex_daemon_SOURCES += daemon/key_operation.cc
hyperdex_daemon_SOURCES += daemon/key_region.cc
hyperdex_daemon_SOURCES += daemon/key_state.cc
hyperdex_daemon_SOURCES += daemon/main.cc
hyperdex_daemon_SOURCES += daemon/replication_manager.cc
hyperdex_daemon_SOURCES += daemon/search_manager.cc
hyperdex_daemon_SOURCES += daemon/state_transfer_manager.cc
hyperdex_daemon_SOURCES += daemon/state_transfer_manager_pending.cc
hyperdex_daemon_SOURCES += daemon/state_transfer_manager_transfer_in_state.cc
hyperdex_daemon_SOURCES += daemon/state_transfer_manager_transfer_out_state.cc
hyperdex_daemon_CXXFLAGS = $(AM_CXXFLAGS) $(CXXFLAGS)
hyperdex_daemon_LDADD =
hyperdex_daemon_LDADD += $(MACAROONS_LIBS)
hyperdex_daemon_LDADD += $(REPLICANT_LIBS)
hyperdex_daemon_LDADD += $(HYPERLEVELDB_LIBS)
hyperdex_daemon_LDADD += $(BUSYBEE_LIBS)
hyperdex_daemon_LDADD += $(E_LIBS)
hyperdex_daemon_LDADD += $(TREADSTONE_LIBS)
hyperdex_daemon_LDADD += -lpopt -lglog $(LRT_LDFLAGS) -lpthread
man/hyperdex-daemon.1: man/hyperdex-daemon.1.h2m daemon/main.cc | hyperdex-daemon$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-daemon$(EXEEXT)

check_PROGRAMS += daemon/test/identifier_collector
check_PROGRAMS += daemon/test/identifier_generator
TESTS += daemon/test/identifier_collector
TESTS += daemon/test/identifier_generator

daemon_test_identifier_collector_SOURCES = daemon/test/identifier_collector.cc daemon/identifier_collector.cc $(th_sources)
daemon_test_identifier_collector_CXXFLAGS = $(AM_CXXFLAGS) $(CXXFLAGS)
daemon_test_identifier_collector_LDFLAGS = $(E_LIBS)

daemon_test_identifier_generator_SOURCES = daemon/test/identifier_generator.cc daemon/identifier_generator.cc $(th_sources)
daemon_test_identifier_generator_CXXFLAGS = $(AM_CXXFLAGS) $(CXXFLAGS)
daemon_test_identifier_generator_LDFLAGS = $(E_LIBS)

################################################################################
################################## Coordinator #################################
################################################################################

if ENABLE_COORDINATOR
hyperdexexec_LTLIBRARIES += libhyperdex-coordinator.la
hyperdexexec_PROGRAMS += hyperdex-coordinator
dist_man_MANS += man/hyperdex-coordinator.1
endif

EXTRA_DIST += initscripts/sysv/default/hyperdex-coordinator
EXTRA_DIST += initscripts/sysv/init.d/hyperdex-coordinator

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
libhyperdex_coordinator_la_SOURCES += common/index.cc
libhyperdex_coordinator_la_SOURCES += common/schema.cc
libhyperdex_coordinator_la_SOURCES += common/serialization.cc
libhyperdex_coordinator_la_SOURCES += common/server.cc
libhyperdex_coordinator_la_SOURCES += common/transfer.cc
libhyperdex_coordinator_la_SOURCES += coordinator/coordinator.cc
libhyperdex_coordinator_la_SOURCES += coordinator/replica_sets.cc
libhyperdex_coordinator_la_SOURCES += coordinator/server_barrier.cc
libhyperdex_coordinator_la_SOURCES += coordinator/symtable.c
libhyperdex_coordinator_la_SOURCES += coordinator/transitions.cc
libhyperdex_coordinator_la_LIBADD =
libhyperdex_coordinator_la_LIBADD += $(E_LIBS)

EXTRA_DIST += man/hyperdex-coordinator.1.md
EXTRA_DIST += man/hyperdex-coordinator.1.h2m
hyperdex_coordinator_SOURCES = tools/coordinator.cc
hyperdex_coordinator_CPPFLAGS = -DHYPERDEX_EXEC_DIR=\""$(hyperdexexecdir)\"" $(AM_CPPFLAGS) $(CPPFLAGS)
hyperdex_coordinator_LDADD =
hyperdex_coordinator_LDADD += $(BUSYBEE_LIBS)
hyperdex_coordinator_LDADD += $(E_LIBS)
hyperdex_coordinator_LDADD += -lpopt
man/hyperdex-coordinator.1: man/hyperdex-coordinator.1.h2m | hyperdex-coordinator${EXEEXT}
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
noinst_HEADERS += client/pending_get_partial.h
noinst_HEADERS += client/pending_group_atomic.h
noinst_HEADERS += client/pending.h
noinst_HEADERS += client/pending_search_describe.h
noinst_HEADERS += client/pending_search.h
noinst_HEADERS += client/pending_sorted_search.h
noinst_HEADERS += client/util.h

libhyperdex_client_la_SOURCES =
libhyperdex_client_la_SOURCES += common/attribute.cc
libhyperdex_client_la_SOURCES += common/attribute_check.cc
libhyperdex_client_la_SOURCES += common/auth_wallet.cc
libhyperdex_client_la_SOURCES += common/configuration.cc
libhyperdex_client_la_SOURCES += common/coordinator_link.cc
libhyperdex_client_la_SOURCES += common/datatype_document.cc
libhyperdex_client_la_SOURCES += common/datatype_float.cc
libhyperdex_client_la_SOURCES += common/datatype_info.cc
libhyperdex_client_la_SOURCES += common/datatype_int64.cc
libhyperdex_client_la_SOURCES += common/datatype_list.cc
libhyperdex_client_la_SOURCES += common/datatype_macaroon_secret.cc
libhyperdex_client_la_SOURCES += common/datatype_map.cc
libhyperdex_client_la_SOURCES += common/datatype_set.cc
libhyperdex_client_la_SOURCES += common/datatype_timestamp.cc
libhyperdex_client_la_SOURCES += common/datatype_string.cc
libhyperdex_client_la_SOURCES += common/documents.cc
libhyperdex_client_la_SOURCES += common/funcall.cc
libhyperdex_client_la_SOURCES += common/hash.cc
libhyperdex_client_la_SOURCES += common/hyperdex.cc
libhyperdex_client_la_SOURCES += common/hyperspace.cc
libhyperdex_client_la_SOURCES += common/ids.cc
libhyperdex_client_la_SOURCES += common/index.cc
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
libhyperdex_client_la_SOURCES += cityhash/city.cc
libhyperdex_client_la_SOURCES += client/c.cc
libhyperdex_client_la_SOURCES += client/client.cc
libhyperdex_client_la_SOURCES += client/datastructures.cc
libhyperdex_client_la_SOURCES += client/keyop_info.cc
libhyperdex_client_la_SOURCES += client/pending_aggregation.cc
libhyperdex_client_la_SOURCES += client/pending_atomic.cc
libhyperdex_client_la_SOURCES += client/pending_group_atomic.cc
libhyperdex_client_la_SOURCES += client/pending.cc
libhyperdex_client_la_SOURCES += client/pending_count.cc
libhyperdex_client_la_SOURCES += client/pending_get.cc
libhyperdex_client_la_SOURCES += client/pending_get_partial.cc
libhyperdex_client_la_SOURCES += client/pending_search.cc
libhyperdex_client_la_SOURCES += client/pending_search_describe.cc
libhyperdex_client_la_SOURCES += client/pending_sorted_search.cc
libhyperdex_client_la_SOURCES += client/util.cc
libhyperdex_client_la_LIBADD =
libhyperdex_client_la_LIBADD += $(MACAROONS_LIBS)
libhyperdex_client_la_LIBADD += $(REPLICANT_LIBS)
libhyperdex_client_la_LIBADD += $(BUSYBEE_LIBS)
libhyperdex_client_la_LIBADD += $(E_LIBS)
libhyperdex_client_la_LIBADD += $(TREADSTONE_LIBS)

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
noinst_HEADERS += admin/backup_state_machine.h
noinst_HEADERS += admin/constants.h
noinst_HEADERS += admin/coord_rpc.h
noinst_HEADERS += admin/coord_rpc_backup.h
noinst_HEADERS += admin/coord_rpc_generic.h
noinst_HEADERS += admin/hyperspace_builder_internal.h
noinst_HEADERS += admin/multi_yieldable.h
noinst_HEADERS += admin/partition.h
noinst_HEADERS += admin/pending.h
noinst_HEADERS += admin/pending_perf_counters.h
noinst_HEADERS += admin/pending_raw_backup.h
noinst_HEADERS += admin/pending_string.h
noinst_HEADERS += admin/yieldable.h

libhyperdex_admin_la_SOURCES =
libhyperdex_admin_la_SOURCES += common/attribute.cc
libhyperdex_admin_la_SOURCES += common/attribute_check.cc
libhyperdex_admin_la_SOURCES += common/configuration.cc
libhyperdex_admin_la_SOURCES += common/coordinator_link.cc
libhyperdex_admin_la_SOURCES += common/datatype_document.cc
libhyperdex_admin_la_SOURCES += common/datatype_float.cc
libhyperdex_admin_la_SOURCES += common/datatype_info.cc
libhyperdex_admin_la_SOURCES += common/datatype_int64.cc
libhyperdex_admin_la_SOURCES += common/datatype_list.cc
libhyperdex_admin_la_SOURCES += common/datatype_macaroon_secret.cc
libhyperdex_admin_la_SOURCES += common/datatype_map.cc
libhyperdex_admin_la_SOURCES += common/datatype_set.cc
libhyperdex_admin_la_SOURCES += common/datatype_timestamp.cc
libhyperdex_admin_la_SOURCES += common/datatype_string.cc
libhyperdex_admin_la_SOURCES += common/documents.cc
libhyperdex_admin_la_SOURCES += common/hash.cc
libhyperdex_admin_la_SOURCES += common/hyperdex.cc
libhyperdex_admin_la_SOURCES += common/hyperspace.cc
libhyperdex_admin_la_SOURCES += common/ids.cc
libhyperdex_admin_la_SOURCES += common/index.cc
libhyperdex_admin_la_SOURCES += common/mapper.cc
libhyperdex_admin_la_SOURCES += common/network_msgtype.cc
libhyperdex_admin_la_SOURCES += common/ordered_encoding.cc
libhyperdex_admin_la_SOURCES += common/range.cc
libhyperdex_admin_la_SOURCES += common/range_searches.cc
libhyperdex_admin_la_SOURCES += common/regex_match.cc
libhyperdex_admin_la_SOURCES += common/schema.cc
libhyperdex_admin_la_SOURCES += common/serialization.cc
libhyperdex_admin_la_SOURCES += common/server.cc
libhyperdex_admin_la_SOURCES += common/transfer.cc
libhyperdex_admin_la_SOURCES += cityhash/city.cc
libhyperdex_admin_la_SOURCES += admin/admin.cc
libhyperdex_admin_la_SOURCES += admin/backup_state_machine.cc
libhyperdex_admin_la_SOURCES += admin/c.cc
libhyperdex_admin_la_SOURCES += admin/coord_rpc.cc
libhyperdex_admin_la_SOURCES += admin/coord_rpc_backup.cc
libhyperdex_admin_la_SOURCES += admin/coord_rpc_generic.cc
libhyperdex_admin_la_SOURCES += admin/hyperspace_builder.cc
libhyperdex_admin_la_SOURCES += admin/multi_yieldable.cc
libhyperdex_admin_la_SOURCES += admin/parse_space_l.l
libhyperdex_admin_la_SOURCES += admin/parse_space_y.y
libhyperdex_admin_la_SOURCES += admin/partition.cc
libhyperdex_admin_la_SOURCES += admin/pending.cc
libhyperdex_admin_la_SOURCES += admin/pending_perf_counters.cc
libhyperdex_admin_la_SOURCES += admin/pending_raw_backup.cc
libhyperdex_admin_la_SOURCES += admin/pending_string.cc
libhyperdex_admin_la_SOURCES += admin/raw_backup.cc
libhyperdex_admin_la_SOURCES += admin/yieldable.cc
libhyperdex_admin_la_LIBADD =
libhyperdex_admin_la_LIBADD += $(MACAROONS_LIBS)
libhyperdex_admin_la_LIBADD += $(REPLICANT_LIBS)
libhyperdex_admin_la_LIBADD += $(BUSYBEE_LIBS)
libhyperdex_admin_la_LIBADD += $(E_LIBS)
libhyperdex_admin_la_LIBADD += $(TREADSTONE_LIBS)

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

EXTRA_DIST += bindings/c.py

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

EXTRA_DIST += bindings/go/admin_api.go
EXTRA_DIST += bindings/go/admin_test.go
EXTRA_DIST += bindings/go/client
EXTRA_DIST += bindings/go/client/client.go
EXTRA_DIST += bindings/go/hypergo.go

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
CLEANFILES += bindings/java/org/hyperdex/client/*.class
CLEANFILES += test/java/*.class

EXTRA_DIST += bindings/java.py
EXTRA_DIST += bindings/java/org/hyperdex/client/ByteString.java
EXTRA_DIST += bindings/java/org_hyperdex_client_Client.definitions.c
EXTRA_DIST += bindings/java/org/hyperdex/client/Client.java
EXTRA_DIST += bindings/java/org/hyperdex/client/Deferred.java
EXTRA_DIST += bindings/java/org/hyperdex/client/GreaterEqual.java
EXTRA_DIST += bindings/java/org/hyperdex/client/GreaterThan.java
EXTRA_DIST += bindings/java/org/hyperdex/client/HyperDexClientException.java
EXTRA_DIST += bindings/java/org/hyperdex/client/Iterator.java
EXTRA_DIST += bindings/java/org/hyperdex/client/LengthEquals.java
EXTRA_DIST += bindings/java/org/hyperdex/client/LengthGreaterEqual.java
EXTRA_DIST += bindings/java/org/hyperdex/client/LengthLessEqual.java
EXTRA_DIST += bindings/java/org/hyperdex/client/LessEqual.java
EXTRA_DIST += bindings/java/org/hyperdex/client/LessThan.java
EXTRA_DIST += bindings/java/org/hyperdex/client/Operation.java
EXTRA_DIST += bindings/java/org/hyperdex/client/Predicate.java
EXTRA_DIST += bindings/java/org/hyperdex/client/Range.java
EXTRA_DIST += bindings/java/org/hyperdex/client/Regex.java
EXTRA_DIST += bindings/java/org/hyperdex/ycsb/HyperDex.java
EXTRA_DIST += test/java/.exists

# Client
noinst_HEADERS += bindings/java/org_hyperdex_client_Client.h
noinst_HEADERS += bindings/java/org_hyperdex_client_Deferred.h
noinst_HEADERS += bindings/java/org_hyperdex_client_GreaterEqual.h
noinst_HEADERS += bindings/java/org_hyperdex_client_GreaterThan.h
noinst_HEADERS += bindings/java/org_hyperdex_client_Iterator.h
noinst_HEADERS += bindings/java/org_hyperdex_client_LengthEquals.h
noinst_HEADERS += bindings/java/org_hyperdex_client_LengthGreaterEqual.h
noinst_HEADERS += bindings/java/org_hyperdex_client_LengthLessEqual.h
noinst_HEADERS += bindings/java/org_hyperdex_client_LessEqual.h
noinst_HEADERS += bindings/java/org_hyperdex_client_LessThan.h
noinst_HEADERS += bindings/java/org_hyperdex_client_Range.h
noinst_HEADERS += bindings/java/org_hyperdex_client_Regex.h

libhyperdex_client_java_la_SOURCES =
libhyperdex_client_java_la_SOURCES += bindings/java/org_hyperdex_client_Client.c
libhyperdex_client_java_la_CPPFLAGS = $(JNI_CPPFLAGS) $(AM_CPPFLAGS) $(CPPFLAGS)
libhyperdex_client_java_la_CFLAGS = $(AM_CFLAGS) $(CFLAGS) -Wmissing-prototypes
libhyperdex_client_java_la_LIBADD = libhyperdex-client.la

JAVA_SRC_BASE=${abs_top_srcdir}/bindings/java
JAVA_BUILD_BASE=${abs_top_builddir}/bindings/java
JAVA_SRC_PKG=${JAVA_SRC_BASE}/org/hyperdex
JAVA_BUILD_PKG=${JAVA_BUILD_BASE}/org/hyperdex

$(hyperdex_client_jarfile): $(wildcard ${JAVA_SRC_PKG}/client/*.java)
	javac -d ${JAVA_BUILD_BASE} ${JAVA_SRC_PKG}/client/*.java
	cd ${JAVA_BUILD_BASE} && $(JAR) cvf $(JARFLAGS) ${abs_top_builddir}/${hyperdex_client_jarfile} org/hyperdex/client/*.class

# YCSB
$(hyperdex_ycsb_jarfile): ${JAVA_SRC_PKG}/ycsb/HyperDex.java $(hyperdex_client_jarfile)
	javac -d ${JAVA_BUILD_BASE} ${JAVA_SRC_PKG}/client/*.java
	javac -cp $(abs_top_builddir)/$(hyperdex_client_jarfile):$(CLASSPATH) -d ${JAVA_BUILD_BASE} ${JAVA_SRC_PKG}/ycsb/*.java
	cd ${JAVA_BUILD_BASE} && $(JAR) cvf $(JARFLAGS) ${abs_top_builddir}/${hyperdex_ycsb_jarfile} org/hyperdex/ycsb/*.class

#################################### node.js ###################################

EXTRA_DIST += bindings/nodejs.py
EXTRA_DIST += bindings/node.js/binding.gyp
EXTRA_DIST += bindings/node.js/client.cc
EXTRA_DIST += bindings/node.js/client.declarations.cc
EXTRA_DIST += bindings/node.js/client.definitions.cc
EXTRA_DIST += bindings/node.js/client.prototypes.cc

#################################### Python ####################################

if ENABLE_PYTHON_BINDINGS
pkgpyexec_DATA += bindings/python/hyperdex/__init__.py
pkgpyexec_DATA += bindings/python/hyperdex/mongo.py
if ENABLE_CLIENT
pkgpyexec_LTLIBRARIES += bindings/python/hyperdex/client.la
endif
if ENABLE_ADMIN
pkgpyexec_LTLIBRARIES += bindings/python/hyperdex/admin.la
endif
endif

CLEANFILES += bindings/python/hyperdex/admin.so
CLEANFILES += bindings/python/hyperdex/client.so
CLEANFILES += bindings/python/hyperdex/*.pyc

EXTRA_DIST += bindings/python.py
EXTRA_DIST += bindings/python/hyperdex/__init__.py
EXTRA_DIST += bindings/python/hyperdex/mongo.py

EXTRA_DIST += bindings/python/hyperdex/client.pyx
bindings_python_hyperdex_client_la_SOURCES = bindings/python/hyperdex/client.c
bindings_python_hyperdex_client_la_CPPFLAGS =
bindings_python_hyperdex_client_la_CPPFLAGS += $(PYTHON_CPPFLAGS)
bindings_python_hyperdex_client_la_CPPFLAGS += $(AM_CPPFLAGS)
bindings_python_hyperdex_client_la_CPPFLAGS += $(CPPFLAGS)
bindings_python_hyperdex_client_la_CFLAGS =
bindings_python_hyperdex_client_la_CFLAGS += -fvisibility=default
bindings_python_hyperdex_client_la_CFLAGS += $(CFLAGS)
bindings_python_hyperdex_client_la_LIBADD =
bindings_python_hyperdex_client_la_LIBADD += libhyperdex-client.la
bindings_python_hyperdex_client_la_LIBADD += $(PYTHON_LDFLAGS)
bindings_python_hyperdex_client_la_LDFLAGS = -module -avoid-version -export-symbols-regex initclient $(AM_LDFLAGS) $(LDFLAGS)
bindings/python/hyperdex/client.c: bindings/python/hyperdex/client.pyx
	$(pyx_verbose)cython bindings/python/hyperdex/client.pyx

bindings/python/hyperdex/client.so: bindings/python/hyperdex/client.la
	$(LN_S) -f ${abs_top_builddir}/bindings/python/hyperdex/.libs/client.so $@

EXTRA_DIST += bindings/python/hyperdex/admin.pyx
bindings_python_hyperdex_admin_la_SOURCES = bindings/python/hyperdex/admin.c
bindings_python_hyperdex_admin_la_CPPFLAGS =
bindings_python_hyperdex_admin_la_CPPFLAGS += $(PYTHON_CPPFLAGS)
bindings_python_hyperdex_admin_la_CPPFLAGS += $(AM_CPPFLAGS)
bindings_python_hyperdex_admin_la_CPPFLAGS += $(CPPFLAGS)
bindings_python_hyperdex_admin_la_CFLAGS =
bindings_python_hyperdex_admin_la_CFLAGS += -fvisibility=default
bindings_python_hyperdex_admin_la_CFLAGS += $(CFLAGS)
bindings_python_hyperdex_admin_la_LIBADD =
bindings_python_hyperdex_admin_la_LIBADD += libhyperdex-admin.la
bindings_python_hyperdex_admin_la_LIBADD += $(PYTHON_LDFLAGS)
bindings_python_hyperdex_admin_la_LDFLAGS = -module -avoid-version -export-symbols-regex initadmin $(AM_LDFLAGS) $(LDFLAGS)
bindings/python/hyperdex/admin.c: bindings/python/hyperdex/admin.pyx
	$(pyx_verbose)cython bindings/python/hyperdex/admin.pyx

bindings/python/hyperdex/admin.so: bindings/python/hyperdex/admin.la
	$(LN_S) -f ${abs_top_builddir}/bindings/python/hyperdex/.libs/admin.so $@

link-python: bindings/python/hyperdex/client.so bindings/python/hyperdex/admin.so

##################################### Ruby #####################################

if ENABLE_RUBY_BINDINGS
rubyexec_LTLIBRARIES += bindings/ruby/hyperdex.la
endif

EXTRA_DIST += bindings/ruby.py
EXTRA_DIST += bindings/ruby/definitions.c
EXTRA_DIST += bindings/ruby/prototypes.c

bindings_ruby_hyperdex_la_SOURCES = bindings/ruby/hyperdex.c
bindings_ruby_hyperdex_la_CPPFLAGS =
bindings_ruby_hyperdex_la_CPPFLAGS += -I$(RUBY_EXT_INC)
bindings_ruby_hyperdex_la_CPPFLAGS += -I$(RUBY_EXT_INC)/ruby
bindings_ruby_hyperdex_la_CPPFLAGS += -I$(RUBY_EXT_ARCHINC)
bindings_ruby_hyperdex_la_CPPFLAGS += -I$(RUBY_EXT_ARCHINC)/ruby
bindings_ruby_hyperdex_la_CPPFLAGS += $(RUBY_EXT_CPPFLAGS)
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

###################################### Go ######################################

################################################################################
##################################### Tests ####################################
################################################################################

check_PROGRAMS += test/replication-stress-test
check_PROGRAMS += test/search-stress-test
check_PROGRAMS += test/simple-consistency-stress-test

shell_wrappers =

doctest_wrappers =
doctest_wrappers += test/sh/doc.quick-start.sh
doctest_wrappers += test/sh/doc.data-types.sh
doctest_wrappers += test/sh/doc.documents.sh
doctest_wrappers += test/sh/doc.async-ops.sh
doctest_wrappers += test/sh/doc.atomic-ops.sh
doctest_wrappers += test/sh/doc.authorization.sh
shell_wrappers += $(doctest_wrappers)

stress_wrappers =
stress_wrappers += test/sh/replication.simple.daemons=1.fault-tolerance=0.sh
stress_wrappers += test/sh/replication.simple.daemons=4.fault-tolerance=0.sh
stress_wrappers += test/sh/replication.simple.daemons=4.fault-tolerance=1.sh
stress_wrappers += test/sh/replication.simple.daemons=4.fault-tolerance=2.sh
stress_wrappers += test/sh/replication.reverse.daemons=1.fault-tolerance=0.sh
stress_wrappers += test/sh/replication.reverse.daemons=4.fault-tolerance=0.sh
stress_wrappers += test/sh/replication.reverse.daemons=4.fault-tolerance=1.sh
stress_wrappers += test/sh/replication.reverse.daemons=4.fault-tolerance=2.sh
stress_wrappers += test/sh/replication.composite.daemons=1.fault-tolerance=0.sh
stress_wrappers += test/sh/replication.composite.daemons=4.fault-tolerance=0.sh
stress_wrappers += test/sh/replication.composite.daemons=4.fault-tolerance=1.sh
stress_wrappers += test/sh/replication.composite.daemons=4.fault-tolerance=2.sh
stress_wrappers += test/sh/search.simple.keytype=int,daemons=1.fault-tolerance=0.sh
stress_wrappers += test/sh/search.simple.keytype=int,daemons=4.fault-tolerance=0.sh
stress_wrappers += test/sh/search.simple.keytype=int,daemons=4.fault-tolerance=1.sh
stress_wrappers += test/sh/search.simple.keytype=string,daemons=1.fault-tolerance=0.sh
stress_wrappers += test/sh/search.simple.keytype=string,daemons=4.fault-tolerance=0.sh
stress_wrappers += test/sh/search.simple.keytype=string,daemons=4.fault-tolerance=1.sh
stress_wrappers += test/sh/search.index.keytype=int,daemons=1.fault-tolerance=0.sh
stress_wrappers += test/sh/search.index.keytype=int,daemons=4.fault-tolerance=0.sh
stress_wrappers += test/sh/search.index.keytype=int,daemons=4.fault-tolerance=1.sh
stress_wrappers += test/sh/search.index.keytype=string,daemons=1.fault-tolerance=0.sh
stress_wrappers += test/sh/search.index.keytype=string,daemons=4.fault-tolerance=0.sh
stress_wrappers += test/sh/search.index.keytype=string,daemons=4.fault-tolerance=1.sh
stress_wrappers += test/sh/search.subspace.keytype=int,daemons=1.fault-tolerance=0.sh
stress_wrappers += test/sh/search.subspace.keytype=int,daemons=4.fault-tolerance=0.sh
stress_wrappers += test/sh/search.subspace.keytype=int,daemons=4.fault-tolerance=1.sh
stress_wrappers += test/sh/search.subspace.keytype=string,daemons=1.fault-tolerance=0.sh
stress_wrappers += test/sh/search.subspace.keytype=string,daemons=4.fault-tolerance=0.sh
stress_wrappers += test/sh/search.subspace.keytype=string,daemons=4.fault-tolerance=1.sh
stress_wrappers += test/sh/search.combination.keytype=int,daemons=1.fault-tolerance=0.sh
stress_wrappers += test/sh/search.combination.keytype=int,daemons=4.fault-tolerance=0.sh
stress_wrappers += test/sh/search.combination.keytype=int,daemons=4.fault-tolerance=1.sh
stress_wrappers += test/sh/search.combination.keytype=string,daemons=1.fault-tolerance=0.sh
stress_wrappers += test/sh/search.combination.keytype=string,daemons=4.fault-tolerance=0.sh
stress_wrappers += test/sh/search.combination.keytype=string,daemons=4.fault-tolerance=1.sh
shell_wrappers += $(stress_wrappers)

python_wrappers =
#python_wrappers += test/sh/bindings.python.Admin.sh
python_wrappers += test/sh/bindings.python.BasicSearch.sh
python_wrappers += test/sh/bindings.python.Basic.sh
python_wrappers += test/sh/bindings.python.CondPut.sh
#python_wrappers += test/sh/bindings.python.DataTypeDocument.sh
python_wrappers += test/sh/bindings.python.DataTypeFloat.sh
python_wrappers += test/sh/bindings.python.DataTypeInt.sh
python_wrappers += test/sh/bindings.python.DataTypeListFloat.sh
python_wrappers += test/sh/bindings.python.DataTypeListInt.sh
python_wrappers += test/sh/bindings.python.DataTypeListString.sh
python_wrappers += test/sh/bindings.python.DataTypeMapFloatFloat.sh
python_wrappers += test/sh/bindings.python.DataTypeMapFloatInt.sh
python_wrappers += test/sh/bindings.python.DataTypeMapFloatString.sh
python_wrappers += test/sh/bindings.python.DataTypeMapIntFloat.sh
python_wrappers += test/sh/bindings.python.DataTypeMapIntInt.sh
python_wrappers += test/sh/bindings.python.DataTypeMapIntString.sh
python_wrappers += test/sh/bindings.python.DataTypeMapStringFloat.sh
python_wrappers += test/sh/bindings.python.DataTypeMapStringInt.sh
python_wrappers += test/sh/bindings.python.DataTypeMapStringString.sh
python_wrappers += test/sh/bindings.python.DataTypeSetFloat.sh
python_wrappers += test/sh/bindings.python.DataTypeSetInt.sh
python_wrappers += test/sh/bindings.python.DataTypeSetString.sh
python_wrappers += test/sh/bindings.python.DataTypeString.sh
python_wrappers += test/sh/bindings.python.GroupAtomic.sh
python_wrappers += test/sh/bindings.python.HyperMongo.sh
python_wrappers += test/sh/bindings.python.LengthString.sh
python_wrappers += test/sh/bindings.python.MultiAttribute.sh
python_wrappers += test/sh/bindings.python.RangeSearchInt.sh
python_wrappers += test/sh/bindings.python.RangeSearchString.sh
python_wrappers += test/sh/bindings.python.RegexSearch.sh
shell_wrappers += $(python_wrappers)

java_wrappers =
java_wrappers += test/sh/bindings.java.BasicSearch.sh
java_wrappers += test/sh/bindings.java.Basic.sh
java_wrappers += test/sh/bindings.java.CondPut.sh
java_wrappers += test/sh/bindings.java.DataTypeFloat.sh
java_wrappers += test/sh/bindings.java.DataTypeInt.sh
java_wrappers += test/sh/bindings.java.DataTypeListFloat.sh
java_wrappers += test/sh/bindings.java.DataTypeListInt.sh
java_wrappers += test/sh/bindings.java.DataTypeListString.sh
java_wrappers += test/sh/bindings.java.DataTypeMapFloatFloat.sh
java_wrappers += test/sh/bindings.java.DataTypeMapFloatInt.sh
java_wrappers += test/sh/bindings.java.DataTypeMapFloatString.sh
java_wrappers += test/sh/bindings.java.DataTypeMapIntFloat.sh
java_wrappers += test/sh/bindings.java.DataTypeMapIntInt.sh
java_wrappers += test/sh/bindings.java.DataTypeMapIntString.sh
java_wrappers += test/sh/bindings.java.DataTypeMapStringFloat.sh
java_wrappers += test/sh/bindings.java.DataTypeMapStringInt.sh
java_wrappers += test/sh/bindings.java.DataTypeMapStringString.sh
java_wrappers += test/sh/bindings.java.DataTypeSetFloat.sh
java_wrappers += test/sh/bindings.java.DataTypeSetInt.sh
java_wrappers += test/sh/bindings.java.DataTypeSetString.sh
java_wrappers += test/sh/bindings.java.DataTypeString.sh
java_wrappers += test/sh/bindings.java.LengthString.sh
java_wrappers += test/sh/bindings.java.MultiAttribute.sh
java_wrappers += test/sh/bindings.java.RangeSearchInt.sh
java_wrappers += test/sh/bindings.java.RangeSearchString.sh
java_wrappers += test/sh/bindings.java.RegexSearch.sh
shell_wrappers += $(java_wrappers)

ruby_wrappers =
ruby_wrappers += test/sh/bindings.ruby.BasicSearch.sh
ruby_wrappers += test/sh/bindings.ruby.Basic.sh
ruby_wrappers += test/sh/bindings.ruby.CondPut.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeFloat.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeInt.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeListFloat.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeListInt.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeListString.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeMapFloatFloat.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeMapFloatInt.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeMapFloatString.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeMapIntFloat.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeMapIntInt.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeMapIntString.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeMapStringFloat.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeMapStringInt.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeMapStringString.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeSetFloat.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeSetInt.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeSetString.sh
ruby_wrappers += test/sh/bindings.ruby.DataTypeString.sh
ruby_wrappers += test/sh/bindings.ruby.LengthString.sh
ruby_wrappers += test/sh/bindings.ruby.MultiAttribute.sh
ruby_wrappers += test/sh/bindings.ruby.RangeSearchInt.sh
ruby_wrappers += test/sh/bindings.ruby.RangeSearchString.sh
ruby_wrappers += test/sh/bindings.ruby.RegexSearch.sh
shell_wrappers += $(ruby_wrappers)

go_wrappers =
go_wrappers += test/sh/bindings.go.Basic.sh
go_wrappers += test/sh/bindings.go.BasicSearch.sh
go_wrappers += test/sh/bindings.go.CondPut.sh
go_wrappers += test/sh/bindings.go.DataTypeFloat.sh
go_wrappers += test/sh/bindings.go.DataTypeInt.sh
go_wrappers += test/sh/bindings.go.DataTypeListFloat.sh
go_wrappers += test/sh/bindings.go.DataTypeListInt.sh
go_wrappers += test/sh/bindings.go.DataTypeListString.sh
go_wrappers += test/sh/bindings.go.DataTypeMapFloatFloat.sh
go_wrappers += test/sh/bindings.go.DataTypeMapFloatInt.sh
go_wrappers += test/sh/bindings.go.DataTypeMapFloatString.sh
go_wrappers += test/sh/bindings.go.DataTypeMapIntFloat.sh
go_wrappers += test/sh/bindings.go.DataTypeMapIntInt.sh
go_wrappers += test/sh/bindings.go.DataTypeMapIntString.sh
go_wrappers += test/sh/bindings.go.DataTypeMapStringFloat.sh
go_wrappers += test/sh/bindings.go.DataTypeMapStringInt.sh
go_wrappers += test/sh/bindings.go.DataTypeMapStringString.sh
go_wrappers += test/sh/bindings.go.DataTypeSetFloat.sh
go_wrappers += test/sh/bindings.go.DataTypeSetInt.sh
go_wrappers += test/sh/bindings.go.DataTypeSetString.sh
go_wrappers += test/sh/bindings.go.DataTypeString.sh
go_wrappers += test/sh/bindings.go.LengthString.sh
go_wrappers += test/sh/bindings.go.MultiAttribute.sh
go_wrappers += test/sh/bindings.go.RangeSearchInt.sh
go_wrappers += test/sh/bindings.go.RangeSearchString.sh
go_wrappers += test/sh/bindings.go.RegexSearch.sh
shell_wrappers += $(go_wrappers)

rust_wrappers =
rust_wrappers += test/sh/bindings.rust.Basic.sh
rust_wrappers += test/sh/bindings.rust.BasicSearch.sh
rust_wrappers += test/sh/bindings.rust.CondPut.sh
rust_wrappers += test/sh/bindings.rust.DataTypeFloat.sh
rust_wrappers += test/sh/bindings.rust.DataTypeInt.sh
rust_wrappers += test/sh/bindings.rust.DataTypeListFloat.sh
rust_wrappers += test/sh/bindings.rust.DataTypeListInt.sh
rust_wrappers += test/sh/bindings.rust.DataTypeListString.sh
rust_wrappers += test/sh/bindings.rust.DataTypeMapFloatFloat.sh
rust_wrappers += test/sh/bindings.rust.DataTypeMapFloatInt.sh
rust_wrappers += test/sh/bindings.rust.DataTypeMapFloatString.sh
rust_wrappers += test/sh/bindings.rust.DataTypeMapIntFloat.sh
rust_wrappers += test/sh/bindings.rust.DataTypeMapIntInt.sh
rust_wrappers += test/sh/bindings.rust.DataTypeMapIntString.sh
rust_wrappers += test/sh/bindings.rust.DataTypeMapStringFloat.sh
rust_wrappers += test/sh/bindings.rust.DataTypeMapStringInt.sh
rust_wrappers += test/sh/bindings.rust.DataTypeMapStringString.sh
rust_wrappers += test/sh/bindings.rust.DataTypeSetFloat.sh
rust_wrappers += test/sh/bindings.rust.DataTypeSetInt.sh
rust_wrappers += test/sh/bindings.rust.DataTypeSetString.sh
rust_wrappers += test/sh/bindings.rust.DataTypeString.sh
rust_wrappers += test/sh/bindings.rust.LengthString.sh
rust_wrappers += test/sh/bindings.rust.MultiAttribute.sh
rust_wrappers += test/sh/bindings.rust.RangeSearchInt.sh
rust_wrappers += test/sh/bindings.rust.RangeSearchString.sh
rust_wrappers += test/sh/bindings.rust.RegexSearch.sh
shell_wrappers += $(rust_wrappers)

if ENABLE_ADMIN
if ENABLE_CLIENT
if ENABLE_PYTHON_BINDINGS
TESTS += $(doctest_wrappers)

if ENABLE_TIME_CONSUMING_TESTS
TESTS += $(stress_wrappers)
endif

TESTS += $(python_wrappers)
check_PROGRAMS += bindings/python/hyperdex/admin.so
check_PROGRAMS += bindings/python/hyperdex/client.so

if ENABLE_JAVA_BINDINGS
TESTS += $(java_wrappers)
endif

if ENABLE_RUBY_BINDINGS
TESTS += $(ruby_wrappers)
endif

if ENABLE_RUST_BINDINGS
TESTS += $(rust_wrappers)
endif

endif # ENABLE_PYTHON_BINDINGS
endif # ENABLE_CLIENT
endif # ENABLE_ADMIN

EXTRA_DIST += $(shell_wrappers)
EXTRA_DIST += test/doc.async-ops.py
EXTRA_DIST += test/doc.atomic-ops.py
EXTRA_DIST += test/doc.data-types.py
EXTRA_DIST += test/doc.documents.py
EXTRA_DIST += test/doc.authorization.py
EXTRA_DIST += test/doc-extract.py
EXTRA_DIST += test/doc.quick-start.py
EXTRA_DIST += test/doctest-runner.py
EXTRA_DIST += test/env.sh
EXTRA_DIST += test/runner.py
EXTRA_DIST += test/python/Basic.py
EXTRA_DIST += test/python/BasicSearch.py
EXTRA_DIST += test/python/CondPut.py
EXTRA_DIST += test/python/DataTypeDocument.py
EXTRA_DIST += test/python/DataTypeFloat.py
EXTRA_DIST += test/python/DataTypeInt.py
EXTRA_DIST += test/python/DataTypeListFloat.py
EXTRA_DIST += test/python/DataTypeListInt.py
EXTRA_DIST += test/python/DataTypeListString.py
EXTRA_DIST += test/python/DataTypeMapFloatFloat.py
EXTRA_DIST += test/python/DataTypeMapFloatInt.py
EXTRA_DIST += test/python/DataTypeMapFloatString.py
EXTRA_DIST += test/python/DataTypeMapIntFloat.py
EXTRA_DIST += test/python/DataTypeMapIntInt.py
EXTRA_DIST += test/python/DataTypeMapIntString.py
EXTRA_DIST += test/python/DataTypeMapStringFloat.py
EXTRA_DIST += test/python/DataTypeMapStringInt.py
EXTRA_DIST += test/python/DataTypeMapStringString.py
EXTRA_DIST += test/python/DataTypeSetFloat.py
EXTRA_DIST += test/python/DataTypeSetInt.py
EXTRA_DIST += test/python/DataTypeSetString.py
EXTRA_DIST += test/python/DataTypeString.py
EXTRA_DIST += test/python/GroupAtomic.py
EXTRA_DIST += test/python/HyperMongo.py
EXTRA_DIST += test/python/LengthString.py
EXTRA_DIST += test/python/MultiAttribute.py
EXTRA_DIST += test/python/RangeSearchInt.py
EXTRA_DIST += test/python/RangeSearchString.py
EXTRA_DIST += test/python/RegexSearch.py
EXTRA_DIST += test/python/testlib.py
EXTRA_DIST += test/java/Basic.java
EXTRA_DIST += test/java/BasicSearch.java
EXTRA_DIST += test/java/CondPut.java
EXTRA_DIST += test/java/DataTypeFloat.java
EXTRA_DIST += test/java/DataTypeInt.java
EXTRA_DIST += test/java/DataTypeListFloat.java
EXTRA_DIST += test/java/DataTypeListInt.java
EXTRA_DIST += test/java/DataTypeListString.java
EXTRA_DIST += test/java/DataTypeMapFloatFloat.java
EXTRA_DIST += test/java/DataTypeMapFloatInt.java
EXTRA_DIST += test/java/DataTypeMapFloatString.java
EXTRA_DIST += test/java/DataTypeMapIntFloat.java
EXTRA_DIST += test/java/DataTypeMapIntInt.java
EXTRA_DIST += test/java/DataTypeMapIntString.java
EXTRA_DIST += test/java/DataTypeMapStringFloat.java
EXTRA_DIST += test/java/DataTypeMapStringInt.java
EXTRA_DIST += test/java/DataTypeMapStringString.java
EXTRA_DIST += test/java/DataTypeSetFloat.java
EXTRA_DIST += test/java/DataTypeSetInt.java
EXTRA_DIST += test/java/DataTypeSetString.java
EXTRA_DIST += test/java/DataTypeString.java
EXTRA_DIST += test/java/LengthString.java
EXTRA_DIST += test/java/MultiAttribute.java
EXTRA_DIST += test/java/RangeSearchInt.java
EXTRA_DIST += test/java/RangeSearchString.java
EXTRA_DIST += test/java/RegexSearch.java
EXTRA_DIST += test/ruby/Basic.rb
EXTRA_DIST += test/ruby/BasicSearch.rb
EXTRA_DIST += test/ruby/CondPut.rb
EXTRA_DIST += test/ruby/DataTypeFloat.rb
EXTRA_DIST += test/ruby/DataTypeInt.rb
EXTRA_DIST += test/ruby/DataTypeListFloat.rb
EXTRA_DIST += test/ruby/DataTypeListInt.rb
EXTRA_DIST += test/ruby/DataTypeListString.rb
EXTRA_DIST += test/ruby/DataTypeMapFloatFloat.rb
EXTRA_DIST += test/ruby/DataTypeMapFloatInt.rb
EXTRA_DIST += test/ruby/DataTypeMapFloatString.rb
EXTRA_DIST += test/ruby/DataTypeMapIntFloat.rb
EXTRA_DIST += test/ruby/DataTypeMapIntInt.rb
EXTRA_DIST += test/ruby/DataTypeMapIntString.rb
EXTRA_DIST += test/ruby/DataTypeMapStringFloat.rb
EXTRA_DIST += test/ruby/DataTypeMapStringInt.rb
EXTRA_DIST += test/ruby/DataTypeMapStringString.rb
EXTRA_DIST += test/ruby/DataTypeSetFloat.rb
EXTRA_DIST += test/ruby/DataTypeSetInt.rb
EXTRA_DIST += test/ruby/DataTypeSetString.rb
EXTRA_DIST += test/ruby/DataTypeString.rb
EXTRA_DIST += test/ruby/LengthString.rb
EXTRA_DIST += test/ruby/MultiAttribute.rb
EXTRA_DIST += test/ruby/RangeSearchInt.rb
EXTRA_DIST += test/ruby/RangeSearchString.rb
EXTRA_DIST += test/ruby/RegexSearch.rb
EXTRA_DIST += test/go/Basic.go
EXTRA_DIST += test/go/BasicSearch.go
EXTRA_DIST += test/go/CondPut.go
EXTRA_DIST += test/go/DataTypeFloat.go
EXTRA_DIST += test/go/DataTypeInt.go
EXTRA_DIST += test/go/DataTypeListFloat.go
EXTRA_DIST += test/go/DataTypeListInt.go
EXTRA_DIST += test/go/DataTypeListString.go
EXTRA_DIST += test/go/DataTypeMapFloatFloat.go
EXTRA_DIST += test/go/DataTypeMapFloatInt.go
EXTRA_DIST += test/go/DataTypeMapFloatString.go
EXTRA_DIST += test/go/DataTypeMapIntFloat.go
EXTRA_DIST += test/go/DataTypeMapIntInt.go
EXTRA_DIST += test/go/DataTypeMapIntString.go
EXTRA_DIST += test/go/DataTypeMapStringFloat.go
EXTRA_DIST += test/go/DataTypeMapStringInt.go
EXTRA_DIST += test/go/DataTypeMapStringString.go
EXTRA_DIST += test/go/DataTypeSetFloat.go
EXTRA_DIST += test/go/DataTypeSetInt.go
EXTRA_DIST += test/go/DataTypeSetString.go
EXTRA_DIST += test/go/DataTypeString.go
EXTRA_DIST += test/go/LengthString.go
EXTRA_DIST += test/go/MultiAttribute.go
EXTRA_DIST += test/go/RangeSearchInt.go
EXTRA_DIST += test/go/RangeSearchString.go
EXTRA_DIST += test/go/RegexSearch.go

test_replication_stress_test_SOURCES = test/replication-stress-test.cc
test_replication_stress_test_LDADD = libhyperdex-client.la $(E_LIBS) -lpopt -lpthread

test_search_stress_test_SOURCES = test/search-stress-test.cc
test_search_stress_test_LDADD = libhyperdex-client.la $(E_LIBS) -lpopt -lpthread

test_simple_consistency_stress_test_SOURCES = test/simple-consistency-stress-test.cc
test_simple_consistency_stress_test_LDADD = libhyperdex-client.la $(E_LIBS) -lpopt -lpthread

################################################################################
##################################### Tools ####################################
################################################################################

if ENABLE_TOOLS
hyperdexexec_PROGRAMS += hyperdex-add-space
hyperdexexec_PROGRAMS += hyperdex-rm-space
hyperdexexec_PROGRAMS += hyperdex-mv-space
hyperdexexec_PROGRAMS += hyperdex-list-spaces
hyperdexexec_PROGRAMS += hyperdex-validate-space
hyperdexexec_PROGRAMS += hyperdex-add-index
hyperdexexec_PROGRAMS += hyperdex-rm-index
hyperdexexec_PROGRAMS += hyperdex-show-config
hyperdexexec_PROGRAMS += hyperdex-server-register
hyperdexexec_PROGRAMS += hyperdex-server-online
hyperdexexec_PROGRAMS += hyperdex-server-offline
hyperdexexec_PROGRAMS += hyperdex-server-kill
hyperdexexec_PROGRAMS += hyperdex-server-forget
hyperdexexec_PROGRAMS += hyperdex-perf-counters
hyperdexexec_PROGRAMS += hyperdex-set-read-only
hyperdexexec_PROGRAMS += hyperdex-set-read-write
hyperdexexec_PROGRAMS += hyperdex-set-fault-tolerance
hyperdexexec_PROGRAMS += hyperdex-wait-until-stable
hyperdexexec_PROGRAMS += hyperdex-backup
hyperdexexec_PROGRAMS += hyperdex-backup-manager
hyperdexexec_PROGRAMS += hyperdex-raw-backup
hyperdexexec_SCRIPTS += hyperdex-noc
dist_man_MANS += man/hyperdex-add-space.1
dist_man_MANS += man/hyperdex-rm-space.1
dist_man_MANS += man/hyperdex-mv-space.1
dist_man_MANS += man/hyperdex-list-spaces.1
dist_man_MANS += man/hyperdex-validate-space.1
dist_man_MANS += man/hyperdex-add-index.1
dist_man_MANS += man/hyperdex-rm-index.1
dist_man_MANS += man/hyperdex-show-config.1
dist_man_MANS += man/hyperdex-server-register.1
dist_man_MANS += man/hyperdex-server-online.1
dist_man_MANS += man/hyperdex-server-offline.1
dist_man_MANS += man/hyperdex-server-kill.1
dist_man_MANS += man/hyperdex-server-forget.1
dist_man_MANS += man/hyperdex-perf-counters.1
dist_man_MANS += man/hyperdex-set-read-only.1
dist_man_MANS += man/hyperdex-set-read-write.1
dist_man_MANS += man/hyperdex-set-fault-tolerance.1
dist_man_MANS += man/hyperdex-wait-until-stable.1
dist_man_MANS += man/hyperdex-backup.1
dist_man_MANS += man/hyperdex-backup-manager.1
dist_man_MANS += man/hyperdex-raw-backup.1
endif

# hyperdex
if ENABLE_HYPERDEX_STUB
bin_PROGRAMS += hyperdex
dist_man_MANS += man/hyperdex.1
endif
EXTRA_DIST += man/hyperdex.1.md
EXTRA_DIST += man/hyperdex.1.h2m
hyperdex_SOURCES = hyperdex.cc
hyperdex_CPPFLAGS = -DHYPERDEX_EXEC_DIR=\""$(hyperdexexecdir)\"" $(AM_CPPFLAGS) $(CPPFLAGS)
hyperdex_LDADD = -lpopt
man/hyperdex.1: man/hyperdex.1.h2m hyperdex.cc | hyperdex$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex$(EXEEXT)

# hyperdex-add-space
EXTRA_DIST += man/hyperdex-add-space.1.md
EXTRA_DIST += man/hyperdex-add-space.1.h2m
hyperdex_add_space_SOURCES = tools/add-space.cc
hyperdex_add_space_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-add-space.1: man/hyperdex-add-space.1.h2m tools/add-space.cc | hyperdex-add-space$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-add-space$(EXEEXT)

# hyperdex-rm-space
EXTRA_DIST += man/hyperdex-rm-space.1.md
EXTRA_DIST += man/hyperdex-rm-space.1.h2m
hyperdex_rm_space_SOURCES = tools/rm-space.cc
hyperdex_rm_space_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-rm-space.1: man/hyperdex-rm-space.1.h2m tools/rm-space.cc | hyperdex-rm-space$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-rm-space$(EXEEXT)

# hyperdex-mv-space
EXTRA_DIST += man/hyperdex-mv-space.1.md
EXTRA_DIST += man/hyperdex-mv-space.1.h2m
hyperdex_mv_space_SOURCES = tools/mv-space.cc
hyperdex_mv_space_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-mv-space.1: man/hyperdex-mv-space.1.h2m tools/mv-space.cc | hyperdex-mv-space$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-mv-space$(EXEEXT)

# hyperdex-list-spaces
EXTRA_DIST += man/hyperdex-list-spaces.1.md
EXTRA_DIST += man/hyperdex-list-spaces.1.h2m
hyperdex_list_spaces_SOURCES = tools/list-spaces.cc
hyperdex_list_spaces_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-list-spaces.1: man/hyperdex-list-spaces.1.h2m tools/list-spaces.cc | hyperdex-list-spaces$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-list-spaces$(EXEEXT)

# hyperdex-validate-space
EXTRA_DIST += man/hyperdex-validate-space.1.md
EXTRA_DIST += man/hyperdex-validate-space.1.h2m
hyperdex_validate_space_SOURCES = tools/validate-space.cc
hyperdex_validate_space_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-validate-space.1: man/hyperdex-validate-space.1.h2m tools/validate-space.cc | hyperdex-validate-space$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-validate-space$(EXEEXT)

# hyperdex-add-index
EXTRA_DIST += man/hyperdex-add-index.1.md
EXTRA_DIST += man/hyperdex-add-index.1.h2m
hyperdex_add_index_SOURCES = tools/add-index.cc
hyperdex_add_index_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-add-index.1: man/hyperdex-add-index.1.h2m tools/add-index.cc | hyperdex-add-index$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-add-index$(EXEEXT)

# hyperdex-rm-index
EXTRA_DIST += man/hyperdex-rm-index.1.md
EXTRA_DIST += man/hyperdex-rm-index.1.h2m
hyperdex_rm_index_SOURCES = tools/rm-index.cc
hyperdex_rm_index_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-rm-index.1: man/hyperdex-rm-index.1.h2m tools/rm-index.cc | hyperdex-rm-index$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-rm-index$(EXEEXT)

# hyperdex-show-config
EXTRA_DIST += man/hyperdex-show-config.1.md
EXTRA_DIST += man/hyperdex-show-config.1.h2m
hyperdex_show_config_SOURCES = tools/show-config.cc
hyperdex_show_config_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-show-config.1: man/hyperdex-show-config.1.h2m tools/show-config.cc | hyperdex-show-config$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-show-config$(EXEEXT)

# hyperdex-server-register
EXTRA_DIST += man/hyperdex-server-register.1.md
EXTRA_DIST += man/hyperdex-server-register.1.h2m
hyperdex_server_register_SOURCES = tools/server-register.cc
hyperdex_server_register_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-server-register.1: man/hyperdex-server-register.1.h2m tools/server-register.cc | hyperdex-server-register$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-server-register$(EXEEXT)

# hyperdex-server-online
EXTRA_DIST += man/hyperdex-server-online.1.md
EXTRA_DIST += man/hyperdex-server-online.1.h2m
hyperdex_server_online_SOURCES = tools/server-online.cc
hyperdex_server_online_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-server-online.1: man/hyperdex-server-online.1.h2m tools/server-online.cc | hyperdex-server-online$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-server-online$(EXEEXT)

# hyperdex-server-offline
EXTRA_DIST += man/hyperdex-server-offline.1.md
EXTRA_DIST += man/hyperdex-server-offline.1.h2m
hyperdex_server_offline_SOURCES = tools/server-offline.cc
hyperdex_server_offline_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-server-offline.1: man/hyperdex-server-offline.1.h2m tools/server-offline.cc | hyperdex-server-offline$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-server-offline$(EXEEXT)

# hyperdex-server-forget
EXTRA_DIST += man/hyperdex-server-forget.1.md
EXTRA_DIST += man/hyperdex-server-forget.1.h2m
hyperdex_server_forget_SOURCES = tools/server-forget.cc
hyperdex_server_forget_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-server-forget.1: man/hyperdex-server-forget.1.h2m tools/server-forget.cc | hyperdex-server-forget$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-server-forget$(EXEEXT)

# hyperdex-server-kill
EXTRA_DIST += man/hyperdex-server-kill.1.md
EXTRA_DIST += man/hyperdex-server-kill.1.h2m
hyperdex_server_kill_SOURCES = tools/server-kill.cc
hyperdex_server_kill_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-server-kill.1: man/hyperdex-server-kill.1.h2m tools/server-kill.cc | hyperdex-server-kill$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-server-kill$(EXEEXT)

# hyperdex-perf-counters
EXTRA_DIST += man/hyperdex-perf-counters.1.md
EXTRA_DIST += man/hyperdex-perf-counters.1.h2m
hyperdex_perf_counters_SOURCES = tools/perf-counters.cc
hyperdex_perf_counters_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-perf-counters.1: man/hyperdex-perf-counters.1.h2m tools/perf-counters.cc | hyperdex-perf-counters$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-perf-counters$(EXEEXT)

# hyperdex-set-read-only
EXTRA_DIST += man/hyperdex-set-read-only.1.md
EXTRA_DIST += man/hyperdex-set-read-only.1.h2m
hyperdex_set_read_only_SOURCES = tools/set-read-only.cc
hyperdex_set_read_only_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-set-read-only.1: man/hyperdex-set-read-only.1.h2m tools/set-read-only.cc | hyperdex-set-read-only$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-set-read-only$(EXEEXT)

# hyperdex-set-read-write
EXTRA_DIST += man/hyperdex-set-read-write.1.md
EXTRA_DIST += man/hyperdex-set-read-write.1.h2m
hyperdex_set_read_write_SOURCES = tools/set-read-write.cc
hyperdex_set_read_write_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-set-read-write.1: man/hyperdex-set-read-write.1.h2m tools/set-read-write.cc | hyperdex-set-read-write$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-set-read-write$(EXEEXT)

# hyperdex-set-fault-tolerance
EXTRA_DIST += man/hyperdex-set-fault-tolerance.1.md
EXTRA_DIST += man/hyperdex-set-fault-tolerance.1.h2m
hyperdex_set_fault_tolerance_SOURCES = tools/set-fault-tolerance.cc
hyperdex_set_fault_tolerance_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-set-fault-tolerance.1: man/hyperdex-set-fault-tolerance.1.h2m tools/set-fault-tolerance.cc | hyperdex-set-fault-tolerance$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-set-fault-tolerance$(EXEEXT)

# hyperdex-wait-until-stable
EXTRA_DIST += man/hyperdex-wait-until-stable.1.md
EXTRA_DIST += man/hyperdex-wait-until-stable.1.h2m
hyperdex_wait_until_stable_SOURCES = tools/wait-until-stable.cc
hyperdex_wait_until_stable_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-wait-until-stable.1: man/hyperdex-wait-until-stable.1.h2m tools/wait-until-stable.cc | hyperdex-wait-until-stable$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-wait-until-stable$(EXEEXT)

# hyperdex-backup
EXTRA_DIST += man/hyperdex-backup.1.md
EXTRA_DIST += man/hyperdex-backup.1.h2m
hyperdex_backup_SOURCES = tools/backup.cc
hyperdex_backup_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-backup.1: man/hyperdex-backup.1.h2m tools/backup.cc | hyperdex-backup$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-backup$(EXEEXT)

# hyperdex-backup-manager
EXTRA_DIST += man/hyperdex-backup-manager.1.md
EXTRA_DIST += man/hyperdex-backup-manager.1.h2m
hyperdex_backup_manager_SOURCES = tools/backup-manager.cc
hyperdex_backup_manager_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-backup-manager.1: man/hyperdex-backup-manager.1.h2m tools/backup-manager.cc | hyperdex-backup-manager$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-backup-manager$(EXEEXT)

# hyperdex-raw-backup
EXTRA_DIST += man/hyperdex-raw-backup.1.md
EXTRA_DIST += man/hyperdex-raw-backup.1.h2m
hyperdex_raw_backup_SOURCES = tools/raw-backup.cc
hyperdex_raw_backup_LDADD = libhyperdex-admin.la -lpopt
man/hyperdex-raw-backup.1: man/hyperdex-raw-backup.1.h2m tools/raw-backup.cc | hyperdex-raw-backup$(EXEEXT)
	$(help2man_verbose)help2man $(HELP2MAN_FLAGS) --section 1 --output $@ --include $< ${abs_top_builddir}/hyperdex-raw-backup$(EXEEXT)

# hyperdex-noc
EXTRA_DIST += hyperdex-noc

################################################################################
################################# Documentation ################################
################################################################################

EXTRA_DIST += noc/css/bootstrap.css
EXTRA_DIST += noc/css/bootstrap.min.css
EXTRA_DIST += noc/css/bootstrap-responsive.css
EXTRA_DIST += noc/css/bootstrap-responsive.min.css
EXTRA_DIST += noc/css/style.css
EXTRA_DIST += noc/img/glyphicons-halflings.png
EXTRA_DIST += noc/img/glyphicons-halflings-white.png
EXTRA_DIST += noc/img/hyperdex.png
EXTRA_DIST += noc/index.html
EXTRA_DIST += noc/js/noc.js
EXTRA_DIST += noc/lib/angular/angular.js
EXTRA_DIST += noc/lib/angular/angular.min.js
EXTRA_DIST += noc/lib/angular/angular-resource.js
EXTRA_DIST += noc/lib/angular/angular-resource.min.js
EXTRA_DIST += noc/lib/angular/ui-bootstrap.js
EXTRA_DIST += noc/lib/angular/ui-bootstrap.min.js
EXTRA_DIST += noc/lib/angular/version.txt
EXTRA_DIST += noc/partials/default_chart.html
EXTRA_DIST += noc/partials/edit_chart.html
EXTRA_DIST += noc/partials/servers.html

################################################################################
################################# Documentation ################################
################################################################################

EXTRA_DIST += $(wildcard doc/*.tex)
EXTRA_DIST += $(wildcard doc/*.xtx)
EXTRA_DIST += $(wildcard doc/*/*.tex)
EXTRA_DIST += $(wildcard doc/*/*/*.tex)
EXTRA_DIST += $(wildcard doc/*/*/*/*.tex)
EXTRA_DIST += $(wildcard doc/*/*/*/*/*.tex)
EXTRA_DIST += doc/Makefile
EXTRA_DIST += doc/latex.py
EXTRA_DIST += doc/api/c/hello-world.c
EXTRA_DIST += doc/api/c/iterate.c
EXTRA_DIST += doc/api/java/HelloWorldAsyncLoop.java
EXTRA_DIST += doc/api/java/HelloWorldAsyncWait.java
EXTRA_DIST += doc/api/java/HelloWorld.java
EXTRA_DIST += doc/api/node.js/hello-world.js
EXTRA_DIST += doc/api/node.js/window-pattern.js
EXTRA_DIST += doc/api/ruby/hello-world-async-loop.rb
EXTRA_DIST += doc/api/ruby/hello-world-async-wait.rb
EXTRA_DIST += doc/api/ruby/hello-world.rb
EXTRA_DIST += doc/install/centos-packages.sh
EXTRA_DIST += doc/install/debian7-packages.sh
EXTRA_DIST += doc/install/fedora-packages.sh
EXTRA_DIST += doc/install/linux-amd64.sh
EXTRA_DIST += doc/install/ubuntu12.04-packages.sh
EXTRA_DIST += doc/install/ubuntu14.04-packages.sh
EXTRA_DIST += doc/install/ubuntu14.10-packages.sh

################################################################################
################################## Maintenance #################################
################################################################################

if ENABLE_MAINTAINER_TOOLS
noinst_PROGRAMS += maint/lib-check
endif

EXTRA_DIST += bindings/__init__.py
EXTRA_DIST += maint/generate-bindings-tests.py
EXTRA_DIST += maint/generate-doc-shell-stubs.py
EXTRA_DIST += maint/generate-replication-shell-stubs.py
EXTRA_DIST += maint/generate-search-shell-stubs.py
EXTRA_DIST += maint/generate-test-packages.py
EXTRA_DIST += doc/maint/generate-api-descriptions.py

maint_lib_check_SOURCES = maint/lib-check.c
maint_lib_check_LDADD = -ldl

maintainer-generate:
	python2 bindings/c.py
	python2 bindings/java.py
	python2 bindings/nodejs.py
	python2 bindings/python.py
	python2 bindings/ruby.py
	python2 maint/generate-replication-shell-stubs.py
	python2 maint/generate-search-shell-stubs.py
	python2 maint/generate-doc-shell-stubs.py
	python2 maint/generate-bindings-tests.py
	python2 test/doc-extract.py python doc/quick-start.tex test/doc.quick-start.py
	python2 test/doc-extract.py python doc/data-types.tex test/doc.data-types.py
	python2 test/doc-extract.py python doc/async-ops.tex test/doc.async-ops.py
	python2 test/doc-extract.py python doc/atomic-ops.tex test/doc.atomic-ops.py
	python2 test/doc-extract.py python doc/documents.tex test/doc.documents.py
	python2 test/doc-extract.py python doc/authorization.tex test/doc.authorization.py
