AM_CPPFLAGS = -I$(abs_top_srcdir)/hyperspacehashing -I$(abs_top_srcdir)/hyperdex -I$(abs_top_srcdir)/hyperdisk
AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)
AM_LDFLAGS = $(GTEST_LDFLAGS)

noinst_LTLIBRARIES = libhyperdaemon.la
noinst_HEADERS = hyperdaemon/daemon.h datalayer.h logical.h network_worker.h \
				 physical.h replication/clientop.h replication/deferred.h \
				 replication/keyholder.h replication/keypair.h \
				 replication_manager.cc replication_manager.h \
				 replication/pending.h replication/transfer_in.h \
				 replication/transfer_out.h runtimeconfig.h searches.h

libhyperdaemon_la_SOURCES = daemon.cc datalayer.cc logical.cc \
							network_worker.cc ongoing_state_transfers.cc \
							physical.cc replication_manager.cc \
							runtimeconfig.cc searches.cc
libhyperdaemon_la_LIBADD = ../hyperspacehashing/libhyperspacehashing.la ../hyperdex/libhyperdex.la -lcityhash $(COVERAGE_LDADD)
