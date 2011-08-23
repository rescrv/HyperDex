noinst_LTLIBRARIES = libhyperdaemon.la

noinst_HEADERS = hyperdaemon/daemon.h datalayer.h logical.h network_worker.h \
				 physical.h replication/clientop.h replication/deferred.h \
				 replication/keyholder.h replication/keypair.h \
				 replication_manager.cc replication_manager.h \
				 replication/pending.h replication/transfer_in.h \
				 replication/transfer_out.h searches.h

libhyperdaemon_la_SOURCES = daemon.cc datalayer.cc logical.cc \
							network_worker.cc physical.cc \
							replication_manager.cc searches.cc
libhyperdaemon_la_CPPFLAGS = -I../hyperdex -I../hyperdisk
libhyperdaemon_la_LIBADD = ../hyperdex/libhyperdex.la
libhyperdaemon_la_LDFLAGS = -lcityhash
