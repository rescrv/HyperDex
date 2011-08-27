SUBDIRS = . test

lib_LTLIBRARIES = libhyperdisk.la

nobase_include_HEADERS = hyperdisk/disk.h hyperdisk/returncode.h hyperdisk/snapshot.h
noinst_HEADERS = log.h shard.h shard_constants.h shard_snapshot.h

libhyperdisk_la_SOURCES = disk.cc log.cc shard.cc shard_snapshot.cc snapshot.cc
libhyperdisk_la_CPPFLAGS = -I$(abs_top_srcdir)/hyperdex -I$(abs_top_srcdir)/bithacks
libhyperdisk_la_LDFLAGS = -lcityhash
