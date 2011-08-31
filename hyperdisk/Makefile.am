SUBDIRS = . test

lib_LTLIBRARIES = libhyperdisk.la

nobase_include_HEADERS = hyperdisk/disk.h hyperdisk/returncode.h hyperdisk/snapshot.h
noinst_HEADERS = coordinate.h log_entry.h shard.h shard_constants.h shard_snapshot.h shard_vector.h

libhyperdisk_la_SOURCES = disk.cc shard.cc shard_snapshot.cc shard_vector.cc snapshot.cc
libhyperdisk_la_CPPFLAGS = -I$(abs_top_srcdir)/hyperdex -I$(abs_top_srcdir)/utils
libhyperdisk_la_LDFLAGS = -lcityhash
