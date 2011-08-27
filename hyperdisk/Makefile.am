SUBDIRS = . test

lib_LTLIBRARIES = libhyperdisk.la

nobase_include_HEADERS = hyperdisk/disk.h hyperdisk/returncode.h
noinst_HEADERS = log.h shard.h shard_snapshot.h

libhyperdisk_la_SOURCES = disk.cc log.cc shard.cc shard_snapshot.cc
libhyperdisk_la_CPPFLAGS = -I${abs_top_srcdir}/hyperdex
libhyperdisk_la_LDFLAGS = -lcityhash
