SUBDIRS = . test

AM_CPPFLAGS = -I$(abs_top_srcdir)/hyperspacehashing -I$(abs_top_srcdir)/utils -I$(abs_top_srcdir)/hyperdex
AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)
AM_LDFLAGS = $(GTEST_LDFLAGS)

lib_LTLIBRARIES = libhyperdisk.la

nobase_include_HEADERS = hyperdisk/disk.h hyperdisk/returncode.h hyperdisk/snapshot.h
noinst_HEADERS = coordinate.h log_entry.h shard.h shard_constants.h shard_snapshot.h shard_vector.h

libhyperdisk_la_SOURCES = disk.cc shard.cc shard_snapshot.cc shard_vector.cc snapshot.cc
libhyperdisk_la_LIBADD = -lcityhash $(COVERAGE_LDADD)
