SUBDIRS = . test utils

AM_CPPFLAGS = -I$(abs_top_srcdir)/hyperspacehashing
AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)

lib_LTLIBRARIES = libhyperdisk.la

nobase_include_HEADERS = hyperdisk/disk.h hyperdisk/returncode.h hyperdisk/snapshot.h
noinst_HEADERS = log_entry.h offset_update.h shard.h shard_constants.h shard_snapshot.h shard_vector.h

libhyperdisk_la_SOURCES = disk.cc shard.cc shard_snapshot.cc shard_vector.cc snapshot.cc
libhyperdisk_la_LIBADD = ../hyperspacehashing/libhyperspacehashing.la -lpthread $(COVERAGE_LDADD)
