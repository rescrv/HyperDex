AM_CPPFLAGS = -I$(abs_top_srcdir)/hyperspacehashing -I$(abs_top_srcdir)/hyperdisk
AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)

noinst_PROGRAMS = shard-dumphashes shard-fsck

shard_dumphashes_SOURCES = shard-dumphashes.cc
shard_dumphashes_LDADD = ../libhyperdisk.la $(COVERAGE_LDADD)
shard_dumphashes_LDFLAGS = -no-install
shard_fsck_SOURCES = shard-fsck.cc
shard_fsck_LDADD = ../libhyperdisk.la $(COVERAGE_LDADD)
shard_fsck_LDFLAGS = -no-install
