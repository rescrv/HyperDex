AM_CPPFLAGS = -I$(abs_top_srcdir)/hyperspacehashing -I$(abs_top_srcdir)/hyperdisk
AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)

noinst_PROGRAMS = shard-fsck

shard_fsck_SOURCES = shard-fsck.cc
shard_fsck_LDADD = ../libhyperdisk.la $(COVERAGE_LDADD)
shard_fsck_LDFLAGS = -no-install
