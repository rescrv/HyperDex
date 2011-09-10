AM_CPPFLAGS = -I$(abs_top_srcdir)/hyperspacehashing -I$(abs_top_srcdir)/hyperdisk
AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)
AM_LDFLAGS = -no-install $(GTEST_LDFLAGS)

if HAVE_GTEST
check_PROGRAMS = log_entry shard
TESTS = $(check_PROGRAMS)

log_entry_SOURCES = ../../runner.cc log_entry.cc
log_entry_LDADD = ../libhyperdisk.la $(COVERAGE_LDADD) $(GTEST_LIBS)
shard_SOURCES = ../../runner.cc shard.cc
shard_LDADD = ../libhyperdisk.la $(COVERAGE_LDADD) $(GTEST_LIBS)
endif
