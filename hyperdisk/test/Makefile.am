AM_CPPFLAGS = -I$(abs_top_srcdir)/hyperdisk
AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)
AM_LDFLAGS = -no-install $(GTEST_LDFLAGS)

if HAVE_GTEST
check_PROGRAMS = shard
TESTS = $(check_PROGRAMS)

shard_SOURCES = $(abs_top_srcdir)/runner.cc shard.cc
shard_LDADD = ../libhyperdisk.la $(COVERAGE_LDADD) $(GTEST_LIBS)
endif
