AM_CPPFLAGS = -I$(abs_top_srcdir)/hyperdisk
AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)
AM_LDFLAGS = -no-install $(GTEST_LDFLAGS)

if HAVE_GTEST
check_PROGRAMS = coordinate shard
TESTS = $(check_PROGRAMS)

coordinate_SOURCES = $(abs_top_srcdir)/runner.cc coordinate.cc
coordinate_LDADD = ../libhyperdisk.la $(COVERAGE_LDADD) $(GTEST_LIBS)
shard_SOURCES = $(abs_top_srcdir)/runner.cc shard.cc
shard_LDADD = ../libhyperdisk.la $(COVERAGE_LDADD) $(GTEST_LIBS)
endif
