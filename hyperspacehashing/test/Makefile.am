AM_CPPFLAGS = -I$(abs_top_srcdir)/hyperspacehashing
AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)
AM_LDFLAGS = -no-install $(GTEST_LDFLAGS)

if HAVE_GTEST
check_PROGRAMS = bithacks cfloat
TESTS = $(check_PROGRAMS)

bithacks_SOURCES = ../../runner.cc bithacks.cc
bithacks_LDADD = ../libhyperspacehashing.la $(COVERAGE_LDADD) $(GTEST_LIBS)
cfloat_SOURCES = cfloat.cc
cfloat_LDADD = ../libhyperspacehashing.la $(COVERAGE_LDADD)
endif
