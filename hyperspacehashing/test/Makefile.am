AM_CPPFLAGS = -I$(abs_top_srcdir)/hyperspacehashing
AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)
AM_LDFLAGS = -no-install $(GTEST_LDFLAGS)

if HAVE_GTEST
check_PROGRAMS = bithacks cfloat mask prefix search
TESTS = $(check_PROGRAMS)
# XXX This needs to be fixed because cfloat changed.
XFAIL_TESTS = prefix

bithacks_SOURCES = ../../runner.cc bithacks.cc
bithacks_LDADD = ../libhyperspacehashing.la $(COVERAGE_LDADD) $(GTEST_LIBS)
cfloat_SOURCES = cfloat.cc
cfloat_LDADD = ../libhyperspacehashing.la $(COVERAGE_LDADD)
mask_SOURCES = ../../runner.cc mask.cc
mask_LDADD = ../libhyperspacehashing.la $(COVERAGE_LDADD) $(GTEST_LIBS)
prefix_SOURCES = ../../runner.cc prefix.cc
prefix_LDADD = ../libhyperspacehashing.la $(COVERAGE_LDADD) $(GTEST_LIBS)
search_SOURCES = search.cc
search_LDADD = ../libhyperspacehashing.la $(COVERAGE_LDADD)
endif
