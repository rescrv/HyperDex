AM_CPPFLAGS = -I$(abs_top_srcdir)/hyperspacehashing
AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)

noinst_PROGRAMS = copint

copint_SOURCES = copint.cc
copint_LDADD = ../libhyperspacehashing.la $(COVERAGE_LDADD)
copint_LDFLAGS = -no-install
