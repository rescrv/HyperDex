AM_CPPFLAGS = -I$(abs_top_srcdir)/hyperspacehashing
AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)

noinst_PROGRAMS = cfloat

cfloat_SOURCES = cfloat.cc
cfloat_LDADD = ../libhyperspacehashing.la $(COVERAGE_LDADD)
cfloat_LDFLAGS = -no-install
