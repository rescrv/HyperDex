AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)
AM_LDFLAGS = $(GTEST_LDFLAGS)

noinst_LTLIBRARIES = libhyperdex.la
noinst_HEADERS = hyperdex/configuration.h hyperdex/coordinatorlink.h \
				 hyperdex/hyperspace.h hyperdex/ids.h hyperdex/instance.h \
				 hyperdex/network_constants.h hyperdex/search.h

libhyperdex_la_SOURCES = configuration.cc coordinatorlink.cc search.cc
libhyperdex_la_LIBADD = $(COVERAGE_LDADD)
