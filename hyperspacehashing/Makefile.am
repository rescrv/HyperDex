AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)

noinst_LTLIBRARIES = libhyperspacehashing.la
noinst_HEADERS = hyperspacehashing/hashes.h \
				 hyperspacehashing/mask.h \
				 hyperspacehashing/prefix.h

libhyperspacehashing_la_SOURCES = hashes.cc mask.cc prefix.cc
libhyperspacehashing_la_LIBADD = $(COVERAGE_LDADD)
