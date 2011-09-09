AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)

noinst_LTLIBRARIES = libhyperspacehashing.la
noinst_HEADERS = hyperspacehashing/equality_wildcard.h \
				 hyperspacehashing/hashes.h \
				 hyperspacehashing/mask.h \
				 hyperspacehashing/prefix.h

libhyperspacehashing_la_SOURCES = equality_wildcard.cc hashes.cc mask.cc prefix.cc
libhyperspacehashing_la_LIBADD = $(COVERAGE_LDADD)
