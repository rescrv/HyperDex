SUBDIRS = . test

AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)

lib_LTLIBRARIES = libhyperspacehashing.la
nobase_include_HEADERS = hyperspacehashing/hashes.h \
						 hyperspacehashing/mask.h \
						 hyperspacehashing/prefix.h \
						 hyperspacehashing/search.h
noinst_HEADERS = bithacks.h cfloat.h range_match.h

libhyperspacehashing_la_SOURCES = cfloat.cc hashes.cc mask.cc prefix.cc range_match.cc search.cc
libhyperspacehashing_la_LIBADD = -lcityhash $(COVERAGE_LDADD)
