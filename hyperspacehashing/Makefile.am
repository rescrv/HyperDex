AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)

lib_LTLIBRARIES = libhyperspacehashing.la
nobase_include_HEADERS = hyperspacehashing/equality_wildcard.h \
						 hyperspacehashing/hashes.h \
						 hyperspacehashing/mask.h \
						 hyperspacehashing/prefix.h
noinst_HEADERS = bithacks.h

libhyperspacehashing_la_SOURCES = equality_wildcard.cc hashes.cc mask.cc prefix.cc
libhyperspacehashing_la_LIBADD = -lcityhash $(COVERAGE_LDADD)
