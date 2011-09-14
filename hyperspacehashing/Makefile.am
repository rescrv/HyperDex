SUBDIRS = . test

AM_CXXFLAGS = $(COVERAGE_CXXFLAGS)

lib_LTLIBRARIES = libhyperspacehashing.la
nobase_include_HEADERS = hyperspacehashing/hashes.h \
						 hyperspacehashing/mask.h \
						 hyperspacehashing/prefix.h
noinst_HEADERS = bithacks.h

libhyperspacehashing_la_SOURCES = hashes.cc mask.cc prefix.cc search.cc
libhyperspacehashing_la_LIBADD = -lcityhash $(COVERAGE_LDADD)
