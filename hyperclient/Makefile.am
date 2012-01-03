SUBDIRS = . java

lib_LTLIBRARIES = libhyperclient.la

EXTRA_DIST = hyperclient.i
nobase_include_HEADERS = hyperclient.h

libhyperclient_la_SOURCES = hyperclient.cc
libhyperclient_la_CPPFLAGS = -I${abs_top_srcdir}/hyperspacehashing -I${abs_top_srcdir}/hyperdex
libhyperclient_la_LIBADD = ../hyperspacehashing/libhyperspacehashing.la ../hyperdex/libhyperdex.la -lrt
libhyperclient_la_LDFLAGS = -lcityhash
