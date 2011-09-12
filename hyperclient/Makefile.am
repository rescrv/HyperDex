SUBDIRS = . java

lib_LTLIBRARIES = libhyperclient.la

EXTRA_DIST = hyperclient.i
nobase_include_HEADERS = hyperclient/client.h hyperclient/result.h hyperclient/returncode.h

libhyperclient_la_SOURCES = client.cc
libhyperclient_la_CPPFLAGS = -I${abs_top_srcdir}/hyperspacehashing -I${abs_top_srcdir}/hyperdex
libhyperclient_la_LIBADD = ../hyperspacehashing/libhyperspacehashing.la ../hyperdex/libhyperdex.la
libhyperclient_la_LDFLAGS = -lcityhash
