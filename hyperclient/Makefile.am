SUBDIRS = . java

lib_LTLIBRARIES = libhyperclient.la

EXTRA_DIST = hyperclient.i
nobase_include_HEADERS = hyperclient/async_client.h \
						 hyperclient/sync_client.h

libhyperclient_la_SOURCES = async_client.cc sync_client.cc
libhyperclient_la_CPPFLAGS = -I${abs_top_srcdir}/hyperdex -I${abs_top_srcdir}/utils
libhyperclient_la_LIBADD = ../hyperdex/libhyperdex.la
libhyperclient_la_LDFLAGS = -lcityhash
