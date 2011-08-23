lib_LTLIBRARIES = libhyperclient.la

nobase_include_HEADERS = hyperclient/client.h

libhyperclient_la_SOURCES = client.cc
libhyperclient_la_CPPFLAGS = -I../hyperdex
libhyperclient_la_LIBADD = ../hyperdex/libhyperdex.la
libhyperclient_la_LDFLAGS = -lcityhash
