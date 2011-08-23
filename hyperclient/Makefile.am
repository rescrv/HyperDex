lib_LTLIBRARIES = libhyperclient.la libhyperclient-java.la

BUILT_SOURCES = hyperclient_wrap.cxx
nobase_include_HEADERS = hyperclient/client.h

libhyperclient_la_SOURCES = client.cc
libhyperclient_la_CPPFLAGS = -I${abs_top_srcdir}/hyperdex
libhyperclient_la_LIBADD = ../hyperdex/libhyperdex.la
libhyperclient_la_LDFLAGS = -lcityhash

# Java bindings
libhyperclient_java_la_SOURCES = hyperclient_wrap.cxx
libhyperclient_java_la_LIBADD = libhyperclient.la
libhyperclient_java_la_LDFLAGS = -module
libhyperclient_java_la_CPPFLAGS = -I${abs_top_srcdir}/hyperdex

hyperclient_wrap.cxx: hyperclient.i hyperclient/client.h
	mkdir -p hyperclient
	${SWIG} -java -package hyperclient -outdir hyperclient hyperclient.i

JAVAROOT = .

jarfile = hyperclient.jar
jardir = $(datadir)/java

$(jarfile): hyperclient_wrap.cxx java.stamp
		$(JAR) cf $(JARFLAGS) $@ hyperclient/*.class

java.stamp: hyperclient_wrap.cxx hyperclient/*.java
	javac -cp . hyperclient/*.java
	echo 'Java bindings compiled' > java.stamp

jar_DATA = $(jarfile)

all-local: $(jarfile) java.stamp

CLEANFILES = hyperclient_wrap.cxx $(jarfile) \
			 hyperclient/*.java \
			 hyperclient/*.class \
			 java.stamp
