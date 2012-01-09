if ENABLE_JAVA_BINDINGS

AM_CPPFLAGS = ${JNI_CPPFLAGS}
AM_CXXFLAGS = -fno-strict-aliasing

BUILT_SOURCES = hyperclient_wrap.cxx

lib_LTLIBRARIES = libhyperclient-java.la

libhyperclient_java_la_SOURCES = syncclient.cc
nodist_libhyperclient_java_la_SOURCES = hyperclient_wrap.cxx

hyperclient_wrap.cxx: hyperclient.i ../hyperclient.h syncclient.h
	mkdir -p hyperclient
	${SWIG} -java -package hyperclient -outdir hyperclient -w518 hyperclient.i

JAVAROOT = $(abs_srcdir)
jarfile = $(PACKAGE)-$(VERSION).jar
jardir = $(datadir)/java
pkgpath = hyperclient

java.stamp: hyperclient_wrap.cxx
	javac hyperclient/*.java
	date > java.stamp

$(jarfile): java.stamp
	$(JAR) cvf $(JARFLAGS) $@ $(pkgpath)/*.class

jar_DATA = $(jarfile)

all-local: $(jarfile)

CLEANFILES = $(jarfile) hyperclient_wrap.cxx \
			 hyperclient/*.java \
			 hyperclient/*.class \
			 java.stamp

endif
