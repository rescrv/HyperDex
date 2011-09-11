if ENABLE_JAVA
lib_LTLIBRARIES = libhyperclient-java.la

AM_CXXFLAGS = -fno-strict-aliasing

# XXX This should be put into the correct java path.  On my system it is
# /usr/lib/java.  If I'm going to change it, I want to auto-detect the correct
# system path for it.
nodist_libhyperclient_java_la_SOURCES = hyperclient_wrap.cxx
libhyperclient_java_la_CPPFLAGS = -I$(abs_top_srcdir)/hyperclient
libhyperclient_java_la_LIBADD = ../libhyperclient.la

hyperclient_wrap.cxx: $(abs_srcdir)/../hyperclient.i ../hyperclient/client.h
	mkdir -p hyperclient
	${SWIG} -java -package hyperclient -outcurrentdir -outdir hyperclient $(abs_srcdir)/../hyperclient.i

JAVAROOT = .

jarfile = hyperclient.jar
jardir = $(datadir)/java

$(jarfile): hyperclient_wrap.cxx java.stamp
	$(JAR) cf $(JARFLAGS) $@ hyperclient/*.class

java.stamp: hyperclient_wrap.cxx
	javac -cp . hyperclient/*.java
	echo 'Java bindings compiled' > java.stamp

jar_DATA = $(jarfile)
all-local: $(jarfile) java.stamp

CLEANFILES = hyperclient_wrap.cxx $(jarfile) \
			 hyperclient/*.java \
			 hyperclient/*.class \
			 java.stamp
endif
