EXTRA_DIST = hyperclient/HyperClientYCSB.java

if ENABLE_YCSB_DRIVER

JAVAROOT = $(abs_srcdir)
jarfile = hyperclient-ycsb-$(VERSION).jar
jardir = $(datadir)/java
pkgpath = hyperclient/ycsb

java.stamp: hyperclient/HyperClientYCSB.java
	javac hyperclient/HyperClientYCSB.java
	date > java.stamp

$(jarfile): java.stamp
	$(JAR) cvf $(JARFLAGS) $@ hyperclient/HyperClientYCSB.class

jar_DATA = $(jarfile)

all-local: $(jarfile)

CLEANFILES = $(jarfile) hyperclient/HyperClientYCSB.class

endif
