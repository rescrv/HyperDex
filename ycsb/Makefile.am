EXTRA_DIST = hyperclient/HyperClientYCSB.java

if ENABLE_YCSB_DRIVER

JAVAROOT = $(abs_srcdir)
jarfile = hyperclient-ycsb-$(VERSION).jar
jardir = $(datadir)/java
pkgpath = hyperclient/ycsb

hyperclient/HyperClientYCSB.class: hyperclient/HyperClientYCSB.java
	javac -cp ../hyperclient/java/hyperclient-$(VERSION).jar:$(CLASSPATH) hyperclient/HyperClientYCSB.java

$(jarfile): hyperclient/HyperClientYCSB.class
	$(JAR) cvf $(JARFLAGS) $@ hyperclient/HyperClientYCSB.class

jar_DATA = $(jarfile)

all-local: $(jarfile)

CLEANFILES = $(jarfile) hyperclient/HyperClientYCSB.class

endif
