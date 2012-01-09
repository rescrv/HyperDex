if ENABLE_YCSB_DRIVER

JAVAROOT = $(abs_srcdir)
jarfile = hyperclient-ycsb-$(VERSION).jar
jardir = $(datadir)/java
pkgpath = hyperclient/ycsb

java.stamp: hyperclient/ycsb/HyperClientDriver.java
	javac hyperclient/ycsb/HyperClientDriver.java
	date > java.stamp

$(jarfile): java.stamp
	$(JAR) cvf $(JARFLAGS) $@ $<

jar_DATA = $(jarfile)

all-local: $(jarfile)

CLEANFILES = $(jarfile) hyperclient/ycsb/HyperClientDriver.class

endif
