if ENABLE_YCSB_DRIVER

JAVAROOT = $(abs_srcdir)
jarfile = hyperclient-ycsb-$(VERSION).jar
jardir = $(datadir)/java
pkgpath = hyperclient/ycsb

dist_noinst_JAVA = hyperclient/ycsb/HyperClientDriver.java

$(jarfile): hyperclient/ycsb/HyperClientDriver.class
	$(JAR) cvf $(JARFLAGS) $@ $<

jar_DATA = $(jarfile)

all-local: $(jarfile)

CLEANFILES = $(jarfile) hyperclient/ycsb/HyperClientDriver.class

endif
