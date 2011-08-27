EXTRA_DIST = hyperclient/ycsb/HyperClient.java

if ENABLE_YCSB
JAVAROOT = .

jarfile = hyperclient.ycsb.jar
jardir = $(datadir)/java

$(jarfile): $(abs_builddir)/hyperclient/ycsb/HyperClient.class
	$(JAR) cf $(JARFLAGS) $@ $(abs_builddir)/hyperclient/ycsb/HyperClient.class

# XXX This is improper CLASSPATH tracking.
$(abs_builddir)/hyperclient/ycsb/HyperClient.class: $(abs_srcdir)/hyperclient/ycsb/HyperClient.java
	mkdir -p $(abs_builddir)/hyperclient/ycsb
	javac -cp $(CLASSPATH):../hyperclient/java/hyperclient.jar:. -d $(abs_builddir) $(abs_srcdir)/hyperclient/ycsb/HyperClient.java

jar_DATA = $(jarfile)
all-local: $(jarfile)

CLEANFILES = $(jarfile) $(abs_builddir)/hyperclient/ycsb/HyperClient.class
endif
