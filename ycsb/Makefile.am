JAVAROOT = .

EXTRA_DIST = hyperclient/ycsb/HyperClient.java

jarfile = hyperclient.ycsb.jar
jardir = $(datadir)/java

$(jarfile): $(abs_srcdir)/hyperclient/ycsb/HyperClient.class
	$(JAR) cf $(JARFLAGS) $@ $<

# XXX This is improper CLASSPATH tracking.
$(abs_srcdir)/hyperclient/ycsb/HyperClient.class: $(abs_srcdir)/hyperclient/ycsb/HyperClient.java
	javac -cp $(CLASSPATH):../hyperclient/java/hyperclient.jar:. $(abs_srcdir)/hyperclient/ycsb/HyperClient.java

if ENABLE_YCSB
jar_DATA = $(jarfile)
all-local: $(jarfile)
else
all-local:
endif

CLEANFILES = $(jarfile) hyperclient/ycsb/HyperClient.class
