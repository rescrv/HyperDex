EXTRA_DIST = hyperclient/ycsb/HyperClient.java

if ENABLE_YCSB
JAVAROOT = .

jarfile = hyperclient.ycsb.jar
jardir = $(datadir)/java

$(jarfile): hyperclient/ycsb/HyperClient.class
	$(JAR) cf $(JARFLAGS) $@ $<

# XXX This is improper CLASSPATH tracking.
hyperclient/ycsb/HyperClient.class: hyperclient/ycsb/HyperClient.java
	javac -cp $(CLASSPATH):../hyperclient/java/hyperclient.jar:. hyperclient/ycsb/HyperClient.java

jar_DATA = $(jarfile)
all-local: $(jarfile)

CLEANFILES = $(jarfile) hyperclient/ycsb/HyperClient.class
endif
