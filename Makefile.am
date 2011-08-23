ACLOCAL_AMFLAGS = -I m4
SUBDIRS = hyperdex hyperdisk hyperclient hyperdaemon

bin_PROGRAMS = cli daemon
noinst_bin_PROGRAMS = binary-test count-test repl-test
noinst_bindir = ${top_src_dir}

cli_SOURCES = cli.cc
cli_CPPFLAGS = -Ihyperclient/
cli_LDADD = hyperclient/libhyperclient.la
cli_LDFLAGS = -lpthread -lrt

daemon_SOURCES = daemon.cc
daemon_CPPFLAGS = -Ihyperdaemon/
daemon_LDADD = hyperdisk/libhyperdisk.la hyperdaemon/libhyperdaemon.la
daemon_LDFLAGS = -lpthread -lglog

binary_test_SOURCES = binary-test.cc
binary_test_CPPFLAGS = -Ihyperclient/
binary_test_LDADD = hyperclient/libhyperclient.la
binary_test_LDFLAGS = -lrt

count_test_SOURCES = count-test.cc
count_test_CPPFLAGS = -Ihyperclient/
count_test_LDADD = hyperclient/libhyperclient.la
count_test_LDFLAGS = -lrt

repl_test_SOURCES = repl-test.cc
repl_test_CPPFLAGS = -Ihyperclient/
repl_test_LDADD = hyperclient/libhyperclient.la
