ACLOCAL_AMFLAGS = -I m4
SUBDIRS = hyperdex hyperdisk hyperclient hyperdaemon ycsb

bin_PROGRAMS = cli daemon
noinst_PROGRAMS = binary-test count-test repl-test
noinst_HEADERS = bithacks/bithacks.h

cli_SOURCES = cli.cc
cli_CPPFLAGS = -I${abs_top_srcdir}/hyperclient/
cli_LDADD = hyperclient/libhyperclient.la
cli_LDFLAGS = -lpthread -lrt

daemon_SOURCES = daemon.cc
daemon_CPPFLAGS = -I${abs_top_srcdir}/hyperdaemon/
daemon_LDADD = hyperdisk/libhyperdisk.la hyperdaemon/libhyperdaemon.la
daemon_LDFLAGS = -lpthread -lglog

binary_test_SOURCES = binary-test.cc
binary_test_CPPFLAGS = -I${abs_top_srcdir}/hyperclient/
binary_test_LDADD = hyperclient/libhyperclient.la
binary_test_LDFLAGS = -lrt

count_test_SOURCES = count-test.cc
count_test_CPPFLAGS = -I${abs_top_srcdir}/hyperclient/
count_test_LDADD = hyperclient/libhyperclient.la
count_test_LDFLAGS = -lrt

repl_test_SOURCES = repl-test.cc
repl_test_CPPFLAGS = -I${abs_top_srcdir}/hyperclient/
repl_test_LDADD = hyperclient/libhyperclient.la
