ACLOCAL_AMFLAGS = -I m4
SUBDIRS = hyperspacehashing hyperdex hyperdisk hyperclient hyperdaemon ycsb

bin_PROGRAMS = daemon
noinst_PROGRAMS = count-test

EXTRA_DIST = LICENSE runner.cc

nobase_python_PYTHON = hdcoordinator/__init__.py hdcoordinator/coordinator.py hdcoordinator/parser.py

daemon_SOURCES = daemon.cc
daemon_CPPFLAGS = -I${abs_top_srcdir}/hyperdaemon/
daemon_LDADD = hyperdisk/libhyperdisk.la hyperdaemon/libhyperdaemon.la
daemon_LDFLAGS = -lpthread -lglog -lrt -lm -le -no-install

#binary_test_SOURCES = binary-test.cc
#binary_test_CPPFLAGS = -I${abs_top_srcdir}/hyperclient/
#binary_test_LDADD = hyperclient/libhyperclient.la
#binary_test_LDFLAGS = -lrt

count_test_SOURCES = count-test.cc
count_test_CPPFLAGS = -I${abs_top_srcdir}/hyperclient/
count_test_LDADD = hyperclient/libhyperclient.la
count_test_LDFLAGS = -lrt -le

#repl_test_SOURCES = repl-test.cc
#repl_test_CPPFLAGS = -I${abs_top_srcdir}/hyperclient/ -I${abs_top_srcdir}/hyperspacehashing/
#repl_test_LDADD = hyperclient/libhyperclient.la -lrt

coverage.dat: check
	lcov --directory $(abs_top_builddir) --compat-libtool --capture --output $(abs_top_builddir)/coverage.dat
	lcov --extract $(abs_top_builddir)/coverage.dat $(abs_top_builddir)/'*' --output $(abs_top_builddir)/coverage.dat

if ENABLE_COVERAGE
coverage: coverage.dat
	mkdir -p coverage
	genhtml --function-coverage --branch-coverage --demangle-cpp --output coverage $(abs_top_builddir)/coverage.dat
clean-coverage:
	rm -f coverage.dat
	rm -rf coverage/
	find $(abs_top_builddir) -type f -iname '*.gcda' -delete
clean-local: clean-coverage
	find $(abs_top_builddir) -type f -iname '*.gcno' -delete
else
coverage:
	@echo Coverage not configured.
endif

.PHONY: coverage
