if HAVE_GTEST
check_PROGRAMS = hyperdisk-test
TESTS = $(check_PROGRAMS)

hyperdisk_test_SOURCES = ../../runner.cc log.cc shard.cc
hyperdisk_test_CPPFLAGS = -I$(abs_top_srcdir)/hyperdisk
hyperdisk_test_LDADD = ../libhyperdisk.la $(GTEST_LIBS)
hyperdisk_test_LDFLAGS = -no-install
endif
