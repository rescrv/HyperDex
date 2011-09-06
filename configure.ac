#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.66])
AC_INIT([HyperDex], [0.1.dev], [escriva@cs.cornell.edu])
AM_INIT_AUTOMAKE([foreign dist-bzip2 dist-lzma])
AM_SILENT_RULES([yes])
LT_PREREQ([2.2])
LT_INIT
AC_CONFIG_SRCDIR([hyperdex/hyperdex/ids.h])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_LANG(C++)

AC_ARG_ENABLE([java-bindings], [AS_HELP_STRING([--enable-java-bindings],
              [enable the Java bindings @<:@default: no@:>@])],
              [java_bindings=${enableval}], [java_bindings=no])
need_swig=no

if test x"${java_bindings}" = xyes; then
    AX_PROG_JAR
    AX_PROG_JAVAC
    AX_JNI_INCLUDE_DIR
    need_swig=yes

    for JNI_INCLUDE_DIR in $JNI_INCLUDE_DIRS
    do
        CPPFLAGS="$CPPFLAGS -I$JNI_INCLUDE_DIR"
    done
fi

if test x"${need_swig}" = xyes; then
    AX_PKG_SWIG
    AX_SWIG_ENABLE_CXX
    AX_SWIG_MULTI_MODULE_SUPPORT
fi

AC_ARG_ENABLE([coverage], [AS_HELP_STRING([--enable-coverage],
              [enable code coverage statistics @<:@default: no@:>@])],
              [enable_coverage=${enableval}], [enable_coverage=no])

COVERAGE_CXXFLAGS=
COVERAGE_LDADD=
if test x"${enable_coverage}" = xyes; then
    AC_CHECK_PROG(LCOV, lcov, lcov)
    if test x"${LCOV}" = xlcov; then
        COVERAGE_CXXFLAGS="-fprofile-arcs -ftest-coverage"
        COVERAGE_LDADD="-lgcov"
    else
        AC_MSG_WARN([Could not find lcov for test coverage])
        enable_coverage=no
    fi
fi
AC_SUBST([COVERAGE_CXXFLAGS], [${COVERAGE_CXXFLAGS}])
AC_SUBST([COVERAGE_LDADD], [${COVERAGE_LDADD}])

# Checks for libraries.
GTEST_LIB_CHECK

# Checks for header files.
AC_CHECK_HEADERS([netinet/in.h stdint.h stdlib.h sys/socket.h unistd.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_HEADER_STDBOOL
AC_C_INLINE
AC_TYPE_INT64_T
AC_TYPE_SIZE_T
AC_TYPE_SSIZE_T
AC_TYPE_UINT16_T
AC_TYPE_UINT32_T
AC_TYPE_UINT64_T
AC_TYPE_UINT8_T

# Checks for library functions.
AC_FUNC_ERROR_AT_LINE
AC_FUNC_MMAP
AC_CHECK_FUNCS([alarm clock_gettime ftruncate memmove mkdir munmap rmdir socket])

# Optional components
AC_ARG_ENABLE([ycsb], [AS_HELP_STRING([--enable-ycsb],
              [enable the Yahoo Cloud Serving Benchmark @<:@default: no@:>@])],
              [enable_ycsb=${enableval}], [enable_ycsb=no])
if test x"${enable_ycsb}" = xyes; then
    if test x"${java_bindings}" = xno; then
        AC_MSG_WARN([
-------------------------------------------------
Cannot build YCSB without building Java bindings.
YCSB will not be built.
-------------------------------------------------])
        enable_ycsb=no
    fi
fi
AM_CONDITIONAL([ENABLE_JAVA], [test x"${java_bindings}" = xyes])
AM_CONDITIONAL([ENABLE_YCSB], [test x"${enable_ycsb}" = xyes])
AM_CONDITIONAL([ENABLE_COVERAGE], [test x"${enable_coverage}" = xyes])

AC_CONFIG_FILES([Makefile hyperclient/Makefile hyperclient/java/Makefile
                 hyperdaemon/Makefile hyperdex/Makefile hyperdisk/Makefile
                 hyperdisk/test/Makefile ycsb/Makefile])
AC_OUTPUT
