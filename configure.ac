#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.66])
AC_INIT([HyperDex], [1.0.rc1], [escriva@cs.cornell.edu])
AM_INIT_AUTOMAKE([foreign subdir-objects dist-bzip2 dist-lzma])
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])
AM_PATH_PYTHON([2.6])
LT_PREREQ([2.2])
LT_INIT
AC_CONFIG_SRCDIR([hyperdex.h])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_PROG_LEX
AC_PROG_YACC
AC_LANG(C++)

ANAL_WARNINGS_CXX

AC_ARG_ENABLE([java-bindings], [AS_HELP_STRING([--enable-java-bindings],
              [enable the Java bindings @<:@default: no@:>@])],
              [java_bindings=${enableval}], [java_bindings=no])
need_swig=no

JNI_CPPFLAGS=
if test x"${java_bindings}" = xyes; then
    AC_MSG_ERROR([
-------------------------------------------------
The Java bindings are temporarily unavailable.
They will be available in HyperDex 1.0
-------------------------------------------------])
    AC_PROG_JAR
    AC_PROG_JAVAC
    AC_JNI_INCLUDE_DIR
    need_swig=yes
    found_jnidir=no
    for jnidir in ${JNI_INCLUDE_DIRS}
    do
        if test x"${found_jnidir}" = xno; then
            JNI_CPPFLAGS="-I${jnidir}"
            found_jnidir=yes
        fi
    done
    if test x"${found_jnidir}" = xno; then
        AC_MSG_ERROR([
-------------------------------------------------
HyperDex relies upon the JNI for Java bindings.
No JNI include dir could be found.
Set JAVAC to point to your Java compiler.
-------------------------------------------------])
    fi
fi
AC_SUBST([JNI_CPPFLAGS], [${JNI_CPPFLAGS}])

if test x"${need_swig}" = xyes; then
    AX_PKG_SWIG(1.3)
    SWIG_ENABLE_CXX
fi

# Checks for libraries.
GTEST_LIB_CHECK
AC_CHECK_LIB([popt], [poptGetContext], [], [AC_MSG_ERROR([
-------------------------------------------------
HyperDex relies upon the popt library.
Please install popt to continue.
-------------------------------------------------])])
PKG_CHECK_MODULES([PO6], [libpo6 >= 0.2.3])
PKG_CHECK_MODULES([E], [libe >= 0.2.7])
PKG_CHECK_MODULES([BUSYBEE], [busybee >= 0.1.0])

# Checks for header files.
AC_CHECK_HEADERS([netinet/in.h stdint.h stdlib.h sys/socket.h unistd.h])
AC_CHECK_HEADER([city.h],,[AC_MSG_ERROR([
-------------------------------------------------
HyperDex relies upon the cityhash library.
Please install cityhash to continue.
-------------------------------------------------])])
AC_CHECK_HEADER([glog/logging.h],,[AC_MSG_ERROR([
-------------------------------------------------
HyperDex relies upon the glog library.
Please install glog to continue.
-------------------------------------------------])])
AC_CHECK_HEADER([popt.h],,[AC_MSG_ERROR([
-------------------------------------------------
HyperDex relies upon the popt library.
Please install popt to continue.
-------------------------------------------------])])
AC_CHECK_HEADER([leveldb/db.h],,[AC_MSG_ERROR([
---------------------------------------
Replicant relies upon the LevelDB library.
Please install LevelDB to continue.
---------------------------------------])])

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
if test x"${swig_version}" = x; then
    if test x"${java_bindings}" = xyes; then
        AC_MSG_ERROR([
-------------------------------------------------
Cannot build Java bindings without SWIG
Java bindings will not be built.
-------------------------------------------------])
        java_bindings=no
    fi
fi
if test x"${enable_ycsb}" = xyes; then
    if test x"${java_bindings}" = xno; then
        AC_MSG_ERROR([
-------------------------------------------------
Cannot build YCSB without building Java bindings.
YCSB will not be built.
-------------------------------------------------])
        enable_ycsb=no
    fi
fi

AC_ARG_ENABLE([python_bindings], [AS_HELP_STRING([--enable-python-bindings],
              [build Python bindings @<:@default: no@:>@])],
              [python_bindings=${enableval}], [python_bindings=no])
if test x"${python_bindings}" = xyes; then
    AC_PYTHON_DEVEL([>= 2.6])
fi

AC_ARG_ENABLE([log-all-messages], [AS_HELP_STRING([--enable-log-all-messages],
              [enable code to log all messages @<:@default: no@:>@])],
              [enable_logall=${enableval}], [enable_logall=no])
if test x"${enable_logall}" = xyes; then
    AC_DEFINE([HD_LOG_ALL_MESSAGES], [], [Log all network traffic at the INFO level])
fi

AM_CONDITIONAL([ENABLE_JAVA_BINDINGS], [test x"${java_bindings}" = xyes])
AM_CONDITIONAL([ENABLE_PYTHON_BINDINGS], [test x"${python_bindings}" = xyes])
AM_CONDITIONAL([ENABLE_YCSB_DRIVER], [test x"${enable_ycsb}" = xyes])

AC_CONFIG_FILES([Makefile])
AC_OUTPUT
