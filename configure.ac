#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.62])
AC_INIT([HyperDex], [1.8.dev], [robert@hyperdex.org])
m4_define([serial_tests], [
    m4_esyscmd([case `automake --version | head -n 1` in
                *1.11*);;
                *) echo serial-tests;;
                esac])
])
AM_INIT_AUTOMAKE(foreign serial_tests subdir-objects dist-bzip2)
m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])
AM_PATH_PYTHON([2.6])
LT_PREREQ([2.2])
LT_INIT
AC_CONFIG_SRCDIR([include/hyperdex.h])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Components that influence other checks
AC_ARG_ENABLE([all-components], [AS_HELP_STRING([--enable-all-components],
              [enable building all components @<:@default: yes@:>@])],
              [enable_all_components=${enable_all_components}], [enable_all_components=yes])
AC_ARG_ENABLE([client], [AS_HELP_STRING([--enable-client],
              [enable building the client library @<:@default: yes@:>@])],
              [enable_client=${enable_client}], [enable_client=${enable_all_components}])
AM_CONDITIONAL([ENABLE_CLIENT], [test x"${enable_client}" = xyes])
AC_ARG_ENABLE([admin], [AS_HELP_STRING([--enable-admin],
              [enable building the admin library @<:@default: yes@:>@])],
              [enable_admin=${enable_admin}], [enable_admin=${enable_all_components}])
AM_CONDITIONAL([ENABLE_ADMIN], [test x"${enable_admin}" = xyes])
AC_ARG_ENABLE([tools], [AS_HELP_STRING([--enable-tools],
              [enable building the command-line tools @<:@default: yes@:>@])],
              [enable_tools=${enable_tools}], [enable_tools=${enable_all_components}])
AM_CONDITIONAL([ENABLE_TOOLS], [test x"${enable_tools}" = xyes])
AC_ARG_ENABLE([coordinator], [AS_HELP_STRING([--enable-coordinator],
              [enable building the coordinator @<:@default: yes@:>@])],
              [enable_coordinator=${enable_coordinator}], [enable_coordinator=${enable_all_components}])
AM_CONDITIONAL([ENABLE_COORDINATOR], [test x"${enable_coordinator}" = xyes])
AC_ARG_ENABLE([daemon], [AS_HELP_STRING([--enable-daemon],
              [enable building the daemon @<:@default: yes@:>@])],
              [enable_daemon=${enable_daemon}], [enable_daemon=${enable_all_components}])
AM_CONDITIONAL([ENABLE_DAEMON], [test x"${enable_daemon}" = xyes])
AM_CONDITIONAL([ENABLE_HYPERDEX_STUB], [test x"${enable_daemon}" = xyes -o x"${enable_tools}" = xyes])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_PROG_LEX
AC_PROG_YACC
AC_LANG(C++)

ANAL_WARNINGS

AC_ARG_ENABLE([java-bindings], [AS_HELP_STRING([--enable-java-bindings],
              [enable the Java bindings @<:@default: no@:>@])],
              [java_bindings=${enableval}], [java_bindings=no])

JNI_CPPFLAGS=
if test x"${java_bindings}" = xyes; then
    AC_PROG_JAR
    AC_PROG_JAVAC
    AC_JNI_INCLUDE_DIR
    found_jnidir=no
    for jnidir in ${JNI_INCLUDE_DIRS}
    do
        if test x"${found_jnidir}" = xno; then
            JNI_CPPFLAGS="-I${jnidir} -I${jnidir}/linux"
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

# Checks for libraries.
if test x"${enable_all_components}" = xyes -o \
        x"${enable_tools}" = xyes -o \
        x"${enable_coordinator}" = xyes -o \
        x"${enable_daemon}" = xyes; then
    AC_CHECK_LIB([popt], [poptGetContext], [has_popt=true], [AC_MSG_ERROR([
-------------------------------------------------
HyperDex relies upon the popt library.
Please install popt to continue.
-------------------------------------------------])])
fi

if test x"${enable_all_components}" = xyes -o \
        x"${enable_client}" = xyes -o \
        x"${enable_daemon}" = xyes; then
    AC_CHECK_LIB([json], [json_object_get_double], [needs_json=yes], [needs_json=no])
    AC_CHECK_LIB([json-c], [json_object_get_double], [needs_json_c=yes], [needs_json_c=no])
fi

PKG_CHECK_MODULES([PO6], [libpo6 >= 0.8])
PKG_CHECK_MODULES([E], [libe >= 0.11])
PKG_CHECK_MODULES([BUSYBEE], [busybee >= 0.7])

if test x"${enable_all_components}" = xyes -o \
        x"${enable_daemon}" = xyes; then
    PKG_CHECK_MODULES([HYPERLEVELDB], [libhyperleveldb >= 1.2])
fi

PKG_CHECK_MODULES([REPLICANT], [replicant >= 0.8])
PKG_CHECK_MODULES([MACAROONS], [libmacaroons >= 0.3])
PKG_CHECK_MODULES([TREADSTONE], [libtreadstone >= 0.2])

# Checks for header files.
AC_CHECK_HEADERS([netinet/in.h stdint.h stdlib.h sys/socket.h unistd.h])

if test x"${enable_all_components}" = xyes -o \
        x"${enable_daemon}" = xyes; then
    AC_CHECK_HEADER([glog/logging.h],,[AC_MSG_ERROR([
-------------------------------------------------
HyperDex relies upon the glog library.
Please install glog to continue.
-------------------------------------------------])])
fi
AC_ARG_VAR(POPT_LIBS, [linker flags for popt])
AS_IF([test "x$POPT_LIBS" = x], [POPT_LIBS="-lpopt"])

if test x"${enable_all_components}" = xyes -o \
        x"${enable_tools}" = xyes -o \
        x"${enable_coordinator}" = xyes -o \
        x"${enable_daemon}" = xyes; then
    AC_CHECK_HEADER([popt.h],,[AC_MSG_ERROR([
-------------------------------------------------
HyperDex relies upon the popt library.
Please install popt to continue.
-------------------------------------------------])])
fi
AC_ARG_VAR(GLOG_LIBS, [linker flags for glog])
AS_IF([test "x$GLOG_LIBS" = x], [GLOG_LIBS="-lglog"])

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

AC_MSG_CHECKING([for builtin expect])
AC_COMPILE_IFELSE(
  [AC_LANG_PROGRAM(, [[return __builtin_expect(1, 1) ? 1 : 0;]])],
  [
    hyperdex_have_builtin_expect=yes
    AC_MSG_RESULT([yes])
], [
    hyperdex_have_builtin_expect=no
    AC_MSG_RESULT([no])
])
if test x"${hyperdex_have_builtin_expect}" = xyes ; then
    AC_DEFINE([HAVE_BUILTIN_EXPECT], [1], [Define to 1 if the compiler supports __builtin_expect.])
else
    AC_DEFINE([HAVE_BUILTIN_EXPECT], [0], [Define to 1 if the compiler supports __builtin_expect.])
fi

# Checks for library functions.
AC_FUNC_ERROR_AT_LINE
AC_FUNC_MMAP
AC_CHECK_FUNCS([alarm ftruncate memmove mkdir munmap rmdir socket])

# Optional components
AC_ARG_ENABLE([ycsb], [AS_HELP_STRING([--enable-ycsb],
              [enable the Yahoo Cloud Serving Benchmark @<:@default: no@:>@])],
              [enable_ycsb=${enableval}], [enable_ycsb=no])
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
    AC_PYTHON_DEVEL([>= '2.6'])
    AS_CASE([$PYTHON_VERSION], [3*], [pythonsym=PyInit_], [2*], [pythonsym=init], [])
    AC_SUBST([PYTHON_SYMBOL], [${pythonsym}])
fi

AC_ARG_ENABLE([ruby_bindings], [AS_HELP_STRING([--enable-ruby-bindings],
              [build Ruby bindings @<:@default: no@:>@])],
              [ruby_bindings=${enableval}], [ruby_bindings=no])
if test x"${ruby_bindings}" = xyes; then
    AX_RUBY_EXT
fi

AC_ARG_ENABLE([log-all-messages], [AS_HELP_STRING([--enable-log-all-messages],
              [enable code to log all messages @<:@default: no@:>@])],
              [enable_logall=${enableval}], [enable_logall=no])
if test x"${enable_logall}" = xyes; then
    AC_DEFINE([HD_LOG_ALL_MESSAGES], [], [Log all network traffic at the INFO level])
fi

AM_CONDITIONAL([ENABLE_JAVA_BINDINGS], [test x"${java_bindings}" = xyes])
AM_CONDITIONAL([ENABLE_PYTHON_BINDINGS], [test x"${python_bindings}" = xyes])
AM_CONDITIONAL([ENABLE_RUBY_BINDINGS], [test x"${ruby_bindings}" = xyes])
AM_CONDITIONAL([ENABLE_YCSB], [test x"${enable_ycsb}" = xyes])

# Optional components
AC_ARG_ENABLE([maintainer-tools], [AS_HELP_STRING([--enable-maintainer-tools],
              [enable maintainer tools @<:@default: no@:>@])],
              [enable_maintaine_tools=${enable_maintainer_tools}], [enable_maintaine_tools=no])
AM_CONDITIONAL([ENABLE_MAINTAINER_TOOLS], [test x"${enable_maintainer_tools}" = xyes])
AC_ARG_ENABLE([time-consuming-tests], [AS_HELP_STRING([--enable-time-consuming-tests],
              [enable tests that can take quite awhile to run @<:@default: yes@:>@])],
              [enable_time_consuming_tests=${enable_time_consuming_tests}], [enable_time_consuming_tests=yes])
AM_CONDITIONAL([ENABLE_TIME_CONSUMING_TESTS], [test x"${enable_time_consuming_tests}" = xyes])

# Generate outputs and finish
AC_CONFIG_FILES([Makefile hyperdex-client.pc hyperdex-admin.pc])
AC_CONFIG_LINKS([test/java/.exists:test/java/.exists])
AC_CONFIG_LINKS([bindings/python/hyperdex/__init__.py:bindings/python/hyperdex/__init__.py])
AC_CONFIG_LINKS([bindings/python/hyperdex/mongo.py:bindings/python/hyperdex/mongo.py])
AC_OUTPUT
