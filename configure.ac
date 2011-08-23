#                                               -*- Autoconf -*-
# Process this file with autoconf to produce a configure script.

AC_PREREQ([2.66])
AC_INIT([HyperDex], [0.1.0], [escriva@cs.cornell.edu])
AM_INIT_AUTOMAKE([foreign dist-bzip2 dist-lzma])
LT_PREREQ([2.2])
LT_INIT
AC_CONFIG_SRCDIR([hyperdex/hyperdex/ids.h])
AC_CONFIG_HEADERS([config.h])
AC_CONFIG_MACRO_DIR([m4])

# Checks for programs.
AC_PROG_CXX
AC_PROG_CC
AC_PROG_JAR
AC_PROG_JAVAC
AX_PKG_SWIG
AX_SWIG_ENABLE_CXX
AX_SWIG_MULTI_MODULE_SUPPORT
AX_SWIG_PYTHON
AX_JNI_INCLUDE_DIR

for JNI_INCLUDE_DIR in $JNI_INCLUDE_DIRS
do
    CPPFLAGS="$CPPFLAGS -I$JNI_INCLUDE_DIR"
done

# Checks for libraries.
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

AC_CONFIG_FILES([Makefile hyperclient/Makefile hyperclient/java/Makefile
                 hyperdaemon/Makefile hyperdex/Makefile hyperdisk/Makefile])
AC_OUTPUT
