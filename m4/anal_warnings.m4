# Copyright (c) 2012-2013, Robert Escriva
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
#     * Redistributions of source code must retain the above copyright notice,
#       this list of conditions and the following disclaimer.
#     * Redistributions in binary form must reproduce the above copyright
#       notice, this list of conditions and the following disclaimer in the
#       documentation and/or other materials provided with the distribution.
#     * Neither the name of this project nor the names of its contributors may
#       be used to endorse or promote products derived from this software
#       without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
#
# This macro enables many compiler warnings for C++ that generally catch bugs in
# code.  It offers the "--enable-wanal-flags" option which defaults to "no".

AC_DEFUN([ANAL_WARNINGS],
    [WANAL_CFLAGS=""
    WANAL_CXXFLAGS=""
    WANAL_CFLAGS_ONLY=""
    AC_ARG_ENABLE([wanal-flags],
              [AS_HELP_STRING([--enable-wanal-flags], [enable many warnings @<:@default: no@:>@])],
              [wanal_flags=${enableval}], [wanal_flags=no])
    if test x"${wanal_flags}" = xyes; then
        AX_CHECK_COMPILE_FLAG([-pedantic],[WANAL_CFLAGS="${WANAL_CFLAGS} -pedantic"],,)
        AX_CHECK_COMPILE_FLAG([-Wabi],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wabi"],,)
        AX_CHECK_COMPILE_FLAG([-Waddress],[WANAL_CFLAGS="${WANAL_CFLAGS} -Waddress"],,)
        AX_CHECK_COMPILE_FLAG([-Wall],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wall"],,)
        AX_CHECK_COMPILE_FLAG([-Warray-bounds],[WANAL_CFLAGS="${WANAL_CFLAGS} -Warray-bounds"],,)
        AX_CHECK_COMPILE_FLAG([-Wc++0x-compat],[WANAL_CXXFLAGS="${WANAL_CXXFLAGS} -Wc++0x-compat"],,)
        AX_CHECK_COMPILE_FLAG([-Wcast-align],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wcast-align"],,)
        AX_CHECK_COMPILE_FLAG([-Wcast-qual],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wcast-qual"],,)
        AX_CHECK_COMPILE_FLAG([-Wchar-subscripts],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wchar-subscripts"],,)
        AX_CHECK_COMPILE_FLAG([-Wclobbered],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wclobbered"],,)
        AX_CHECK_COMPILE_FLAG([-Wcomment],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wcomment"],,)
        #AX_CHECK_COMPILE_FLAG([-Wconversion],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wconversion"],,)
        AX_CHECK_COMPILE_FLAG([-Wctor-dtor-privacy],[WANAL_CXXFLAGS="${WANAL_CXXFLAGS} -Wctor-dtor-privacy"],,)
        AX_CHECK_COMPILE_FLAG([-Wdisabled-optimization],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wdisabled-optimization"],,)
        AX_CHECK_COMPILE_FLAG([-Weffc++],[WANAL_CXXFLAGS="${WANAL_CXXFLAGS} -Weffc++"],,)
        AX_CHECK_COMPILE_FLAG([-Wempty-body],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wempty-body"],,)
        AX_CHECK_COMPILE_FLAG([-Wenum-compare],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wenum-compare"],,)
        AX_CHECK_COMPILE_FLAG([-Wextra],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wextra"],,)
        AX_CHECK_COMPILE_FLAG([-Wfloat-equal],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wfloat-equal"],,)
        AX_CHECK_COMPILE_FLAG([-Wformat=2],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wformat=2"],,)
        AX_CHECK_COMPILE_FLAG([-Wformat-nonliteral],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wformat-nonliteral"],,)
        AX_CHECK_COMPILE_FLAG([-Wformat-security],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wformat-security"],,)
        AX_CHECK_COMPILE_FLAG([-Wformat],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wformat"],,)
        AX_CHECK_COMPILE_FLAG([-Wformat-y2k],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wformat-y2k"],,)
        AX_CHECK_COMPILE_FLAG([-Wignored-qualifiers],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wignored-qualifiers"],,)
        AX_CHECK_COMPILE_FLAG([-Wimplicit],[WANAL_CFLAGS_ONLY="${WANAL_CFLAGS} -Wimplicit"],,)
        AX_CHECK_COMPILE_FLAG([-Winit-self],[WANAL_CFLAGS="${WANAL_CFLAGS} -Winit-self"],,)
        AX_CHECK_COMPILE_FLAG([-Winline],[WANAL_CFLAGS="${WANAL_CFLAGS} -Winline"],,)
        AX_CHECK_COMPILE_FLAG([-Wlarger-than=4096],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wlarger-than=4096"],,)
        AX_CHECK_COMPILE_FLAG([-Wlogical-op],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wlogical-op"],,)
        AX_CHECK_COMPILE_FLAG([-Wmain],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wmain"],,)
        AX_CHECK_COMPILE_FLAG([-Wmissing-braces],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wmissing-braces"],,)
        AX_CHECK_COMPILE_FLAG([-Wmissing-field-initializers],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wmissing-field-initializers"],,)
        AX_CHECK_COMPILE_FLAG([-Wmissing-format-attribute],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wmissing-format-attribute"],,)
        AX_CHECK_COMPILE_FLAG([-Wmissing-include-dirs],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wmissing-include-dirs"],,)
        AX_CHECK_COMPILE_FLAG([-Wno-long-long],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wno-long-long"],,)
        AX_CHECK_COMPILE_FLAG([-Wnon-virtual-dtor],[WANAL_CXXFLAGS="${WANAL_CXXFLAGS} -Wnon-virtual-dtor"],,)
        AX_CHECK_COMPILE_FLAG([-Woverlength-strings],[WANAL_CFLAGS="${WANAL_CFLAGS} -Woverlength-strings"],,)
        AX_CHECK_COMPILE_FLAG([-Woverloaded-virtual],[WANAL_CXXFLAGS="${WANAL_CXXFLAGS} -Woverloaded-virtual"],,)
        AX_CHECK_COMPILE_FLAG([-Wpacked-bitfield-compat],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wpacked-bitfield-compat"],,)
        AX_CHECK_COMPILE_FLAG([-Wpacked],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wpacked"],,)
        #AX_CHECK_COMPILE_FLAG([-Wpadded],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wpadded"],,)
        AX_CHECK_COMPILE_FLAG([-Wparentheses],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wparentheses"],,)
        AX_CHECK_COMPILE_FLAG([-Wpointer-arith],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wpointer-arith"],,)
        AX_CHECK_COMPILE_FLAG([-Wredundant-decls],[WANAL_CXXFLAGS="${WANAL_CXXFLAGS} -Wredundant-decls"],,)
        AX_CHECK_COMPILE_FLAG([-Wreorder],[WANAL_CXXFLAGS="${WANAL_CXXFLAGS} -Wreorder"],,)
        AX_CHECK_COMPILE_FLAG([-Wreturn-type],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wreturn-type"],,)
        AX_CHECK_COMPILE_FLAG([-Wsequence-point],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wsequence-point"],,)
        AX_CHECK_COMPILE_FLAG([-Wshadow],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wshadow"],,)
        AX_CHECK_COMPILE_FLAG([-Wsign-compare],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wsign-compare"],,)
        #AX_CHECK_COMPILE_FLAG([-Wsign-conversion],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wsign-conversion"],,)
        AX_CHECK_COMPILE_FLAG([-Wsign-promo],[WANAL_CXXFLAGS="${WANAL_CXXFLAGS} -Wsign-promo"],,)
        AX_CHECK_COMPILE_FLAG([-Wstack-protector],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wstack-protector"],,)
        AX_CHECK_COMPILE_FLAG([-Wstrict-aliasing=3],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wstrict-aliasing=3"],,)
        AX_CHECK_COMPILE_FLAG([-Wstrict-aliasing],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wstrict-aliasing"],,)
        AX_CHECK_COMPILE_FLAG([-Wstrict-null-sentinel],[WANAL_CXXFLAGS="${WANAL_CXXFLAGS} -Wstrict-null-sentinel"],,)
        #AX_CHECK_COMPILE_FLAG([-Wstrict-overflow=4],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wstrict-overflow=4"],,)
        #AX_CHECK_COMPILE_FLAG([-Wstrict-overflow],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wstrict-overflow"],,)
        AX_CHECK_COMPILE_FLAG([-Wswitch-default],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wswitch-default"],,)
        AX_CHECK_COMPILE_FLAG([-Wswitch-enum],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wswitch-enum"],,)
        AX_CHECK_COMPILE_FLAG([-Wswitch],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wswitch"],,)
        AX_CHECK_COMPILE_FLAG([-Wtrigraphs],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wtrigraphs"],,)
        AX_CHECK_COMPILE_FLAG([-Wtype-limits],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wtype-limits"],,)
        AX_CHECK_COMPILE_FLAG([-Wundef],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wundef"],,)
        AX_CHECK_COMPILE_FLAG([-Wuninitialized],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wuninitialized"],,)
        AX_CHECK_COMPILE_FLAG([-Wunsafe-loop-optimizations],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wunsafe-loop-optimizations"],,)
        AX_CHECK_COMPILE_FLAG([-Wunused-function],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wunused-function"],,)
        AX_CHECK_COMPILE_FLAG([-Wunused-label],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wunused-label"],,)
        AX_CHECK_COMPILE_FLAG([-Wunused-parameter],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wunused-parameter"],,)
        AX_CHECK_COMPILE_FLAG([-Wunused-value],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wunused-value"],,)
        AX_CHECK_COMPILE_FLAG([-Wunused-variable],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wunused-variable"],,)
        AX_CHECK_COMPILE_FLAG([-Wunused],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wunused"],,)
        AX_CHECK_COMPILE_FLAG([-Wvolatile-register-var],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wvolatile-register-var"],,)
        AX_CHECK_COMPILE_FLAG([-Wwrite-strings],[WANAL_CFLAGS="${WANAL_CFLAGS} -Wwrite-strings"],,)
        AX_CHECK_COMPILE_FLAG([-Qunused-arguments],[WANAL_CFLAGS="${WANAL_CFLAGS} -Qunused-arguments"],,)
    fi
    WANAL_CXXFLAGS="${WANAL_CFLAGS} ${WANAL_CXXFLAGS}"
    WANAL_CFLAGS="${WANAL_CFLAGS} ${WANAL_CFLAGS_ONLY}"
    AC_SUBST([WANAL_CFLAGS], [${WANAL_CFLAGS}])
    AC_SUBST([WANAL_CXXFLAGS], [${WANAL_CXXFLAGS}])
])
