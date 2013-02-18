%{
/* Copyright (c) 2012, Cornell University
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *     * Redistributions of source code must retain the above copyright notice,
 *       this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of HyperDex nor the names of its contributors may be
 *       used to endorse or promote products derived from this software without
 *       specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* C */
#include <stdint.h>

/* HyperDex */
#include "hyperdex.h"
#include "client/parse_space_y.h"
#ifdef _MSC_VER
#define sstrtoull(A, B, C) _strtoui64((A),(B),(C))
#endif

extern YYSTYPE yylval;
%}

%option noyywrap

%%

"space"                 { return SPACE; }
"key"                   { return KEY; }
"attributes"            { return ATTRIBUTES; }
"attribute"             { return ATTRIBUTES; }
"tolerate"              { return TOLERATE; }
"failures"              { return FAILURES; }
"failure"               { return FAILURES; }
"create"                { return CREATE; }
"partitions"            { return PARTITIONS; }
"partition"             { return PARTITIONS; }
"subspace"              { return SUBSPACE; }
":"                     { return COLON; }
","                     { return COMMA; }
"("                     { return OP; }
")"                     { return CP; }
"string"                { return STRING; }
"int"                   { return INT64; }
"int64"                 { return INT64; }
"float"                 { return FLOAT; }
"list"                  { return LIST; }
"set"                   { return SET; }
"map"                   { return MAP; }
[ \t\n\r]               ;
[0-9]*                  { yylval.num = strtoull(yytext, NULL, 10); return NUMBER; }
[a-zA-Z_][a-zA-Z_0-9]*  { yylval.str = strdup(yytext); return IDENTIFIER; }
