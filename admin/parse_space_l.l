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
#include <hyperdex/hyperspace_builder.h>
#include "admin/parse_space_y.h"
#ifdef _MSC_VER
#define strtoull(A, B, C) _strtoui64((A),(B),(C))
#endif

#define YY_USER_ACTION { \
        yylloc->first_line = yylineno; \
        yylloc->last_line = yylineno; \
        yylloc->first_column = yylloc->last_column; \
        columns(yytext, &yylloc->last_column); \
}

#pragma GCC diagnostic ignored "-Wredundant-decls"
#pragma GCC diagnostic ignored "-Wswitch-default"
#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wunused-parameter"

struct hyperdex_admin_space_keyword
{
    int token;
    const char* name;
};

extern struct hyperdex_admin_space_keyword hyperdex_admin_space_keywords[];
extern void
columns(const char* text, int* column);

%}

%option noyywrap
%option yylineno
%option reentrant
%option bison-bridge
%option bison-locations

%%

[a-zA-Z_][a-zA-Z_0-9]*  { int i = 0;
                          while (hyperdex_admin_space_keywords[i].name &&
                                 strcmp(hyperdex_admin_space_keywords[i].name, yytext) != 0)
                          {
                              ++i;
                          }
                          if (hyperdex_admin_space_keywords[i].name)
                          {
                              yylval->str = NULL;
                              return hyperdex_admin_space_keywords[i].token;
                          }
                          yylval->str = strdup(yytext); return IDENTIFIER; }
[0-9]*                  { yylval->num = strtoull(yytext, NULL, 10); return NUMBER; }
\#.*$                   ;
[ \t\r]                 ;
\n                      ;
.                       return (int) yytext[0];

%%

struct hyperdex_admin_space_keyword hyperdex_admin_space_keywords[] = {
    {SPACE, "space"},
    {KEY, "key"},
    {ATTRIBUTES, "attributes"},
    {ATTRIBUTES, "attribute"},
    {TOLERATE, "tolerate"},
    {FAILURES, "failures"},
    {FAILURES, "failure"},
    {CREATE, "create"},
    {PARTITIONS, "partitions"},
    {PARTITIONS, "partition"},
    {WITH, "with"},
    {AUTHORIZATION, "authorization"},
    {SUBSPACE, "subspace"},
    {INDEX, "index"},
    {STRING, "string"},
    {INT64, "int"},
    {INT64, "int64"},
    {FLOAT, "float"},
    {DOCUMENT, "document"},
    {TIMESTAMP, "timestamp"},
    {SECOND, "second"},
    {MINUTE, "minute"},
    {HOUR, "hour"},
    {DAY, "day"},
    {WEEK, "week"},
    {MONTH, "month"},
    {LIST, "list"},
    {SET, "set"},
    {MAP, "map"},
    {0, NULL}
};

void
columns(const char* text, int* column)
{
    int i;

    for (i = 0; text[i] != '\0'; i++)
    {
        if (text[i] == '\n')
        {
            *column = 1;
        }
        else if (text[i] == '\t')
        {
            *column += 4 - (*column % 4);
        }
        else
        {
            (*column)++;
        }
    }
}
