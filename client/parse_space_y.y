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
#include <stdio.h>
#include <stdlib.h>

/* HyperDex */
#include "client/hyperspace_builder.h"
#include "client/hyperspace_builder_internal.h"

typedef struct YYLTYPE
{
  int first_line;
  int first_column;
  int last_line;
  int last_column;
} YYLTYPE;
#define YYLTYPE_IS_DECLARED 1
#define YYLTYPE_IS_TRIVIAL 1

struct hyperspace;

extern struct yy_buffer_state*
yy_scan_string(const char *bytes, void* yyscanner);
extern void
yyerror(YYLTYPE* yylloc, struct hyperspace* space, void* scanner, char* msg);

%}

%locations
%pure-parser
%lex-param {void* scanner}
%parse-param {struct hyperspace* space}
%parse-param {void* scanner}

%token SPACE
%token KEY
%token ATTRIBUTES
%token TOLERATE
%token FAILURES
%token CREATE
%token PARTITIONS
%token SUBSPACE
%token PINDEX
%token SINDEX

%token <str> IDENTIFIER
%token <num> NUMBER
%token STRING
%token INT64
%token FLOAT
%token LIST
%token SET
%token MAP

%type <type> type
%type <attr> attribute
%type <ret> attribute_list
%type <str> pindex
%type <str> sindex

%union
{
    char* str;
    uint64_t num;
    enum hyperdatatype type;
    enum hyperspace_returncode ret;
    struct
    {
        char* name;
        enum hyperdatatype type;
    } attr;
}

%%

space : SPACE name key ATTRIBUTES attribute_list pindices subspaces options

name : IDENTIFIER { hyperspace_set_name(space, $1); free($1); }

key : KEY attribute { hyperspace_set_key(space, $2.name, $2.type); free($2.name); }

attribute_list : attribute                    { hyperspace_add_attribute(space, $1.name, $1.type); free($1.name); }
               | attribute_list ',' attribute { hyperspace_add_attribute(space, $3.name, $3.type); free($3.name); }

attribute : IDENTIFIER      { $$.name = $1; $$.type = HYPERDATATYPE_STRING; }
          | type IDENTIFIER { $$.name = $2; $$.type = $1; }

pindices :
         | PINDEX pindex

pindex : IDENTIFIER            { hyperspace_primary_index(space, $1); free($1); }
       | pindex ',' IDENTIFIER { hyperspace_primary_index(space, $3); free($3); }

subspaces :
          | subspaces subspace

subspace : SUBSPACE sattrs sindices

sattrs : IDENTIFIER            { hyperspace_add_subspace(space); hyperspace_add_subspace_attribute(space, $1); free($1); }
       | sattrs ',' IDENTIFIER { hyperspace_add_subspace_attribute(space, $3); free($3); }

sindices :
         | SINDEX sindex

sindex : IDENTIFIER            { hyperspace_add_secondary_index(space, $1); free($1); }
       | sindex ',' IDENTIFIER { hyperspace_add_secondary_index(space, $3); free($3); }

options :                { }
        | options option { }

option : TOLERATE NUMBER FAILURES { hyperspace_set_fault_tolerance(space, $2); }
       | CREATE NUMBER PARTITIONS { hyperspace_set_number_of_partitions(space, $2); }

type : STRING                        { $$ = HYPERDATATYPE_STRING; }
     | INT64                         { $$ = HYPERDATATYPE_INT64; }
     | FLOAT                         { $$ = HYPERDATATYPE_FLOAT; }
     | LIST '(' STRING ')'           { $$ = HYPERDATATYPE_LIST_STRING; }
     | LIST '(' INT64 ')'            { $$ = HYPERDATATYPE_LIST_INT64; }
     | LIST '(' FLOAT ')'            { $$ = HYPERDATATYPE_LIST_FLOAT; }
     | SET '(' STRING ')'            { $$ = HYPERDATATYPE_SET_STRING; }
     | SET '(' INT64 ')'             { $$ = HYPERDATATYPE_SET_INT64; }
     | SET '(' FLOAT ')'             { $$ = HYPERDATATYPE_SET_FLOAT; }
     | MAP '(' STRING ',' STRING ')' { $$ = HYPERDATATYPE_MAP_STRING_STRING; }
     | MAP '(' STRING ',' INT64 ')'  { $$ = HYPERDATATYPE_MAP_STRING_INT64; }
     | MAP '(' STRING ',' FLOAT ')'  { $$ = HYPERDATATYPE_MAP_STRING_FLOAT; }
     | MAP '(' INT64 ',' STRING ')'  { $$ = HYPERDATATYPE_MAP_INT64_STRING; }
     | MAP '(' INT64 ',' INT64 ')'   { $$ = HYPERDATATYPE_MAP_INT64_INT64; }
     | MAP '(' INT64 ',' FLOAT ')'   { $$ = HYPERDATATYPE_MAP_INT64_FLOAT; }
     | MAP '(' FLOAT ',' STRING ')'  { $$ = HYPERDATATYPE_MAP_FLOAT_STRING; }
     | MAP '(' FLOAT ',' INT64 ')'   { $$ = HYPERDATATYPE_MAP_FLOAT_INT64; }
     | MAP '(' FLOAT ',' FLOAT ')'   { $$ = HYPERDATATYPE_MAP_FLOAT_FLOAT; };

%%

void
yyerror(YYLTYPE* yylloc, struct hyperspace* space, void* scanner, char* msg)
{
    char* buffer = hyperspace_buffer(space);
    size_t buffer_sz = hyperspace_buffer_sz(space);
    snprintf(buffer, buffer_sz, "%s at line %d column %d", msg, yylloc->first_line, yylloc->first_column);
    buffer[buffer_sz - 1] = '\0';
    hyperspace_set_error(space, buffer);
}
