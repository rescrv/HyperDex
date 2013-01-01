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
#include <stdlib.h>

/* HyperDex */
#include "hyperdex.h"
#include "client/parse_space_aux.h"

void yyerror(char* s);
%}

%union {
    char* str;
    struct hyperparse_space* space;
    struct hyperparse_attribute* attr;
    struct hyperparse_attribute_list* attrs;
    struct hyperparse_subspace* subspace;
    struct hyperparse_subspace_list* subspaces;
    struct hyperparse_identifier_list* identifiers;
    enum hyperdatatype type;
}

%token SPACE
%token KEY
%token ATTRIBUTES
%token SUBSPACE
%token COLON
%token COMMA
%token OP
%token CP
%token STRING
%token INT64
%token FLOAT
%token LIST
%token SET
%token MAP
%token <str> IDENTIFIER

%type <space> space
%type <attrs> attribute_list
%type <attr> attribute
%type <type> type
%type <subspaces> subspace_list
%type <subspace> subspace
%type <identifiers> identifier_list;

%%

space : SPACE IDENTIFIER KEY attribute ATTRIBUTES attribute_list subspace_list { hyperparsed_space = hyperparse_create_space($2, $4, $6, $7); };

subspace_list :                                 { $$ = NULL; }
              | subspace_list SUBSPACE subspace { $$ = hyperparse_create_subspace_list($3, $1); };

subspace : identifier_list { $$ = hyperparse_create_subspace($1); } /*XXX*/

attribute_list : attribute                      { $$ = hyperparse_create_attribute_list($1, NULL); }
               | attribute_list COMMA attribute   { $$ = hyperparse_create_attribute_list($3, $1); };

attribute : IDENTIFIER { $$ = hyperparse_create_attribute($1, HYPERDATATYPE_STRING); }
          | type IDENTIFIER { $$ = hyperparse_create_attribute($2, $1); };

identifier_list : IDENTIFIER                        { $$ = hyperparse_create_identifier_list($1, NULL); }
                | identifier_list COMMA IDENTIFIER    { $$ = hyperparse_create_identifier_list($3, $1); };

type : STRING                        { $$ = HYPERDATATYPE_STRING; }
     | INT64                         { $$ = HYPERDATATYPE_INT64; }
     | FLOAT                         { $$ = HYPERDATATYPE_FLOAT; }
     | LIST OP STRING CP             { $$ = HYPERDATATYPE_LIST_STRING; }
     | LIST OP INT64 CP              { $$ = HYPERDATATYPE_LIST_INT64; }
     | LIST OP FLOAT CP              { $$ = HYPERDATATYPE_LIST_FLOAT; }
     | SET OP STRING CP              { $$ = HYPERDATATYPE_SET_STRING; }
     | SET OP INT64 CP               { $$ = HYPERDATATYPE_SET_INT64; }
     | SET OP FLOAT CP               { $$ = HYPERDATATYPE_SET_FLOAT; }
     | MAP OP STRING COMMA STRING CP { $$ = HYPERDATATYPE_MAP_STRING_STRING; }
     | MAP OP STRING COMMA INT64 CP  { $$ = HYPERDATATYPE_MAP_STRING_INT64; }
     | MAP OP STRING COMMA FLOAT CP  { $$ = HYPERDATATYPE_MAP_STRING_FLOAT; }
     | MAP OP INT64 COMMA STRING CP  { $$ = HYPERDATATYPE_MAP_INT64_STRING; }
     | MAP OP INT64 COMMA INT64 CP   { $$ = HYPERDATATYPE_MAP_INT64_INT64; }
     | MAP OP INT64 COMMA FLOAT CP   { $$ = HYPERDATATYPE_MAP_INT64_FLOAT; }
     | MAP OP FLOAT COMMA STRING CP  { $$ = HYPERDATATYPE_MAP_FLOAT_STRING; }
     | MAP OP FLOAT COMMA INT64 CP   { $$ = HYPERDATATYPE_MAP_FLOAT_INT64; }
     | MAP OP FLOAT COMMA FLOAT CP   { $$ = HYPERDATATYPE_MAP_FLOAT_FLOAT; };

%%

void
yyerror(char* s)
{
    hyperparsed_error = 1;
}
