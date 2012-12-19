// Copyright (c) 2012, Cornell University
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are met:
//
//     * Redistributions of source code must retain the above copyright notice,
//       this list of conditions and the following disclaimer.
//     * Redistributions in binary form must reproduce the above copyright
//       notice, this list of conditions and the following disclaimer in the
//       documentation and/or other materials provided with the distribution.
//     * Neither the name of HyperDex nor the names of its contributors may be
//       used to endorse or promote products derived from this software without
//       specific prior written permission.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
// AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
// IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
// ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
// LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
// CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
// SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
// INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
// CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
// ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
// POSSIBILITY OF SUCH DAMAGE.

// C
#include <cstdlib>

// HyperDex
#include "client/parse_space_aux.h"

extern "C"
{

struct hyperparse_space* hyperparsed_space = NULL;
int hyperparsed_error = 0;

static void
free_identifier_list(struct hyperparse_identifier_list* sl)
{
    if (sl->name)
    {
        free(sl->name);
    }

    while (sl->next)
    {
        struct hyperparse_identifier_list* tmp = sl->next;
        sl->next = tmp->next;
        tmp->next = NULL;
        free_identifier_list(tmp);
    }

    free(sl);
}

static void
free_attribute(struct hyperparse_attribute* a)
{
    if (a->name)
    {
        free(a->name);
    }

    free(a);
}

static void
free_attribute_list(struct hyperparse_attribute_list* al)
{
    if (al->attr)
    {
        free_attribute(al->attr);
    }

    while (al->next)
    {
        struct hyperparse_attribute_list* tmp = al->next;
        al->next = tmp->next;
        tmp->next = NULL;
        free_attribute_list(tmp);
    }

    free(al);
}

static void
free_subspace(struct hyperparse_subspace* s)
{
    if (s->attrs)
    {
        free_identifier_list(s->attrs);
    }
}

static void
free_subspace_list(struct hyperparse_subspace_list* sl)
{
    if (sl->subspace)
    {
        free_subspace(sl->subspace);
    }

    while (sl->next)
    {
        struct hyperparse_subspace_list* tmp = sl->next;
        sl->next = tmp->next;
        tmp->next = NULL;
        free_subspace_list(tmp);
    }

    free(sl);
}

void
hyperparse_free(struct hyperparse_space* s)
{
    if (!s)
    {
        return;
    }

    if (s->name)
    {
        free(s->name);
    }

    if (s->key)
    {
        free_attribute(s->key);
    }

    if (s->attrs)
    {
        free_attribute_list(s->attrs);
    }

    if (s->subspaces)
    {
        free_subspace_list(s->subspaces);
    }
}

struct hyperparse_space*
hyperparse_create_space(char* name,
                        struct hyperparse_attribute* key,
                        struct hyperparse_attribute_list* attrs,
                        struct hyperparse_subspace_list* subspaces)
{
    struct hyperparse_space* s = reinterpret_cast<struct hyperparse_space*>(malloc(sizeof(struct hyperparse_space)));
    s->name = name;
    s->key = key;
    s->attrs = attrs;
    s->subspaces = subspaces;
    return s;
}

struct hyperparse_attribute_list*
hyperparse_create_attribute_list(struct hyperparse_attribute* attr,
                                 struct hyperparse_attribute_list* list)
{
    struct hyperparse_attribute_list* al = reinterpret_cast<struct hyperparse_attribute_list*>(malloc(sizeof(struct hyperparse_attribute_list)));
    struct hyperparse_attribute_list* tmp = list;
    al->attr = attr;
    al->next = NULL;

    if (list)
    {
        while (tmp->next)
        {
            tmp = tmp->next;
        }

        tmp->next = al;
        tmp = list;
    }
    else
    {
        tmp = al;
    }

    return tmp;
}

struct hyperparse_attribute*
hyperparse_create_attribute(char* name, enum hyperdatatype type)
{
    struct hyperparse_attribute* a = reinterpret_cast<struct hyperparse_attribute*>(malloc(sizeof(struct hyperparse_attribute)));
    a->name = name;
    a->type = type;
    return a;
}

struct hyperparse_subspace_list*
hyperparse_create_subspace_list(struct hyperparse_subspace* subspace,
                                struct hyperparse_subspace_list* list)
{
    struct hyperparse_subspace_list* sl = reinterpret_cast<struct hyperparse_subspace_list*>(malloc(sizeof(struct hyperparse_subspace_list)));
    struct hyperparse_subspace_list* tmp = list;
    sl->subspace = subspace;
    sl->next = NULL;

    if (list)
    {
        while (tmp->next)
        {
            tmp = tmp->next;
        }

        tmp->next = sl;
        tmp = list;
    }
    else
    {
        tmp = sl;
    }

    return tmp;
}

struct hyperparse_subspace*
hyperparse_create_subspace(struct hyperparse_identifier_list* attrs)
{
    struct hyperparse_subspace* s = reinterpret_cast<struct hyperparse_subspace*>(malloc(sizeof(struct hyperparse_subspace)));
    s->attrs = attrs;
    return s;
}

struct hyperparse_identifier_list*
hyperparse_create_identifier_list(char* name, struct hyperparse_identifier_list* list)
{
    struct hyperparse_identifier_list* il = reinterpret_cast<struct hyperparse_identifier_list*>(malloc(sizeof(struct hyperparse_identifier_list)));
    struct hyperparse_identifier_list* tmp = list;
    il->name = name;
    il->next = NULL;

    if (list)
    {
        while (tmp->next)
        {
            tmp = tmp->next;
        }

        tmp->next = il;
        tmp = list;
    }
    else
    {
        tmp = il;
    }

    return tmp;
}

} // extern "C"
