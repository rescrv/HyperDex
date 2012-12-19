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

#ifndef hyperdex_client_parse_space_aux_h_
#define hyperdex_client_parse_space_aux_h_

/* HyperDex */
#include "hyperdex.h"

#ifdef __cplusplus
extern "C"
{
#endif /* __cplusplus */

struct hyperparse_space;
struct hyperparse_subspace_list;
struct hyperparse_subspace;
struct hyperparse_attribute_list;
struct hyperparse_attribute;

struct hyperparse_space
{
    char* name;
    struct hyperparse_attribute* key;
    struct hyperparse_attribute_list* attrs;
    struct hyperparse_subspace_list* subspaces;
};

struct hyperparse_subspace_list
{
    struct hyperparse_subspace* subspace;
    struct hyperparse_subspace_list* next;
};

struct hyperparse_subspace
{
    struct hyperparse_identifier_list* attrs;
};

struct hyperparse_attribute_list
{
    struct hyperparse_attribute* attr;
    struct hyperparse_attribute_list* next;
};

struct hyperparse_attribute
{
    char* name;
    enum hyperdatatype type;
};

struct hyperparse_identifier_list
{
    char* name;
    struct hyperparse_identifier_list* next;
};

extern struct hyperparse_space* hyperparsed_space;
extern int hyperparsed_error;

void hyperparse_free(struct hyperparse_space* s);

struct hyperparse_space*
hyperparse_create_space(char* name,
                        struct hyperparse_attribute* key,
                        struct hyperparse_attribute_list* attrs,
                        struct hyperparse_subspace_list* subspaces);

struct hyperparse_attribute_list*
hyperparse_create_attribute_list(struct hyperparse_attribute* attr,
                                 struct hyperparse_attribute_list* list);

struct hyperparse_attribute*
hyperparse_create_attribute(char* name, enum hyperdatatype type);

struct hyperparse_subspace_list*
hyperparse_create_subspace_list(struct hyperparse_subspace* subspace,
                                struct hyperparse_subspace_list* list);

struct hyperparse_subspace*
hyperparse_create_subspace(struct hyperparse_identifier_list* attrs);

struct hyperparse_identifier_list*
hyperparse_create_identifier_list(char* name, struct hyperparse_identifier_list* list);

#ifdef __cplusplus
} // extern "C"
#endif /* __cplusplus */
#endif /* hyperdex_client_parse_space_aux_h_ */
