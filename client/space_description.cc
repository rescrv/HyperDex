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

#define __STDC_LIMIT_MACROS

// po6
#include <po6/threads/mutex.h>

// e
#include <e/guard.h>

// HyperDex
#include "client/parse_space_aux.h"
#include "client/partition.h"
#include "client/space_description.h"

static po6::threads::mutex protect_the_bison/*!*/;

extern "C"
{
extern int yyparse();
extern struct yy_buffer_state* yy_scan_buffer(char *, size_t);
} // extern "C"

bool
hyperdex :: space_description_to_space(const char* description, space* ret)
{
    std::vector<char> desc(description, description + strlen(description));
    hyperparse_space* parsed = NULL;
    bool error = false;

    {
        po6::threads::mutex::hold hold(&protect_the_bison);
        desc.push_back('\0'); desc.push_back('\0');
        yy_scan_buffer(&desc.front(), desc.size());
        yyparse();
        parsed = hyperparsed_space;
        error = hyperparsed_error != 0;
        hyperparsed_space = NULL;
        hyperparsed_error = 0;
    }

    if (!parsed)
    {
        return false;
    }

    e::guard g = e::makeguard(hyperparse_free, parsed);
    g.use_variable();

    if (error)
    {
        return false;
    }

    std::vector<attribute> attrs;
    attrs.push_back(attribute(parsed->key->name, parsed->key->type));

    for (hyperparse_attribute_list* l = parsed->attrs; l; l = l->next)
    {
        attrs.push_back(attribute(l->attr->name, l->attr->type));
    }

    schema sc;
    sc.attrs_sz = attrs.size();
    sc.attrs = &attrs.front();

    space sp(parsed->name, sc);
    sp.subspaces.push_back(subspace());
    sp.subspaces.back().attrs.push_back(0);

    for (hyperparse_subspace_list* l = parsed->subspaces; l; l = l->next)
    {
        sp.subspaces.push_back(subspace());

        for (hyperparse_identifier_list* i = l->subspace->attrs; i; i = i->next)
        {
            uint16_t attr = sc.lookup_attr(i->name);

            if (attr == sc.attrs_sz)
            {
                return false;
            }

            sp.subspaces.back().attrs.push_back(attr);
        }
    }

    sp.fault_tolerance = parsed->fault_tolerance;

    if (!sp.validate())
    {
        return false;
    }

    for (size_t i = 0; i < sp.subspaces.size(); ++i)
    {
        partition(sp.subspaces[i].attrs.size(), parsed->partitioning, &sp.subspaces[i].regions);
    }

    *ret = sp;
    return true;
}
