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

// HyperDex
#include "common/datatypes.h"
#include "common/hash.h"

uint64_t
hyperdex :: hash(hyperdatatype t, const e::slice& v)
{
    datatype_info* di = datatype_info::lookup(t);
    assert(di);

    if (!di->hashable())
    {
        return 0;
    }

    return di->hash(v);
}

void
hyperdex :: hash(const schema& sc,
                 const e::slice& key,
                 uint64_t* h)
{
    *h = hash(sc.attrs[0].type, key);
}

void
hyperdex :: hash(const hyperdex::schema& sc,
                 const e::slice& key,
                 const std::vector<e::slice>& value,
                 uint64_t* hs)
{
    hs[0] = hash(sc.attrs[0].type, key);

    for (size_t i = 1; i < sc.attrs_sz; ++i)
    {
        hs[i] = hash(sc.attrs[i].type, value[i - 1]);
    }
}
