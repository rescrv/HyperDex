// Copyright (c) 2014, Cornell University
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
#include "common/index.h"

using hyperdex::index;

index :: index()
    : type(NORMAL)
    , id()
    , attr(UINT16_MAX)
    , extra()
{
}

index :: index(index_t t, index_id i, uint16_t a, const e::slice& e)
    : type(t)
    , id(i)
    , attr(a)
    , extra(e)
{
}

index :: ~index() throw ()
{
}

hyperdex::index&
index :: operator = (const index& rhs)
{
    if (this != &rhs)
    {
        id = rhs.id;
        attr = rhs.attr;
    }

    return *this;
}

std::ostream&
hyperdex :: operator << (std::ostream& lhs, const index& rhs)
{
    switch (rhs.type)
    {
        case index::NORMAL:
            lhs << "index(" << rhs.id.get() << ", " << rhs.attr << ")";
            break;
        case index::DOCUMENT:
            lhs << "index(" << rhs.id.get()
                << ", " << rhs.extra.str()
                << ", " << rhs.attr <<  ")";
            break;
        default:
            abort();
    }

    return lhs;
}

e::packer
hyperdex :: operator << (e::packer pa, const index& t)
{
    return pa << t.type << t.id << t.attr << t.extra;
}

e::unpacker
hyperdex :: operator >> (e::unpacker up, index& t)
{
    up = up >> t.type >> t.id >> t.attr >> t.extra;
    return up;
}

size_t
hyperdex :: pack_size(const index& t)
{
    return pack_size(t.type) + pack_size(t.id) + sizeof(t.attr)
         + sizeof(uint32_t) + t.extra.size();
}

e::packer
hyperdex :: operator << (e::packer pa, const index::index_t& t)
{
    uint8_t x = t;
    return pa << x;
}

e::unpacker
hyperdex :: operator >> (e::unpacker up, index::index_t& t)
{
    uint8_t x;
    up = up >> x;
    t = static_cast<index::index_t>(x);
    return up;
}

size_t
hyperdex :: pack_size(const index::index_t&)
{
    return sizeof(uint8_t);
}
