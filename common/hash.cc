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

// Google CityHash
#include <city.h>

// e
#include <e/endian.h>

// HyperDex
#include "common/float_encode.h"
#include "common/hash.h"

static uint64_t
_hash(hyperdatatype t, const e::slice& v)
{
    uint8_t tmp[sizeof(int64_t)];
    double ret_d;
    int64_t ret_i;

    switch (t)
    {
        case HYPERDATATYPE_STRING:
            return CityHash64(reinterpret_cast<const char*>(v.data()), v.size());
        case HYPERDATATYPE_INT64:
            memset(tmp, 0, sizeof(int64_t));
            memmove(tmp, v.data(), std::min(v.size(), sizeof(int64_t)));
            e::unpack64le(tmp, &ret_i);
            return ret_i;
        case HYPERDATATYPE_FLOAT:
            memset(tmp, 0, sizeof(uint64_t));
            memmove(tmp, v.data(), std::min(v.size(), sizeof(uint64_t)));
            e::unpackdoublele(tmp, &ret_d);
            return hyperdex::float_encode(ret_d);
        case HYPERDATATYPE_GENERIC:
        case HYPERDATATYPE_LIST_GENERIC:
        case HYPERDATATYPE_LIST_STRING:
        case HYPERDATATYPE_LIST_INT64:
        case HYPERDATATYPE_LIST_FLOAT:
        case HYPERDATATYPE_SET_GENERIC:
        case HYPERDATATYPE_SET_STRING:
        case HYPERDATATYPE_SET_INT64:
        case HYPERDATATYPE_SET_FLOAT:
        case HYPERDATATYPE_MAP_GENERIC:
        case HYPERDATATYPE_MAP_STRING_KEYONLY:
        case HYPERDATATYPE_MAP_STRING_STRING:
        case HYPERDATATYPE_MAP_STRING_INT64:
        case HYPERDATATYPE_MAP_STRING_FLOAT:
        case HYPERDATATYPE_MAP_INT64_KEYONLY:
        case HYPERDATATYPE_MAP_INT64_STRING:
        case HYPERDATATYPE_MAP_INT64_INT64:
        case HYPERDATATYPE_MAP_INT64_FLOAT:
        case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
        case HYPERDATATYPE_MAP_FLOAT_STRING:
        case HYPERDATATYPE_MAP_FLOAT_INT64:
        case HYPERDATATYPE_MAP_FLOAT_FLOAT:
        case HYPERDATATYPE_GARBAGE:
        default:
            return 0;
    }
}

void
hyperdex :: hash(const schema& sc,
                 const e::slice& key,
                 uint64_t* hash)
{
    *hash = _hash(sc.attrs[0].type, key);
}

void
hyperdex :: hash(const hyperdex::schema& sc,
                 const e::slice& key,
                 const std::vector<e::slice>& value,
                 uint64_t* hashes)
{
    hashes[0] = _hash(sc.attrs[0].type, key);

    for (size_t i = 1; i < sc.attrs_sz; ++i)
    {
        hashes[i] = _hash(sc.attrs[i].type, value[i - 1]);
    }
}
