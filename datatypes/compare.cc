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

// e
#include <e/endian.h>

// HyperDex
#include "datatypes/compare.h"

int
compare_as_type(const e::slice& lhs, const e::slice& rhs, hyperdatatype type)
{
    switch (type)
    {
        case HYPERDATATYPE_STRING:
            return compare_string(lhs, rhs);
        case HYPERDATATYPE_INT64:
            return compare_int64(lhs, rhs);
        case HYPERDATATYPE_FLOAT:
            return compare_float(lhs, rhs);
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

int
compare_string(const e::slice& lhs, const e::slice& rhs)
{
    int cmp = memcmp(lhs.data(), rhs.data(), std::min(lhs.size(), rhs.size()));

    if (cmp == 0)
    {
        if (lhs.size() < rhs.size())
        {
            return -1;
        }

        if (lhs.size() > rhs.size())
        {
            return 1;
        }

        return 0;
    }

    return cmp;
}

int
compare_int64(const e::slice& lhs, const e::slice& rhs)
{
    int64_t lhsnum = 0;
    int64_t rhsnum = 0;
    e::unpack64le(lhs.data(), &lhsnum);
    e::unpack64le(rhs.data(), &rhsnum);

    if (lhsnum < rhsnum)
    {
        return -1;
    }
    if (lhsnum > rhsnum)
    {
        return 1;
    }

    return 0;
}

int
compare_float(const e::slice& lhs, const e::slice& rhs)
{
    double lhsnum = 0;
    double rhsnum = 0;
    e::unpackdoublele(lhs.data(), &lhsnum);
    e::unpackdoublele(rhs.data(), &rhsnum);

    if (lhsnum < rhsnum)
    {
        return -1;
    }
    if (lhsnum > rhsnum)
    {
        return 1;
    }

    return 0;
}
