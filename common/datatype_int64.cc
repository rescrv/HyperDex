// Copyright (c) 2013, Cornell University
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

// C
#include <cstdlib>

// e
#include <e/endian.h>
#include <e/safe_math.h>

// HyperDex
#include "common/datatype_int64.h"
#include "common/ordered_encoding.h"

using hyperdex::datatype_info;
using hyperdex::datatype_int64;

datatype_int64 :: datatype_int64()
{
}

datatype_int64 :: ~datatype_int64() throw ()
{
}

hyperdatatype
datatype_int64 :: datatype()
{
    return HYPERDATATYPE_INT64;
}

bool
datatype_int64 :: validate(const e::slice& value)
{
    return value.size() == sizeof(int64_t) || value.empty();
}

bool
datatype_int64 :: check_args(const funcall& func)
{
    return func.arg1_datatype == HYPERDATATYPE_INT64 &&
           validate(func.arg1) &&
           (func.name == FUNC_SET ||
            func.name == FUNC_NUM_ADD ||
            func.name == FUNC_NUM_SUB ||
            func.name == FUNC_NUM_MUL ||
            func.name == FUNC_NUM_DIV ||
            func.name == FUNC_NUM_MOD ||
            func.name == FUNC_NUM_AND ||
            func.name == FUNC_NUM_OR ||
            func.name == FUNC_NUM_XOR);
}

uint8_t*
datatype_int64 :: apply(const e::slice& old_value,
                        const funcall* funcs, size_t funcs_sz,
                        uint8_t* writeto)
{
    int64_t number = 0;

    if (validate(old_value) && old_value.size() == sizeof(int64_t))
    {
        e::unpack64le(old_value.data(), &number);
    }

    for (size_t i = 0; i < funcs_sz; ++i)
    {
        const funcall* func = funcs + i;
        int64_t arg;
        e::unpack64le(func->arg1.data(), &arg);

        switch (func->name)
        {
            case FUNC_SET:
                number = arg;
                break;
            case FUNC_NUM_ADD:
                if (!e::safe_add(number, arg, &number))
                {
                    return NULL; // XXX signed overflow
                }
                break;
            case FUNC_NUM_SUB:
                if (!e::safe_sub(number, arg, &number))
                {
                    return NULL; // XXX signed overflow
                }
                break;
            case FUNC_NUM_MUL:
                if (!e::safe_mul(number, arg, &number))
                {
                    return NULL; // XXX signed overflow
                }
                break;
            case FUNC_NUM_DIV:
                if (!e::safe_div(number, arg, &number))
                {
                    return NULL; // XXX signed overflow
                }
                break;
            case FUNC_NUM_MOD:
                if (!e::safe_mod(number, arg, &number))
                {
                    return NULL; // XXX signed overflow
                }
                break;
            case FUNC_NUM_AND:
                number &= arg;
                break;
            case FUNC_NUM_OR:
                number |= arg;
                break;
            case FUNC_NUM_XOR:
                number ^= arg;
                break;
            case FUNC_STRING_APPEND:
            case FUNC_STRING_PREPEND:
            case FUNC_LIST_LPUSH:
            case FUNC_LIST_RPUSH:
            case FUNC_SET_ADD:
            case FUNC_SET_REMOVE:
            case FUNC_SET_INTERSECT:
            case FUNC_SET_UNION:
            case FUNC_MAP_ADD:
            case FUNC_MAP_REMOVE:
            case FUNC_FAIL:
            default:
                abort();
        }
    }

    return e::pack64le(number, writeto);
}

bool
datatype_int64 :: hashable()
{
    return true;
}

uint64_t
datatype_int64 :: hash(const e::slice& value)
{
    assert(validate(value));
    int64_t number = 0;

    if (value.size() == sizeof(int64_t))
    {
        e::unpack64le(value.data(), &number);
    }

    return ordered_encode_int64(number);
}

bool
datatype_int64 :: indexable()
{
    return true;
}

bool
datatype_int64 :: containable()
{
    return true;
}

bool
datatype_int64 :: step(const uint8_t** ptr,
                       const uint8_t* end,
                       e::slice* elem)
{
    if (static_cast<size_t>(end - *ptr) < sizeof(int64_t))
    {
        return false;
    }

    *elem = e::slice(*ptr, sizeof(int64_t));
    *ptr += sizeof(int64_t);
    return true;
}

uint8_t*
datatype_int64 :: write(uint8_t* writeto,
                        const e::slice& elem)
{
    memmove(writeto, elem.data(), elem.size());
    return writeto + elem.size();
}

bool
datatype_int64 :: comparable()
{
    return true;
}

static int
compare(const e::slice& lhs,
        const e::slice& rhs)
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
datatype_int64 :: compare(const e::slice& lhs, const e::slice& rhs)
{
    return ::compare(lhs, rhs);
}

static bool
compare_less(const e::slice& lhs,
             const e::slice& rhs)
{
    return compare(lhs, rhs) < 0;
}

datatype_info::compares_less
datatype_int64 :: compare_less()
{
    return &::compare_less;
}
