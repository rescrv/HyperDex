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

// HyperDex
#include "common/datatype_float.h"
#include "common/datatype_int64.h"
#include "common/ordered_encoding.h"

using hyperdex::datatype_info;
using hyperdex::datatype_float;

double
datatype_float :: unpack(const e::slice& value)
{
    assert(value.size() == 0 || value.size() == sizeof(int64_t));

    if (value.size() == 0)
    {
        return 0;
    }

    double number;
    e::unpackdoublele(value.data(), &number);
    return number;
}

double
datatype_float :: unpack(const funcall& value)
{
    if (value.arg1_datatype == HYPERDATATYPE_INT64)
    {
        return datatype_int64::unpack(value.arg1);
    }
    else if (value.arg1_datatype == HYPERDATATYPE_FLOAT)
    {
        return datatype_float::unpack(value.arg1);
    }
    else
    {
        return 0;
    }
}

void
datatype_float :: pack(double num, std::vector<char>* scratch, e::slice* value)
{
    if (scratch->size() < sizeof(double))
    {
        scratch->resize(sizeof(double));
    }

    e::packdoublele(num, &(*scratch)[0]);
    *value = e::slice(&(*scratch)[0], sizeof(double));
}

bool
datatype_float :: static_validate(const e::slice& value)
{
    return value.size() == sizeof(double) || value.empty();
}

datatype_float :: datatype_float()
{
}

datatype_float :: ~datatype_float() throw ()
{
}

hyperdatatype
datatype_float :: datatype() const
{
    return HYPERDATATYPE_FLOAT;
}

bool
datatype_float :: validate(const e::slice& value) const
{
    return static_validate(value);
}

bool
datatype_float :: check_args(const funcall& func) const
{
    return ((func.arg1_datatype == HYPERDATATYPE_FLOAT && validate(func.arg1)) ||
            (func.arg1_datatype == HYPERDATATYPE_INT64 && datatype_int64::static_validate(func.arg1))) &&
           (func.name == FUNC_SET ||
            func.name == FUNC_NUM_ADD ||
            func.name == FUNC_NUM_SUB ||
            func.name == FUNC_NUM_MUL ||
            func.name == FUNC_NUM_DIV ||
            func.name == FUNC_NUM_MAX ||
            func.name == FUNC_NUM_MIN);
}

bool
datatype_float :: apply(const e::slice& old_value,
                        const funcall* funcs, size_t funcs_sz,
                        e::arena* new_memory,
                        e::slice* new_value) const
{
    double number = unpack(old_value);

    for (size_t i = 0; i < funcs_sz; ++i)
    {
        const funcall* func = funcs + i;
        double arg = unpack(*func);

        switch (func->name)
        {
            case FUNC_SET:
                number = arg;
                break;
            case FUNC_NUM_ADD:
                number += arg;
                break;
            case FUNC_NUM_SUB:
                number -= arg;
                break;
            case FUNC_NUM_MUL:
                number *= arg;
                break;
            case FUNC_NUM_DIV:
                number /= arg;
                break;
            case FUNC_NUM_MAX:
                number = std::max(number, arg);
                break;
            case FUNC_NUM_MIN:
                number = std::min(number, arg);
                break;
            case FUNC_NUM_MOD:
            case FUNC_NUM_AND:
            case FUNC_NUM_OR:
            case FUNC_NUM_XOR:
            case FUNC_STRING_APPEND:
            case FUNC_STRING_PREPEND:
            case FUNC_STRING_LTRIM:
            case FUNC_STRING_RTRIM:
            case FUNC_LIST_LPUSH:
            case FUNC_LIST_RPUSH:
            case FUNC_SET_ADD:
            case FUNC_SET_REMOVE:
            case FUNC_SET_INTERSECT:
            case FUNC_SET_UNION:
            case FUNC_DOC_RENAME:
            case FUNC_DOC_UNSET:
            case FUNC_MAP_ADD:
            case FUNC_MAP_REMOVE:
            case FUNC_FAIL:
            default:
                abort();
        }
    }

    uint8_t* ptr = NULL;
    new_memory->allocate(sizeof(double), &ptr);
    e::packdoublele(number, ptr);
    *new_value = e::slice(ptr, sizeof(double));
    return true;
}

bool
datatype_float :: hashable() const
{
    return true;
}

uint64_t
datatype_float :: hash(const e::slice& value) const
{
    assert(validate(value));
    double number = unpack(value);
    return ordered_encode_double(number);
}

bool
datatype_float :: indexable() const
{
    return true;
}

bool
datatype_float :: containable() const
{
    return true;
}

bool
datatype_float :: step(const uint8_t** ptr,
                       const uint8_t* end,
                       e::slice* elem) const
{
    if (static_cast<size_t>(end - *ptr) < sizeof(double))
    {
        return false;
    }

    *elem = e::slice(*ptr, sizeof(double));
    *ptr += sizeof(double);
    return true;
}

uint64_t
datatype_float :: write_sz(const e::slice& elem) const
{
    return elem.size();
}

uint8_t*
datatype_float :: write(const e::slice& elem,
                        uint8_t* write_to) const
{
    memmove(write_to, elem.data(), elem.size());
    return write_to + elem.size();
}

bool
datatype_float :: comparable() const
{
    return true;
}

static int
compare(const e::slice& lhs,
        const e::slice& rhs)
{
    double lhsnum = datatype_float::unpack(lhs);
    double rhsnum = datatype_float::unpack(rhs);

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
datatype_float :: compare(const e::slice& lhs, const e::slice& rhs) const
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
datatype_float :: compare_less() const
{
    return &::compare_less;
}
