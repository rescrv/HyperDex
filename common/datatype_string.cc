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

// C
#include <cstdlib>

// e
#include <e/endian.h>

// HyperDex
#include "common/datatype_int64.h"
#include "common/datatype_string.h"
#include "common/regex_match.h"
#include "cityhash/city.h"

using hyperdex::datatype_info;
using hyperdex::datatype_string;

datatype_string :: datatype_string()
{
}

datatype_string :: ~datatype_string() throw ()
{
}

hyperdatatype
datatype_string :: datatype() const
{
    return HYPERDATATYPE_STRING;
}

bool
datatype_string :: validate(const e::slice&) const
{
    return true;
}

bool
datatype_string :: check_args(const funcall& func) const
{
    if (func.name == FUNC_SET ||
        func.name == FUNC_STRING_PREPEND ||
        func.name == FUNC_STRING_APPEND)
    {
        return func.arg1_datatype == HYPERDATATYPE_STRING &&
               validate(func.arg1);
    }
    else if (func.name == FUNC_STRING_LTRIM ||
             func.name == FUNC_STRING_RTRIM)
    {
        datatype_info* di = datatype_info::lookup(HYPERDATATYPE_INT64);

        if (func.arg1_datatype != HYPERDATATYPE_INT64 ||
            !di->validate(func.arg1))
        {
            return false;
        }

        return datatype_int64::unpack(func.arg1) >= 0;
    }
    else
    {
        return false;
    }
}

bool
datatype_string :: apply(const e::slice& old_value,
                         const funcall* funcs, size_t funcs_sz,
                         e::arena* new_memory,
                         e::slice* new_value) const
{
    size_t alloc_sz = old_value.size();

    for (size_t i = 0; i < funcs_sz; ++i)
    {
        alloc_sz += funcs[i].arg1.size();
    }

    uint8_t* ptr = NULL;
    new_memory->allocate(alloc_sz, &ptr);
    size_t sz = old_value.size();
    memmove(ptr, old_value.data(), sz);
    int64_t x = 0;

    for (size_t i = 0; i < funcs_sz; ++i)
    {
        switch (funcs[i].name)
        {
            case FUNC_SET:
                // Overwrite
                memmove(ptr, funcs[i].arg1.data(), funcs[i].arg1.size());
                sz = funcs[i].arg1.size();
                break;
            case FUNC_STRING_PREPEND:
                // Shift
                memmove(ptr + funcs[i].arg1.size(), ptr, sz);
                // Fill
                memmove(ptr, funcs[i].arg1.data(), funcs[i].arg1.size());
                // Resize
                sz += funcs[i].arg1.size();
                break;
            case FUNC_STRING_APPEND:
                // Fill
                memmove(ptr + sz, funcs[i].arg1.data(), funcs[i].arg1.size());
                // Resize
                sz += funcs[i].arg1.size();
                break;
            case FUNC_STRING_LTRIM:
                x = datatype_int64::unpack(funcs[i].arg1);
                x = uint64_t(x) > sz ? sz : x;
                sz -= x;
                memmove(ptr, ptr + x, sz);
                break;
            case FUNC_STRING_RTRIM:
                x = datatype_int64::unpack(funcs[i].arg1);
                x = uint64_t(x) > sz ? sz : x;
                sz -= x;
                break;
            case FUNC_FAIL:
            case FUNC_NUM_ADD:
            case FUNC_NUM_SUB:
            case FUNC_NUM_MUL:
            case FUNC_NUM_DIV:
            case FUNC_NUM_MOD:
            case FUNC_NUM_AND:
            case FUNC_NUM_OR:
            case FUNC_NUM_XOR:
            case FUNC_NUM_MAX:
            case FUNC_NUM_MIN:
            case FUNC_LIST_LPUSH:
            case FUNC_LIST_RPUSH:
            case FUNC_SET_ADD:
            case FUNC_SET_REMOVE:
            case FUNC_SET_INTERSECT:
            case FUNC_SET_UNION:
            case FUNC_MAP_ADD:
            case FUNC_MAP_REMOVE:
            case FUNC_DOC_RENAME:
            case FUNC_DOC_UNSET:
            default:
                abort();
        }
    }

    *new_value = e::slice(ptr, sz);
    return true;
}

bool
datatype_string :: hashable() const
{
    return true;
}

uint64_t
datatype_string :: hash(const e::slice& value) const
{
    return CityHash64(reinterpret_cast<const char*>(value.data()), value.size());
}

bool
datatype_string :: indexable() const
{
    return true;
}

bool
datatype_string :: has_length() const
{
    return true;
}

uint64_t
datatype_string :: length(const e::slice& value) const
{
    return value.size();
}

bool
datatype_string :: has_regex() const
{
    return true;
}

bool
datatype_string :: regex(const e::slice& r,
                         const e::slice& v) const
{
    return regex_match(r.data(), r.size(), v.data(), v.size());
}

bool
datatype_string :: containable() const
{
    return true;
}

bool
datatype_string :: step(const uint8_t** ptr,
                        const uint8_t* end,
                        e::slice* elem) const
{
    if (static_cast<size_t>(end - *ptr) < sizeof(uint32_t))
    {
        return false;
    }

    uint32_t sz = 0;
    *ptr = e::unpack32le(*ptr, &sz);
    *elem = e::slice(*ptr, sz);
    *ptr += sz;
    return *ptr <= end;
}

uint64_t
datatype_string :: write_sz(const e::slice& elem) const
{
    return sizeof(uint32_t) + elem.size();
}

uint8_t*
datatype_string :: write(const e::slice& elem,
                         uint8_t* write_to) const
{
    memmove(write_to + sizeof(uint32_t), elem.data(), elem.size());
    e::pack32le(elem.size(), write_to);
    return write_to + sizeof(uint32_t) + elem.size();
}

bool
datatype_string :: comparable() const
{
    return true;
}

static int
compare(const e::slice& lhs,
        const e::slice& rhs)
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
datatype_string :: compare(const e::slice& lhs, const e::slice& rhs) const
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
datatype_string :: compare_less() const
{
    return &::compare_less;
}
