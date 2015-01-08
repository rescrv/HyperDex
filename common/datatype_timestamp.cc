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

// e
#include <e/endian.h>

// HyperDex
#include "cityhash/city.h"
#include "common/datatype_timestamp.h"

using hyperdex::datatype_info;
using hyperdex::datatype_timestamp;

namespace
{

int64_t
unpack(const e::slice& value)
{
    assert(value.size() == sizeof(int64_t) || value.empty());

    if (value.empty())
    {
        return 0;
    }

    int64_t timestamp;
    e::unpack64le(value.data(), &timestamp);
    return timestamp;
}

}

datatype_timestamp :: datatype_timestamp(hyperdatatype t)
    : m_type(t)
{
    assert(CONTAINER_TYPE(m_type) == HYPERDATATYPE_TIMESTAMP_GENERIC);
}

datatype_timestamp :: ~datatype_timestamp() throw ()
{
}

hyperdatatype
datatype_timestamp :: datatype() const
{
    return m_type;

}

bool
datatype_timestamp :: validate(const e::slice& value) const
{
    return value.size() == sizeof(int64_t) || value.empty();
}

bool
datatype_timestamp :: check_args(const funcall& func) const
{
    return func.name == FUNC_SET && func.arg1_datatype == this->datatype() && validate(func.arg1);
}

bool
datatype_timestamp :: apply(const e::slice& old_value,
                            const funcall* funcs, size_t funcs_sz,
                            e::arena* new_memory,
                            e::slice* new_value) const
{
    int64_t timestamp = unpack(old_value);

    for(size_t i = 0; i < funcs_sz; i++ )
    {
        const funcall* func = funcs + i;
        assert(func->name == FUNC_SET);
        timestamp = unpack(func->arg1);
    }

    uint8_t* ptr = NULL;
    new_memory->allocate(sizeof(int64_t), &ptr);
    e::pack64le(timestamp, ptr);
    *new_value = e::slice(ptr, sizeof(int64_t));
    return true;
}

bool
datatype_timestamp::hashable() const
{
    return true;
}

#define INT_SECONDS 60
#define INT_MINUTES 60
#define INT_HOURS 24
#define INT_DAYS 7
#define INT_WEEKS 4
#define INT_MONTHS 12

const uint64_t INTERVALS[] = {INT_SECONDS,
                              INT_MINUTES,
                              INT_HOURS,
                              INT_DAYS,
                              INT_WEEKS,
                              INT_MONTHS};

const unsigned TABLE_SECOND[] = {0, 1, 2, 3, 4, 5, 6};
const unsigned TABLE_MINUTE[] = {1, 0, 2, 3, 4, 5, 6};
const unsigned TABLE_HOUR[]   = {2, 1, 0, 3, 4, 5, 6};
const unsigned TABLE_DAY[]    = {3, 2, 1, 0, 4, 5, 6};
const unsigned TABLE_WEEK[]   = {4, 3, 2, 1, 0, 5, 6};
const unsigned TABLE_MONTH[]  = {5, 4, 3, 2, 1, 0, 6};

uint64_t
datatype_timestamp :: hash(const e::slice& v) const
{
    uint64_t timestamp = unpack(v);
    const unsigned* table = NULL;
    uint64_t value[7];

    switch (m_type)
    {
        case HYPERDATATYPE_TIMESTAMP_SECOND:
            table = TABLE_SECOND;
            break;
        case HYPERDATATYPE_TIMESTAMP_MINUTE:
            table = TABLE_MINUTE;
            break;
        case HYPERDATATYPE_TIMESTAMP_HOUR:
            table = TABLE_HOUR;
            break;
        case HYPERDATATYPE_TIMESTAMP_DAY:
            table = TABLE_DAY;
            break;
        case HYPERDATATYPE_TIMESTAMP_WEEK:
            table = TABLE_WEEK;
            break;
        case HYPERDATATYPE_TIMESTAMP_MONTH:
            table = TABLE_MONTH;
            break;
        case HYPERDATATYPE_GENERIC:
        case HYPERDATATYPE_STRING:
        case HYPERDATATYPE_INT64:
        case HYPERDATATYPE_FLOAT:
        case HYPERDATATYPE_DOCUMENT:
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
        case HYPERDATATYPE_TIMESTAMP_GENERIC:
        case HYPERDATATYPE_MACAROON_SECRET:
        case HYPERDATATYPE_GARBAGE:
        default:
            return timestamp;
    }

    uint64_t x = timestamp / 1000000.;

    for (unsigned i = 0; i < 6; ++i)
    {
        value[i] = x % INTERVALS[i];
        x /= INTERVALS[i];
    }

    value[6] = x;
    assert(table);
    uint64_t y = UINT64_MAX;
    uint64_t h = 0;

    for (unsigned i = 0; i < 6; ++i)
    {
        y = y / INTERVALS[table[i]];
        h += value[table[i]] * y;
    }

    h += value[table[6]];
    return h;
}

bool
datatype_timestamp :: indexable() const
{
    return true;
}

bool
datatype_timestamp :: containable() const
{
    return true;
}

bool
datatype_timestamp :: step(const uint8_t** ptr,
                           const uint8_t* end,
                           e::slice* elem) const
{
    if (static_cast<size_t>(end - *ptr) < sizeof(int64_t))
    {
        return false;
    }

    *elem = e::slice(*ptr, sizeof(int64_t));
    *ptr += sizeof(int64_t);
    return true;
}

uint64_t
datatype_timestamp :: write_sz(const e::slice& elem) const
{
    return elem.size();
}

uint8_t*
datatype_timestamp :: write(const e::slice& elem,
                            uint8_t* write_to) const
{
    memmove(write_to, elem.data(), elem.size());
    return write_to + elem.size();
}

bool
datatype_timestamp :: comparable() const
{
    return true;
}

static int
compare(const e::slice& lhs,
        const e::slice& rhs)
{
    int64_t lhsnum = unpack(lhs);
    int64_t rhsnum = unpack(rhs);

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
datatype_timestamp :: compare(const e::slice& lhs, const e::slice& rhs) const
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
datatype_timestamp :: compare_less() const
{
    return &::compare_less;
}
