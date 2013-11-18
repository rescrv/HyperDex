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

// HyperDex
#include "common/datatypes.h"
#include "common/datatype_float.h"
#include "common/datatype_int64.h"
#include "common/datatype_list.h"
#include "common/datatype_map.h"
#include "common/datatype_set.h"
#include "common/datatype_string.h"

using hyperdex::datatype_info;

static hyperdex::datatype_string d_string;
static hyperdex::datatype_int64 d_int64;
static hyperdex::datatype_float d_float;
static hyperdex::datatype_list d_list_string(&d_string);
static hyperdex::datatype_list d_list_int64(&d_int64);
static hyperdex::datatype_list d_list_float(&d_float);
static hyperdex::datatype_set d_set_string(&d_string);
static hyperdex::datatype_set d_set_int64(&d_int64);
static hyperdex::datatype_set d_set_float(&d_float);
static hyperdex::datatype_map d_map_string_string(&d_string, &d_string);
static hyperdex::datatype_map d_map_string_int64(&d_string, &d_int64);
static hyperdex::datatype_map d_map_string_float(&d_string, &d_float);
static hyperdex::datatype_map d_map_int64_string(&d_int64, &d_string);
static hyperdex::datatype_map d_map_int64_int64(&d_int64, &d_int64);
static hyperdex::datatype_map d_map_int64_float(&d_int64, &d_float);
static hyperdex::datatype_map d_map_float_string(&d_float, &d_string);
static hyperdex::datatype_map d_map_float_int64(&d_float, &d_int64);
static hyperdex::datatype_map d_map_float_float(&d_float, &d_float);

datatype_info*
datatype_info :: lookup(hyperdatatype datatype)
{
    switch (datatype)
    {
        case HYPERDATATYPE_STRING:
            return &d_string;
        case HYPERDATATYPE_INT64:
            return &d_int64;
        case HYPERDATATYPE_FLOAT:
            return &d_float;
        case HYPERDATATYPE_LIST_STRING:
            return &d_list_string;
        case HYPERDATATYPE_LIST_INT64:
            return &d_list_int64;
        case HYPERDATATYPE_LIST_FLOAT:
            return &d_list_float;
        case HYPERDATATYPE_SET_STRING:
            return &d_set_string;
        case HYPERDATATYPE_SET_INT64:
            return &d_set_int64;
        case HYPERDATATYPE_SET_FLOAT:
            return &d_set_float;
        case HYPERDATATYPE_MAP_STRING_STRING:
            return &d_map_string_string;
        case HYPERDATATYPE_MAP_STRING_INT64:
            return &d_map_string_int64;
        case HYPERDATATYPE_MAP_STRING_FLOAT:
            return &d_map_string_float;
        case HYPERDATATYPE_MAP_INT64_STRING:
            return &d_map_int64_string;
        case HYPERDATATYPE_MAP_INT64_INT64:
            return &d_map_int64_int64;
        case HYPERDATATYPE_MAP_INT64_FLOAT:
            return &d_map_int64_float;
        case HYPERDATATYPE_MAP_FLOAT_STRING:
            return &d_map_float_string;
        case HYPERDATATYPE_MAP_FLOAT_INT64:
            return &d_map_float_int64;
        case HYPERDATATYPE_MAP_FLOAT_FLOAT:
            return &d_map_float_float;
        case HYPERDATATYPE_GENERIC:
        case HYPERDATATYPE_LIST_GENERIC:
        case HYPERDATATYPE_SET_GENERIC:
        case HYPERDATATYPE_GARBAGE:
        case HYPERDATATYPE_MAP_GENERIC:
        case HYPERDATATYPE_MAP_STRING_KEYONLY:
        case HYPERDATATYPE_MAP_INT64_KEYONLY:
        case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
        default:
            return NULL;
    }
}

datatype_info :: datatype_info()
{
}

datatype_info :: ~datatype_info() throw ()
{
}

bool
datatype_info :: hashable()
{
    return false;
}

uint64_t
datatype_info :: hash(const e::slice&)
{
    // if you see an abort here, you overrode "hashable", but not this method
    abort();
}

bool
datatype_info :: indexable()
{
    return false;
}

bool
datatype_info :: has_length()
{
    return false;
}

uint64_t
datatype_info :: length(const e::slice&)
{
    // if you see an abort here, you overrode "has_length", but not this method
    abort();
}

bool
datatype_info :: has_regex()
{
    return false;
}

bool
datatype_info :: regex(const e::slice&,
                       const e::slice&)
{
    // if you see an abort here, you overrode "has_regex", but not this method
    abort();
}

bool
datatype_info :: has_contains()
{
    return false;
}

hyperdatatype
datatype_info :: contains_datatype()
{
    // if you see an abort here, you overrode "has_contains", but not this method
    abort();
}

bool
datatype_info :: contains(const e::slice&, const e::slice&)
{
    // if you see an abort here, you overrode "has_contains", but not this method
    abort();
}

bool
datatype_info :: containable()
{
    return false;
}

bool
datatype_info :: step(const uint8_t**,
                      const uint8_t*,
                      e::slice*)
{
    // if you see an abort here, you overrode "containable", but not this
    // method
    abort();
}

uint8_t*
datatype_info :: write(uint8_t*,
                       const e::slice&)
{
    // if you see an abort here, you overrode "containable", but not this
    // method
    abort();
}

bool
datatype_info :: comparable()
{
    return false;
}

int
datatype_info :: compare(const e::slice&, const e::slice&)
{
    // if you see an abort here, you overrode "comparable", but not this
    // method
    abort();
}

datatype_info::compares_less
datatype_info :: compare_less()
{
    // if you see an abort here, you overrode "comparable", but not this
    // method
    abort();
}
