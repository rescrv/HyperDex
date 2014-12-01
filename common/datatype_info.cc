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
#include "common/datatype_info.h"
#include "common/datatype_document.h"
#include "common/datatype_timestamp.h"
#include "common/datatype_float.h"
#include "common/datatype_int64.h"
#include "common/datatype_list.h"
#include "common/datatype_macaroon_secret.h"
#include "common/datatype_map.h"
#include "common/datatype_set.h"
#include "common/datatype_string.h"

using hyperdex::datatype_info;

static hyperdex::datatype_string d_string;
static hyperdex::datatype_int64 d_int64;
static hyperdex::datatype_float d_float;
static hyperdex::datatype_document d_document;
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
static hyperdex::datatype_timestamp d_timestamp_second(HYPERDATATYPE_TIMESTAMP_SECOND);
static hyperdex::datatype_timestamp d_timestamp_minute(HYPERDATATYPE_TIMESTAMP_MINUTE);
static hyperdex::datatype_timestamp d_timestamp_hour(HYPERDATATYPE_TIMESTAMP_HOUR);
static hyperdex::datatype_timestamp d_timestamp_day(HYPERDATATYPE_TIMESTAMP_DAY);
static hyperdex::datatype_timestamp d_timestamp_week(HYPERDATATYPE_TIMESTAMP_WEEK);
static hyperdex::datatype_timestamp d_timestamp_month(HYPERDATATYPE_TIMESTAMP_MONTH);
static hyperdex::datatype_macaroon_secret d_macaroon_secret;

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
        case HYPERDATATYPE_DOCUMENT:
            return &d_document;
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
        case HYPERDATATYPE_TIMESTAMP_SECOND:
          return &d_timestamp_second;
        case HYPERDATATYPE_TIMESTAMP_MINUTE:
          return &d_timestamp_minute;
        case HYPERDATATYPE_TIMESTAMP_HOUR:
          return &d_timestamp_hour;
        case HYPERDATATYPE_TIMESTAMP_DAY:
          return &d_timestamp_day;
        case HYPERDATATYPE_TIMESTAMP_WEEK:
          return &d_timestamp_week;
        case HYPERDATATYPE_TIMESTAMP_MONTH:
          return &d_timestamp_month;
        case HYPERDATATYPE_MACAROON_SECRET:
            return &d_macaroon_secret;
        case HYPERDATATYPE_GENERIC:
        case HYPERDATATYPE_TIMESTAMP_GENERIC:
        case HYPERDATATYPE_LIST_GENERIC:
        case HYPERDATATYPE_SET_GENERIC:
        case HYPERDATATYPE_MAP_GENERIC:
        case HYPERDATATYPE_MAP_STRING_KEYONLY:
        case HYPERDATATYPE_MAP_INT64_KEYONLY:
        case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
        case HYPERDATATYPE_GARBAGE:
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
datatype_info :: client_to_server(const e::slice& client,
                                  e::arena*,
                                  e::slice* server) const
{
    *server = client;
    return true;
}

bool
datatype_info :: server_to_client(const e::slice& server,
                                  e::arena*,
                                  e::slice* client) const
{
    *client = server;
    return true;
}

bool
datatype_info :: hashable() const
{
    return false;
}

uint64_t
datatype_info :: hash(const e::slice&) const
{
    // if you see an abort here, you overrode "hashable", but not this method
    abort();
}

bool
datatype_info :: indexable() const
{
    return false;
}

bool
datatype_info :: has_length() const
{
    return false;
}

uint64_t
datatype_info :: length(const e::slice&) const
{
    // if you see an abort here, you overrode "has_length", but not this method
    abort();
}

bool
datatype_info :: has_regex() const
{
    return false;
}

bool
datatype_info :: regex(const e::slice&,
                       const e::slice&) const
{
    // if you see an abort here, you overrode "has_regex", but not this method
    abort();
}

bool
datatype_info :: has_contains() const
{
    return false;
}

hyperdatatype
datatype_info :: contains_datatype() const
{
    // if you see an abort here, you overrode "has_contains", but not this method
    abort();
}

bool
datatype_info :: contains(const e::slice&, const e::slice&) const
{
    // if you see an abort here, you overrode "has_contains", but not this method
    abort();
}

bool
datatype_info :: containable() const
{
    return false;
}

bool
datatype_info :: step(const uint8_t**,
                      const uint8_t*,
                      e::slice*) const
{
    // if you see an abort here, you overrode "containable", but not this
    // method
    abort();
}

uint64_t
datatype_info :: write_sz(const e::slice&) const
{
    // if you see an abort here, you overrode "containable", but not this
    // method
    abort();
}

uint8_t*
datatype_info :: write(const e::slice&, uint8_t*) const
{
    // if you see an abort here, you overrode "containable", but not this
    // method
    abort();
}

bool
datatype_info :: comparable() const
{
    return false;
}

int
datatype_info :: compare(const e::slice&, const e::slice&) const
{
    // if you see an abort here, you overrode "comparable", but not this
    // method
    abort();
}

datatype_info::compares_less
datatype_info :: compare_less() const
{
    // if you see an abort here, you overrode "comparable", but not this
    // method
    abort();
}

bool
datatype_info :: document() const
{
    return false;
}

bool
datatype_info :: document_check(const attribute_check&,
                                const e::slice&) const
{
    // if you see an abort here, you overrode "document", but not this
    // method
    abort();
}
