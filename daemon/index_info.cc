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

// HyperDex
#include "common/datatypes.h"
#include "daemon/index_float.h"
#include "daemon/index_info.h"
#include "daemon/index_int64.h"
#include "daemon/index_list.h"
#include "daemon/index_map.h"
#include "daemon/index_set.h"
#include "daemon/index_string.h"

using hyperdex::datalayer;
using hyperdex::index_info;

static hyperdex::index_string i_string;
static hyperdex::index_int64 i_int64;
static hyperdex::index_float i_float;
static hyperdex::index_list i_list_string(HYPERDATATYPE_STRING);
static hyperdex::index_list i_list_int64(HYPERDATATYPE_INT64);
static hyperdex::index_list i_list_float(HYPERDATATYPE_FLOAT);
static hyperdex::index_set i_set_string(HYPERDATATYPE_STRING);
static hyperdex::index_set i_set_int64(HYPERDATATYPE_INT64);
static hyperdex::index_set i_set_float(HYPERDATATYPE_FLOAT);
static hyperdex::index_map i_map_string_string(HYPERDATATYPE_STRING, HYPERDATATYPE_STRING);
static hyperdex::index_map i_map_string_int64(HYPERDATATYPE_STRING, HYPERDATATYPE_INT64);
static hyperdex::index_map i_map_string_float(HYPERDATATYPE_STRING, HYPERDATATYPE_FLOAT);
static hyperdex::index_map i_map_int64_string(HYPERDATATYPE_INT64, HYPERDATATYPE_STRING);
static hyperdex::index_map i_map_int64_int64(HYPERDATATYPE_INT64, HYPERDATATYPE_INT64);
static hyperdex::index_map i_map_int64_float(HYPERDATATYPE_INT64, HYPERDATATYPE_FLOAT);
static hyperdex::index_map i_map_float_string(HYPERDATATYPE_FLOAT, HYPERDATATYPE_STRING);
static hyperdex::index_map i_map_float_int64(HYPERDATATYPE_FLOAT, HYPERDATATYPE_INT64);
static hyperdex::index_map i_map_float_float(HYPERDATATYPE_FLOAT, HYPERDATATYPE_FLOAT);

index_info*
index_info :: lookup(hyperdatatype datatype)
{
    switch (datatype)
    {
        case HYPERDATATYPE_STRING:
            return &i_string;
        case HYPERDATATYPE_INT64:
            return &i_int64;
        case HYPERDATATYPE_FLOAT:
            return &i_float;
        case HYPERDATATYPE_LIST_STRING:
            return &i_list_string;
        case HYPERDATATYPE_LIST_INT64:
            return &i_list_int64;
        case HYPERDATATYPE_LIST_FLOAT:
            return &i_list_float;
        case HYPERDATATYPE_SET_STRING:
            return &i_set_string;
        case HYPERDATATYPE_SET_INT64:
            return &i_set_int64;
        case HYPERDATATYPE_SET_FLOAT:
            return &i_set_float;
        case HYPERDATATYPE_MAP_STRING_STRING:
            return &i_map_string_string;
        case HYPERDATATYPE_MAP_STRING_INT64:
            return &i_map_string_int64;
        case HYPERDATATYPE_MAP_STRING_FLOAT:
            return &i_map_string_float;
        case HYPERDATATYPE_MAP_INT64_STRING:
            return &i_map_int64_string;
        case HYPERDATATYPE_MAP_INT64_INT64:
            return &i_map_int64_int64;
        case HYPERDATATYPE_MAP_INT64_FLOAT:
            return &i_map_int64_float;
        case HYPERDATATYPE_MAP_FLOAT_STRING:
            return &i_map_float_string;
        case HYPERDATATYPE_MAP_FLOAT_INT64:
            return &i_map_float_int64;
        case HYPERDATATYPE_MAP_FLOAT_FLOAT:
            return &i_map_float_float;
        case HYPERDATATYPE_GENERIC:
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

index_info :: index_info()
{
}

index_info :: ~index_info() throw ()
{
}

datalayer::index_iterator*
index_info :: iterator_from_range(leveldb_snapshot_ptr,
                                  const region_id&,
                                  const range&,
                                  index_info*)
{
    return NULL;
}

datalayer::index_iterator*
index_info :: iterator_from_check(leveldb_snapshot_ptr,
                                  const region_id&,
                                  const attribute_check&,
                                  index_info*)
{
    return NULL;
}
