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

// HyperDex
#include "datatypes/alltypes.h"
#include "datatypes/apply.h"

uint8_t*
apply_microops(hyperdatatype type,
               const e::slice& old_value,
               microop* ops, size_t num_ops,
               uint8_t* writeto, microerror* error)
{
    switch (type)
    {
        case HYPERDATATYPE_STRING:
            return apply_string(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_INT64:
            return apply_int64(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_FLOAT:
            return apply_float(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_LIST_STRING:
            return apply_list_string(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_LIST_INT64:
            return apply_list_int64(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_LIST_FLOAT:
            return apply_list_float(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_SET_STRING:
            return apply_set_string(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_SET_INT64:
            return apply_set_int64(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_SET_FLOAT:
            return apply_set_float(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_MAP_STRING_STRING:
            return apply_map_string_string(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_MAP_STRING_INT64:
            return apply_map_string_int64(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_MAP_STRING_FLOAT:
            return apply_map_string_float(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_MAP_INT64_STRING:
            return apply_map_int64_string(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_MAP_INT64_INT64:
            return apply_map_int64_int64(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_MAP_INT64_FLOAT:
            return apply_map_int64_float(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_MAP_FLOAT_STRING:
            return apply_map_float_string(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_MAP_FLOAT_INT64:
            return apply_map_float_int64(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_MAP_FLOAT_FLOAT:
            return apply_map_float_float(old_value, ops, num_ops, writeto, error);
        case HYPERDATATYPE_GENERIC:
        case HYPERDATATYPE_LIST_GENERIC:
        case HYPERDATATYPE_SET_GENERIC:
        case HYPERDATATYPE_MAP_GENERIC:
        case HYPERDATATYPE_MAP_STRING_KEYONLY:
        case HYPERDATATYPE_MAP_INT64_KEYONLY:
        case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
        case HYPERDATATYPE_GARBAGE:
        default:
            *error = MICROERROR;
            return NULL;
    }
}
