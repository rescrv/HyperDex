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

// e
#include <e/endian.h>

// HyperDex
#include "datatypes/alltypes.h"
#include "datatypes/compare.h"
#include "datatypes/step.h"
#include "datatypes/validate.h"

bool
validate_as_type(const e::slice& value, hyperdatatype type)
{
    switch (type)
    {
        case HYPERDATATYPE_GENERIC:
            return true;
        case HYPERDATATYPE_STRING:
            return validate_as_string(value);
        case HYPERDATATYPE_INT64:
            return validate_as_int64(value);
        case HYPERDATATYPE_FLOAT:
            return validate_as_float(value);
        case HYPERDATATYPE_LIST_GENERIC:
            return value.size() == 0;
        case HYPERDATATYPE_LIST_STRING:
            return validate_as_list_string(value);
        case HYPERDATATYPE_LIST_INT64:
            return validate_as_list_int64(value);
        case HYPERDATATYPE_LIST_FLOAT:
            return validate_as_list_float(value);
        case HYPERDATATYPE_SET_GENERIC:
            return value.size() == 0;
        case HYPERDATATYPE_SET_STRING:
            return validate_as_set_string(value);
        case HYPERDATATYPE_SET_INT64:
            return validate_as_set_int64(value);
        case HYPERDATATYPE_SET_FLOAT:
            return validate_as_set_float(value);
        case HYPERDATATYPE_MAP_GENERIC:
            return value.size() == 0;
        case HYPERDATATYPE_MAP_STRING_STRING:
            return validate_as_map_string_string(value);
        case HYPERDATATYPE_MAP_STRING_INT64:
            return validate_as_map_string_int64(value);
        case HYPERDATATYPE_MAP_STRING_FLOAT:
            return validate_as_map_string_float(value);
        case HYPERDATATYPE_MAP_INT64_STRING:
            return validate_as_map_int64_string(value);
        case HYPERDATATYPE_MAP_INT64_INT64:
            return validate_as_map_int64_int64(value);
        case HYPERDATATYPE_MAP_INT64_FLOAT:
            return validate_as_map_int64_float(value);
        case HYPERDATATYPE_MAP_FLOAT_STRING:
            return validate_as_map_float_string(value);
        case HYPERDATATYPE_MAP_FLOAT_INT64:
            return validate_as_map_float_int64(value);
        case HYPERDATATYPE_MAP_FLOAT_FLOAT:
            return validate_as_map_float_float(value);
        case HYPERDATATYPE_MAP_STRING_KEYONLY:
        case HYPERDATATYPE_MAP_INT64_KEYONLY:
        case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
        case HYPERDATATYPE_GARBAGE:
        default:
            abort();
    }
}
