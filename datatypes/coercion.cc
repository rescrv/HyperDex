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
#include "datatypes/coercion.h"
#include "datatypes/validate.h"

#define CONTAINER_MASK(X) ((X) & 9664)
#define PRIMITIVE1_MASK(X) ((X) & 9223)
#define PRIMITIVE2_MASK(X) ((((X) & 56) >> 3) | ((X) & 9216))

bool
primitive_numeric(hyperdatatype expected,
                  const e::slice& arg1, hyperdatatype arg1_datatype,
                  const e::slice&, hyperdatatype)
{
    return (expected == HYPERDATATYPE_INT64 || expected == HYPERDATATYPE_FLOAT) &&
           expected == arg1_datatype &&
           validate_as_type(arg1, arg1_datatype);
}

bool
primitive_integer(hyperdatatype expected,
                  const e::slice& arg1, hyperdatatype arg1_datatype,
                  const e::slice&, hyperdatatype)
{
    return expected == HYPERDATATYPE_INT64 &&
           expected == arg1_datatype &&
           validate_as_int64(arg1);
}

bool
primitive_string(hyperdatatype expected,
                 const e::slice& arg1, hyperdatatype arg1_datatype,
                 const e::slice&, hyperdatatype)
{
    return expected == HYPERDATATYPE_STRING &&
           expected == arg1_datatype &&
           validate_as_string(arg1);
}

bool
container_list_elem(hyperdatatype expected,
                    const e::slice& arg1, hyperdatatype arg1_datatype,
                    const e::slice&, hyperdatatype)
{
    return CONTAINER_MASK(expected) == HYPERDATATYPE_LIST_GENERIC &&
           PRIMITIVE1_MASK(expected) == arg1_datatype &&
           validate_as_type(arg1, arg1_datatype);
}

bool
container_set_elem(hyperdatatype expected,
                   const e::slice& arg1, hyperdatatype arg1_datatype,
                   const e::slice&, hyperdatatype)
{
    return CONTAINER_MASK(expected) == HYPERDATATYPE_SET_GENERIC &&
           PRIMITIVE1_MASK(expected) == arg1_datatype &&
           validate_as_type(arg1, arg1_datatype);
}

bool
container_set(hyperdatatype expected,
              const e::slice& arg1, hyperdatatype arg1_datatype,
              const e::slice&, hyperdatatype)
{
    return CONTAINER_MASK(expected) == HYPERDATATYPE_SET_GENERIC &&
           (expected == arg1_datatype || arg1_datatype == HYPERDATATYPE_SET_GENERIC) &&
           validate_as_type(arg1, arg1_datatype);
}

bool
container_map(hyperdatatype expected,
              const e::slice& arg1, hyperdatatype arg1_datatype,
              const e::slice& arg2, hyperdatatype arg2_datatype)
{
    return CONTAINER_MASK(expected) == HYPERDATATYPE_MAP_GENERIC &&
           PRIMITIVE2_MASK(expected) == arg2_datatype &&
           PRIMITIVE1_MASK(expected) == arg1_datatype &&
           validate_as_type(arg2, arg2_datatype) &&
           validate_as_type(arg1, arg1_datatype);
}

bool
container_map_key_only(hyperdatatype expected,
                       const e::slice&, hyperdatatype,
                       const e::slice& arg2, hyperdatatype arg2_datatype)
{
    return CONTAINER_MASK(expected) == HYPERDATATYPE_MAP_GENERIC &&
           PRIMITIVE2_MASK(expected) == arg2_datatype &&
           validate_as_type(arg2, arg2_datatype);
}

bool
container_map_value_numeric(hyperdatatype expected,
                            const e::slice& arg1, hyperdatatype arg1_datatype,
                            const e::slice& arg2, hyperdatatype arg2_datatype)
{
    return CONTAINER_MASK(expected) == HYPERDATATYPE_MAP_GENERIC &&
           PRIMITIVE2_MASK(expected) == arg2_datatype &&
           PRIMITIVE1_MASK(expected) == arg1_datatype &&
           (arg1_datatype == HYPERDATATYPE_INT64 || arg1_datatype == HYPERDATATYPE_FLOAT) &&
           validate_as_type(arg2, arg2_datatype) &&
           validate_as_type(arg1, arg1_datatype);
}

bool
container_map_value_integer(hyperdatatype expected,
                            const e::slice& arg1, hyperdatatype arg1_datatype,
                            const e::slice& arg2, hyperdatatype arg2_datatype)
{
    return CONTAINER_MASK(expected) == HYPERDATATYPE_MAP_GENERIC &&
           PRIMITIVE2_MASK(expected) == arg2_datatype &&
           PRIMITIVE1_MASK(expected) == arg1_datatype &&
           arg1_datatype == HYPERDATATYPE_INT64 &&
           validate_as_type(arg2, arg2_datatype) &&
           validate_as_type(arg1, arg1_datatype);
}

bool
container_map_value_string(hyperdatatype expected,
                           const e::slice& arg1, hyperdatatype arg1_datatype,
                           const e::slice& arg2, hyperdatatype arg2_datatype)
{
    return CONTAINER_MASK(expected) == HYPERDATATYPE_MAP_GENERIC &&
           PRIMITIVE2_MASK(expected) == arg2_datatype &&
           PRIMITIVE1_MASK(expected) == arg1_datatype &&
           arg1_datatype == HYPERDATATYPE_STRING &&
           validate_as_type(arg2, arg2_datatype) &&
           validate_as_type(arg1, arg1_datatype);
}

bool
container_implicit_coercion(hyperdatatype expected,
                            const e::slice& arg1, hyperdatatype arg1_datatype,
                            const e::slice&, hyperdatatype)
{
    return container_implicit_coercion(expected, arg1, arg1_datatype);
}

bool
container_implicit_coercion(hyperdatatype expected,
                            const e::slice& arg1, hyperdatatype arg1_datatype)
{
    return CONTAINER_MASK(expected) == CONTAINER_MASK(arg1_datatype) &&
           ((PRIMITIVE1_MASK(expected) == PRIMITIVE1_MASK(arg1_datatype)  &&
             PRIMITIVE2_MASK(expected) == PRIMITIVE2_MASK(arg1_datatype) &&
             validate_as_type(arg1, expected)) ||
            (PRIMITIVE1_MASK(arg1_datatype) == HYPERDATATYPE_GENERIC &&
             PRIMITIVE2_MASK(arg1_datatype) == HYPERDATATYPE_GENERIC &&
             arg1.size() == 0 &&
             (CONTAINER_MASK(expected) == HYPERDATATYPE_LIST_GENERIC ||
              CONTAINER_MASK(expected) == HYPERDATATYPE_SET_GENERIC ||
              CONTAINER_MASK(expected) == HYPERDATATYPE_MAP_GENERIC)));
}
