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

#ifndef datatypes_coercion_h_
#define datatypes_coercion_h_

// e
#include <e/slice.h>

// HyperDex
#include "hyperdex.h"

#define CONTAINER_MASK(X) (CONTAINER_TYPE(X))
#define PRIMITIVE1_MASK(X) (CONTAINER_ELEM(X))
#define PRIMITIVE2_MASK(X) (CONTAINER_KEY(X))

bool
primitive_numeric(hyperdatatype expected,
                  const e::slice& arg1, hyperdatatype arg1_datatype,
                  const e::slice&, hyperdatatype);

bool
primitive_integer(hyperdatatype expected,
                  const e::slice& arg1, hyperdatatype arg1_datatype,
                  const e::slice&, hyperdatatype);

bool
primitive_string(hyperdatatype expected,
                 const e::slice& arg1, hyperdatatype arg1_datatype,
                 const e::slice&, hyperdatatype);

bool
container_list_elem(hyperdatatype expected,
                    const e::slice& arg1, hyperdatatype arg1_datatype,
                    const e::slice&, hyperdatatype);

bool
container_set_elem(hyperdatatype expected,
                   const e::slice& arg1, hyperdatatype arg1_datatype,
                   const e::slice&, hyperdatatype);

bool
container_set(hyperdatatype expected,
              const e::slice& arg1, hyperdatatype arg1_datatype,
              const e::slice&, hyperdatatype);

bool
container_map(hyperdatatype expected,
              const e::slice& arg1, hyperdatatype arg1_datatype,
              const e::slice& arg2, hyperdatatype arg2_datatype);

bool
container_map_key_only(hyperdatatype expected,
                       const e::slice& arg1, hyperdatatype arg1_datatype,
                       const e::slice& arg2, hyperdatatype arg2_datatype);

bool
container_map_value_numeric(hyperdatatype expected,
                            const e::slice& arg1, hyperdatatype arg1_datatype,
                            const e::slice& arg2, hyperdatatype arg2_datatype);

bool
container_map_value_integer(hyperdatatype expected,
                            const e::slice& arg1, hyperdatatype arg1_datatype,
                            const e::slice& arg2, hyperdatatype arg2_datatype);

bool
container_map_value_string(hyperdatatype expected,
                           const e::slice& arg1, hyperdatatype arg1_datatype,
                           const e::slice& arg2, hyperdatatype arg2_datatype);

bool
container_implicit_coercion(hyperdatatype expected,
                            const e::slice& arg1, hyperdatatype arg1_datatype,
                            const e::slice& arg2, hyperdatatype arg2_datatype);

bool
container_implicit_coercion(hyperdatatype expected,
                            const e::slice& arg1, hyperdatatype arg1_datatype);

#endif // datatypes_coercion_h_
