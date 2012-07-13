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
#include "datatypes/string.h"

bool
validate_as_string(const e::slice&)
{
    return true;
}

uint8_t*
apply_string(const e::slice& old_value,
             const microop* ops, size_t num_ops,
             uint8_t* writeto, microerror* error)
{
    uint8_t* const ptr = writeto;
    size_t sz = old_value.size();
    memmove(ptr, old_value.data(), sz);

    for (size_t i = 0; i < num_ops; ++i)
    {
        if (ops[i].arg1_datatype != HYPERDATATYPE_STRING)
        {
            *error = MICROERR_WRONGTYPE;
            return NULL;
        }

        if (!validate_as_string(ops[i].arg1))
        {
            *error = MICROERR_MALFORMED;
            return NULL;
        }

        switch (ops[i].action)
        {
            case OP_SET:
                // Overwrite
                memmove(ptr, ops[i].arg1.data(), ops[i].arg1.size());
                sz = ops[i].arg1.size();
                break;
            case OP_STRING_PREPEND:
                // Shift
                memmove(ptr + ops[i].arg1.size(), ptr, sz);
                // Fill
                memmove(ptr, ops[i].arg1.data(), ops[i].arg1.size());
                // Resize
                sz += ops[i].arg1.size();
                break;
            case OP_STRING_APPEND:
                // Fill
                memmove(ptr + sz, ops[i].arg1.data(), ops[i].arg1.size());
                // Resize
                sz += ops[i].arg1.size();
                break;
            case OP_FAIL:
            case OP_NUM_ADD:
            case OP_NUM_SUB:
            case OP_NUM_MUL:
            case OP_NUM_DIV:
            case OP_NUM_MOD:
            case OP_NUM_AND:
            case OP_NUM_OR:
            case OP_NUM_XOR:
            case OP_LIST_LPUSH:
            case OP_LIST_RPUSH:
            case OP_SET_ADD:
            case OP_SET_REMOVE:
            case OP_SET_INTERSECT:
            case OP_SET_UNION:
            case OP_MAP_ADD:
            case OP_MAP_REMOVE:
            default:
                *error = MICROERR_WRONGACTION;
                return NULL;
        }
    }

    return ptr + sz;
}
