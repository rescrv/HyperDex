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
    e::slice base = old_value;
    bool set_base = false;
    const microop* prepend = NULL;
    const microop* append = NULL;

    for (size_t i = 0; i < num_ops; ++i)
    {
        switch (ops[i].action)
        {
            case OP_SET:

                if (set_base || prepend || append)
                {
                    *error = MICROERROR;
                    return NULL;
                }

                base = ops[i].arg1;
                set_base = true;
                break;
            case OP_STRING_PREPEND:

                if (set_base || prepend)
                {
                    *error = MICROERROR;
                    return NULL;
                }

                prepend = ops + i;
                break;
            case OP_STRING_APPEND:

                if (set_base || append)
                {
                    *error = MICROERROR;
                    return NULL;
                }

                append = ops + i;
                break;
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
            case OP_FAIL:
            default:
                *error = MICROERROR;
                return NULL;
        }
    }

    if (prepend)
    {
        memmove(writeto, prepend->arg1.data(), prepend->arg1.size());
        writeto += prepend->arg1.size();
    }

    memmove(writeto, base.data(), base.size());
    writeto += base.size();

    if (append)
    {
        memmove(writeto, append->arg1.data(), append->arg1.size());
        writeto += append->arg1.size();
    }

    return writeto;
}
