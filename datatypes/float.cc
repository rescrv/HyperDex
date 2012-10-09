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

#define __STDC_LIMIT_MACROS

// e
#include <e/endian.h>
#include <e/safe_math.h>

// HyperDex
#include "datatypes/float.h"

using hyperdex::funcall;

bool
validate_as_float(const e::slice& value)
{
    return value.size() == 8;
}

uint8_t*
apply_float(const e::slice& old_value,
            const funcall* funcs, size_t num_funcs,
            uint8_t* writeto, microerror* error)
{
    double number = 0.0;

    if (old_value.size())
    {
        e::unpackdoublele(old_value.data(), &number);
    }    

    for (size_t i = 0; i < num_funcs; ++i)
    {
        const funcall* func = funcs + i;

        if (funcs[i].arg1_datatype != HYPERDATATYPE_FLOAT)
        {
            *error = MICROERR_WRONGTYPE;
            return NULL;
        }

        if (!validate_as_float(funcs[i].arg1))
        {
            *error = MICROERR_MALFORMED;
            return NULL;
        }

        double arg;
        e::unpackdoublele(func->arg1.data(), &arg);

        switch (func->name)
        {
            case hyperdex::FUNC_SET:
                number = arg;
                break;
            case hyperdex::FUNC_NUM_ADD:
                number += arg;
                break;
            case hyperdex::FUNC_NUM_SUB:
                number -= arg;
                break;
            case hyperdex::FUNC_NUM_MUL:
                number *= arg;
                break;
            case hyperdex::FUNC_NUM_DIV:
                number /= arg;
                break;
            case hyperdex::FUNC_NUM_MOD:
            case hyperdex::FUNC_NUM_AND:
            case hyperdex::FUNC_NUM_OR:
            case hyperdex::FUNC_NUM_XOR:
            case hyperdex::FUNC_STRING_APPEND:
            case hyperdex::FUNC_STRING_PREPEND:
            case hyperdex::FUNC_LIST_LPUSH:
            case hyperdex::FUNC_LIST_RPUSH:
            case hyperdex::FUNC_SET_ADD:
            case hyperdex::FUNC_SET_REMOVE:
            case hyperdex::FUNC_SET_INTERSECT:
            case hyperdex::FUNC_SET_UNION:
            case hyperdex::FUNC_MAP_ADD:
            case hyperdex::FUNC_MAP_REMOVE:
            case hyperdex::FUNC_FAIL:
            default:
                *error = MICROERR_WRONGACTION;
                return NULL;
        }
    }

    return e::packdoublele(number, writeto);
}
