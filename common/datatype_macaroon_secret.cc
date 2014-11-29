// Copyright (c) 2014, Cornell University
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

// HyperDex
#include "common/datatype_macaroon_secret.h"

using hyperdex::datatype_macaroon_secret;

datatype_macaroon_secret :: datatype_macaroon_secret()
{
}

datatype_macaroon_secret :: ~datatype_macaroon_secret() throw ()
{
}

hyperdatatype
datatype_macaroon_secret :: datatype() const
{
    return HYPERDATATYPE_STRING;
}

bool
datatype_macaroon_secret :: validate(const e::slice&) const
{
    return true;
}

bool
datatype_macaroon_secret :: check_args(const funcall& func) const
{
    return func.arg1_datatype == HYPERDATATYPE_MACAROON_SECRET &&
           validate(func.arg1) && func.name == FUNC_SET;
}

uint8_t*
datatype_macaroon_secret :: apply(const e::slice& old_value,
                                  const funcall* funcs, size_t funcs_sz,
                                  uint8_t* writeto)
{
    uint8_t* const ptr = writeto;
    size_t sz = old_value.size();
    memmove(ptr, old_value.data(), sz);

    for (size_t i = 0; i < funcs_sz; ++i)
    {
        switch (funcs[i].name)
        {
            case FUNC_SET:
                // Overwrite
                memmove(ptr, funcs[i].arg1.data(), funcs[i].arg1.size());
                sz = funcs[i].arg1.size();
                break;
            case FUNC_FAIL:
            case FUNC_STRING_PREPEND:
            case FUNC_STRING_APPEND:
            case FUNC_NUM_ADD:
            case FUNC_NUM_SUB:
            case FUNC_NUM_MUL:
            case FUNC_NUM_DIV:
            case FUNC_NUM_MOD:
            case FUNC_NUM_AND:
            case FUNC_NUM_OR:
            case FUNC_NUM_XOR:
            case FUNC_LIST_LPUSH:
            case FUNC_LIST_RPUSH:
            case FUNC_SET_ADD:
            case FUNC_SET_REMOVE:
            case FUNC_SET_INTERSECT:
            case FUNC_SET_UNION:
            case FUNC_MAP_ADD:
            case FUNC_MAP_REMOVE:
            default:
                abort();
        }
    }

    return ptr + sz;
}
