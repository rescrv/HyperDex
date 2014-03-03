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

// json-c
#include <json/json.h>

// HyperDex
#include "common/datatype_document.h"

using hyperdex::datatype_info;
using hyperdex::datatype_document;

datatype_document :: datatype_document()
{
}

datatype_document :: ~datatype_document() throw ()
{
}

hyperdatatype
datatype_document :: datatype()
{
    return HYPERDATATYPE_DOCUMENT;
}

bool
datatype_document :: validate(const e::slice& value)
{
    json_tokener* tok = json_tokener_new();

    if (!tok)
    {
        throw std::bad_alloc();
    }

    const char* data = reinterpret_cast<const char*>(value.data());
    json_object* obj = json_tokener_parse_ex(tok, data, value.size());
    bool retval = obj && json_tokener_get_error(tok) == json_tokener_success
                      && tok->char_offset == (ssize_t)value.size();

    if (obj)
    {
        json_object_put(obj);
    }

    if (tok)
    {
        json_tokener_free(tok);
    }

    return retval;
}

bool
datatype_document :: check_args(const funcall& func)
{
    return func.arg1_datatype == HYPERDATATYPE_DOCUMENT &&
           validate(func.arg1) && func.name == FUNC_SET;
}

uint8_t*
datatype_document :: apply(const e::slice& old_value,
                        const funcall* funcs, size_t funcs_sz,
                        uint8_t* writeto)
{
    e::slice new_value = old_value;

    for (size_t i = 0; i < funcs_sz; ++i)
    {
        const funcall* func = funcs + i;
        assert(check_args(*func));
        new_value = func->arg1;
    }

    memmove(writeto, new_value.data(), new_value.size());
    return writeto + new_value.size();
}
