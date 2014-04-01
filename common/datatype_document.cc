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

// e
#include <e/endian.h>
#include <e/guard.h>

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

bool
datatype_document :: document()
{
    return true;
}

bool
datatype_document :: document_check(const attribute_check& check,
                                    const e::slice& doc)
{
    if (check.datatype == HYPERDATATYPE_DOCUMENT)
    {
        return check.predicate == HYPERPREDICATE_EQUALS &&
               check.value == doc;
    }

    const char* path = reinterpret_cast<const char*>(check.value.data());
    size_t path_sz = strnlen(path, check.value.size());

    if (path_sz >= check.value.size())
    {
        return false;
    }

    hyperdatatype hint = CONTAINER_ELEM(check.datatype);
    hyperdatatype type;
    std::vector<char> scratch;
    e::slice value;

    if (!parse_path(path, path + path_sz, doc, hint, &type, &scratch, &value))
    {
        return false;
    }

    attribute_check new_check;
    new_check.attr      = check.attr;
    new_check.value     = check.value;
    new_check.datatype  = check.datatype;
    new_check.predicate = check.predicate;
    new_check.value.advance(path_sz + 1);
    return passes_attribute_check(type, new_check, value);
}

bool
datatype_document :: parse_path(const char* path,
                                const char* const end,
                                const e::slice& doc,
                                hyperdatatype hint,
                                hyperdatatype* type,
                                std::vector<char>* scratch,
                                e::slice* value)
{
    json_tokener* tok = json_tokener_new();

    if (!tok)
    {
        throw std::bad_alloc();
    }

    e::guard gtok = e::makeguard(json_tokener_free, tok);
    gtok.use_variable();
    const char* data = reinterpret_cast<const char*>(doc.data());
    json_object* obj = json_tokener_parse_ex(tok, data, doc.size());

    if (!obj)
    {
        return false;
    }

    e::guard gobj = e::makeguard(json_object_put, obj);
    gobj.use_variable();

    if (json_tokener_get_error(tok) != json_tokener_success ||
        tok->char_offset != static_cast<ssize_t>(doc.size()))
    {
        return false;
    }

    json_object* parent = obj;

    // Iterate until we hit the last nested object.  For example, if the
    // starting document is {foo: {bar: {baz: 5}}}, we'll break out of this loop
    // when parent=5 and path="baz"
    //
    // If the requested path is not found, we break;
    while (path < end)
    {
        const char* dot = static_cast<const char*>(memchr(path, '.', end - path));
        dot = dot ? dot : end;
        std::string key(path, dot);
        json_object* child;

        if (!json_object_is_type(parent, json_type_object) ||
            !json_object_object_get_ex(parent, key.c_str(), &child))
        {
            return false;
        }

        parent = child;
        path = dot + 1;
    }

    if (json_object_is_type(parent, json_type_double) ||
        json_object_is_type(parent, json_type_int))
    {
        const size_t number_sz = sizeof(double) > sizeof(int64_t)
                               ? sizeof(double) : sizeof(int64_t);

        if (scratch->size() < number_sz)
        {
            scratch->resize(number_sz);
        }

        if (hint == HYPERDATATYPE_INT64)
        {
            int64_t i = json_object_get_int64(parent);
            e::pack64le(i, &(*scratch)[0]);
            *type = HYPERDATATYPE_INT64;
            *value = e::slice(&(*scratch)[0], sizeof(int64_t));
            return true;
        }
        else
        {
            double d = json_object_get_double(parent);
            e::packdoublele(d, &(*scratch)[0]);
            *type = HYPERDATATYPE_FLOAT;
            *value = e::slice(&(*scratch)[0], sizeof(double));
            return true;
        }
    }
    else if (json_object_is_type(parent, json_type_string))
    {
        size_t str_sz = json_object_get_string_len(parent);
        const char* str = json_object_get_string(parent);

        if (scratch->size() < str_sz)
        {
            scratch->resize(str_sz);
        }

        memmove(&scratch->front(), str, str_sz);
        *type = HYPERDATATYPE_STRING;
        *value = e::slice(&(*scratch)[0], str_sz);
        return true;
    }

    return false;
}
