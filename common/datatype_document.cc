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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// HyperDex
#include "common/datatype_document.h"
#include "common/datatype_string.h"
#include "common/datatype_int64.h"

// json-c
#if HAVE_JSON_H
#include <json/json.h>
#elif HAVE_JSON_C_H
#include <json-c/json.h>
#else
#error no suitable json.h found
#endif

// e
#include <e/endian.h>
#include <e/guard.h>


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
datatype_document :: check_path(uint16_t attr, const std::string& path) const
{
    //TODO implement me
    return true;
}

bool
datatype_document :: check_args(const funcall& func)
{
    if(func.name == FUNC_SET)
    {
        // set (or replace with) a new document
        return func.arg1_datatype == HYPERDATATYPE_DOCUMENT && validate(func.arg1);
    }
    else if(func.name == FUNC_NUM_ADD)
    {
        // they second argument is a path to the field we want to manipulate
        // (the path is represented as a string)
        return func.arg1_datatype == HYPERDATATYPE_INT64 && func.arg2_datatype == HYPERDATATYPE_STRING
                && check_path(func.attr, std::string(func.arg2.c_str()));
    }
    else
    {
        // Unsupported operation
        return false;
    }
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

        switch(func->name)
        {
        case FUNC_SET:
        {
            assert(check_args(*func));
            new_value = func->arg1;
            break;
        }
        case FUNC_NUM_ADD:
        {
            const e::slice& key = funcs[i].arg2;
            const e::slice& val = funcs[i].arg1;

            std::string path(key.c_str());

            // cut off the "$."
            std::string relpath = path.substr(2);

            const int64_t addval = static_cast<const int64_t>(*val.data());

            json_object *data = to_json(old_value);
            atomic_add(NULL, NULL, data, relpath, addval);

            new_value = json_object_to_json_string(data);
            break;
        }
        default:
            abort();
        }
    }

    memmove(writeto, new_value.data(), new_value.size());
    return writeto + new_value.size();
}

void datatype_document :: atomic_add(const char* key, json_object* parent, json_object* data, const std::string& path, const int64_t addval) const
{
    json_type type = json_object_get_type(data);

    switch(type)
    {
    case json_type_object:
    {
        lh_table* data_children = json_object_get_object(data);

        // the child is the direct child
        // subpath is the subtree of that child (if any)
        std::string childname;
        std::string subpath;
        int pos = path.find(".");

        if(pos == std::string::npos)
        {
            // we're at the end of the tree
            subpath = "";
            childname = path;
        }
        else
        {
            childname = path.substr(0, pos);
            subpath = path.substr(pos+1);
        }

        lh_entry* data_it = data_children->head;

        // Find the corresponding entry in the original data
        while(data_it != NULL)
        {
            if(std::string((const char*)data_it->k) == childname)
                break; // we found it!
            else
                data_it = data_it->next;
        }

        // we should have caught this in validate()
        assert(data_it != NULL);

        // proceed with the subtree
        atomic_add((const char*)data_it->k, data, (json_object*)data_it->v, subpath, addval);
        break;
    }
    case json_type_int:
    {
        assert(key != NULL);
        assert(parent != NULL);
        assert(path == "");

        int64_t data_val = json_object_get_int64(data);

        json_object* new_val = json_object_new_int64(data_val + addval);
        json_object_object_add(parent, key, new_val);
        break;
    }
    default:
        // unknown json type...
        abort();
    }
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

json_object* datatype_document :: to_json(const e::slice& doc) const
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
        return NULL;
    }

    if (json_tokener_get_error(tok) != json_tokener_success ||
        tok->char_offset != static_cast<ssize_t>(doc.size()))
    {
        assert(json_object_put(obj) == 0);
        return NULL;
    }

    return obj;
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
    json_object* obj = to_json(doc);

    if(!obj)
    {
        return false;
    }

    e::guard gobj = e::makeguard(json_object_put, obj);
    gobj.use_variable();

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
