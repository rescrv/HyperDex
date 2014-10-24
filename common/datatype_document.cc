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
#include "common/key_change.h"

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
datatype_document :: datatype() const
{
    return HYPERDATATYPE_DOCUMENT;
}

bool
datatype_document :: validate(const e::slice& value) const
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
datatype_document :: validate_old_values(const key_change& kc, const std::vector<e::slice>& old_values, const funcall& func) const
{
    // we only need to check old values for atomic operations
    if(func.name == FUNC_NUM_ADD)
    {
        json_object* root = to_json(old_values[0]);

        if(!root)
        {
            return false;
        }

        e::guard gobj = e::makeguard(json_object_put, root);
        gobj.use_variable();

        // Arugment 2 must be the path
        // otherwise, check_args should have caught this
        assert(func.arg2_datatype == HYPERDATATYPE_STRING);

        json_path path(func.arg2.c_str());
        path.make_relative();
        json_object* obj = traverse_path(root, path);

        if(!obj)
        {
            // new child will be created...
            return true;
        }
        else if(json_object_get_type(obj) != json_type_int)
        {
            // we can only add integers
            return false;
        }
    }

    // No check failed
    return true;
}

bool
datatype_document :: check_args(const funcall& func) const
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
        return func.arg1_datatype == HYPERDATATYPE_INT64 && func.arg2_datatype == HYPERDATATYPE_STRING;
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

            json_path path(key.c_str());
            path.make_relative();

            const int64_t addval = static_cast<const int64_t>(*val.data());

            json_object *data = to_json(old_value);
            atomic_add(data, path, addval);

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

void datatype_document :: atomic_add(json_object* root, const json_path& path, const int64_t addval) const
{
    std::string child_name;
    json_object *parent;

    if(path.has_subtree())
    {
        json_path parent_path;
        path.split_reverse(parent_path, child_name);

        // Apperantly, there is no easier way in json-c to get the parent
        parent = traverse_path(root, parent_path);
    }
    else
    {
        parent = root;
        child_name = path.str();
    }

    int64_t data_val = 0;
    json_object* obj = traverse_path(root, path);

    // if object doesnt exist, just create it
    // TODO: we might also create the parent
    if(obj)
    {
        data_val = json_object_get_int64(obj);
    }

    json_object* new_val = json_object_new_int64(data_val + addval);
    json_object_object_add(parent, child_name.c_str(), new_val);
}

bool
datatype_document :: document() const
{
    return true;
}

bool
datatype_document :: document_check(const attribute_check& check,
                                    const e::slice& doc)
{
    // a check that compares the whole document with another document
    if (check.datatype == HYPERDATATYPE_DOCUMENT)
    {
        return check.predicate == HYPERPREDICATE_EQUALS &&
               check.value == doc;
    }
    // we compare/evaluate one value of the document
    else
    {
        // Search for a \0-character and cut the string of after that point
        const char* cstr = reinterpret_cast<const char*>(check.value.data());
        size_t path_sz = strnlen(cstr, check.value.size());

        // If we don't find a null character we terminate
        // because there is no value following the path information
        // so check.value should be in the following form: <path>\0<value>
        if(path_sz >= check.value.size())
        {
            return false;
        }

        json_path path(std::string(cstr, path_sz));

        hyperdatatype hint = CONTAINER_ELEM(check.datatype);
        hyperdatatype type;
        std::vector<char> scratch;
        e::slice value;

        if (!extract_value(path, doc, hint, &type, &scratch, &value))
        {
            return false;
        }

        attribute_check new_check;
        new_check.attr      = check.attr;
        new_check.value     = check.value;
        new_check.datatype  = check.datatype;
        new_check.predicate = check.predicate;
        new_check.value.advance(path.size() + 1);
        return passes_attribute_check(type, new_check, value);
    }
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
        return NULL;
    }

    return obj;
}

json_object*
datatype_document :: traverse_path(const json_object* parent, const json_path& path) const
{
    assert(parent != NULL);

    std::string childname;
    json_path subpath;

    if(!path.has_subtree())
    {
        // we're at the end of the tree
        childname = path.str();
    }
    else
    {
        path.split(childname, subpath);
    }

    // json_object_object_get_ex also checks if parent is an object
    // for some reason this function want a non-const pointer
    // let us hack around it...
    json_object* child;
    if (!json_object_object_get_ex(const_cast<json_object*>(parent), childname.c_str(), &child))
    {
        return NULL;
    }

    if(subpath.empty())
    {
        return child;
    }
    else
    {
        return traverse_path(child, subpath);
    }
}

bool
datatype_document :: extract_value(const json_path& path,
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

    json_object* parent = traverse_path(obj, path);

    if(!parent)
    {
        return false;
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
