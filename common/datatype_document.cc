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
#include <e/safe_math.h>

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
datatype_document :: validate_old_values(const std::vector<e::slice>& old_values, const funcall& func) const
{
    // we only need to check old values for atomic operations
    switch(func.name)
    {
    case FUNC_DOC_RENAME:
    case FUNC_DOC_UNSET:
    {
        json_object* root = to_json(old_values[0]);

        if(!root)
        {
            return false;
        }

        e::guard gobj = e::makeguard(json_object_put, root);
        gobj.use_variable();

        json_path path(func.arg2.c_str());
        json_object* obj = traverse_path(root, path);

        bool exists = (obj != NULL);

        if (func.name == FUNC_DOC_UNSET)
        {
            //json_object_put(obj);
            return exists;
        }
        else
        {
            std::string new_name(func.arg2.c_str());

            json_path parent_path;
            std::string obj_name;
            path.split_reverse(parent_path, obj_name);

            parent_path.append(new_name);
            json_object* other_obj = traverse_path(root, path);

            bool other_exists = (other_obj == NULL);

            //json_object_put(obj);
            //json_object_put(other_obj);

            return exists && !other_exists;
        }
    }
    case FUNC_NUM_ADD:
    case FUNC_NUM_AND:
    case FUNC_NUM_MAX:
    case FUNC_NUM_MIN:
    case FUNC_NUM_MOD:
    case FUNC_NUM_MUL:
    case FUNC_NUM_SUB:
    case FUNC_NUM_XOR:
    case FUNC_NUM_OR:
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
        json_object* obj = traverse_path(root, path);

        if(!obj)
        {
            // check if a new child can be created
            json_path child_path;
            return (json_object_get_type(get_last_elem_in_path(root, path, child_path)) == json_type_object);
        }
        else if(json_object_get_type(obj) != json_type_int
            && json_object_get_type(obj) != json_type_double)
        {
            // we can only add integers
            return false;
        }
        break;
    }
    case FUNC_STRING_APPEND:
    case FUNC_STRING_PREPEND:
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
        json_object* obj = traverse_path(root, path);

        if(!obj)
        {
            // new child will be created...
            return true;
        }
        else if(json_object_get_type(obj) != json_type_string)
        {
            // we can only add integers
            return false;
        }
        break;
    }
    case FUNC_FAIL:
    case FUNC_SET:
    case FUNC_NUM_DIV:
    case FUNC_LIST_LPUSH:
    case FUNC_LIST_RPUSH:
    case FUNC_SET_ADD:
    case FUNC_SET_REMOVE:
    case FUNC_SET_INTERSECT:
    case FUNC_SET_UNION:
    case FUNC_MAP_ADD:
    case FUNC_MAP_REMOVE:
    default:
        break;
    }

    // No check failed
    return true;
}

bool
datatype_document :: check_args(const funcall& func) const
{
    switch(func.name)
    {
    case FUNC_SET:
    {
        // set (or replace with) a new document
        return func.arg1_datatype == HYPERDATATYPE_DOCUMENT && validate(func.arg1);
    }
    case FUNC_NUM_ADD:
    case FUNC_NUM_SUB:
    case FUNC_NUM_MUL:
    case FUNC_NUM_DIV:
    case FUNC_NUM_XOR:
    case FUNC_NUM_AND:
    case FUNC_NUM_OR:
    case FUNC_NUM_MOD:
    case FUNC_NUM_MIN:
    case FUNC_NUM_MAX:
    {
        // they second argument is a path to the field we want to manipulate
        // (the path is represented as a string)
        return (func.arg1_datatype == HYPERDATATYPE_INT64 || func.arg1_datatype == HYPERDATATYPE_FLOAT)
            && func.arg2_datatype == HYPERDATATYPE_STRING;
    }
    case FUNC_STRING_APPEND:
    case FUNC_STRING_PREPEND:
    {
        // they second argument is a path to the field we want to manipulate
        // (the path is represented as a string)
        return func.arg1_datatype == HYPERDATATYPE_STRING && func.arg2_datatype == HYPERDATATYPE_STRING;
    }
    case FUNC_DOC_UNSET:
    {
        // Key should be a path and the value will not be evaluated (for now)
        return func.arg1_datatype == HYPERDATATYPE_INT64 && func.arg2_datatype == HYPERDATATYPE_STRING;
    }
    case FUNC_DOC_RENAME:
    {
        if(!func.arg1_datatype == HYPERDATATYPE_STRING && func.arg2_datatype == HYPERDATATYPE_STRING)
        {
            return false;
        }

        // New name must not be empty or contain a '.'
        std::string new_path = func.arg1.c_str();
        return !new_path.empty() && new_path.find('.') == std::string::npos;
    }
    case FUNC_FAIL:
    case FUNC_LIST_LPUSH:
    case FUNC_LIST_RPUSH:
    case FUNC_SET_ADD:
    case FUNC_SET_INTERSECT:
    case FUNC_SET_REMOVE:
    case FUNC_SET_UNION:
    case FUNC_MAP_ADD:
    case FUNC_MAP_REMOVE:
    default:
    {
        // Unsupported operation
        return false;
    }
    }
}

uint8_t*
datatype_document :: apply(const e::slice& old_value,
                           const funcall* funcs, size_t funcs_sz,
                           uint8_t* writeto)
{
    e::slice new_value = old_value;

    // To support multiple updates on the same document
    // we reuse the json object
    // This should also save some parsing time
    json_object* root = NULL;

    for (size_t i = 0; i < funcs_sz; ++i)
    {
        const funcall* func = funcs + i;

        switch(func->name)
        {
        case FUNC_SET:
        {
            new_value = func->arg1;
            break;
        }
        case FUNC_DOC_UNSET:
        {
            json_path path(func->arg2.c_str());
            root = root ? root : to_json(old_value);

            if(path.has_subtree())
            {
                json_path parent_path;
                std::string obj_name;
                path.split_reverse(parent_path, obj_name);
                json_object *parent = traverse_path(root, parent_path);

                assert(parent);
                json_object_object_del(parent, obj_name.c_str());
            }
            else
            {
                json_object_object_del(root, path.str().c_str());
            }
            break;
        }
        case FUNC_DOC_RENAME:
        {
            root = root ? root : to_json(old_value);

            std::string new_name(func->arg1.c_str());
            json_path path(func->arg2.c_str());
            json_object *obj = traverse_path(root, path);

            if(path.has_subtree())
            {
                json_path parent_path;
                std::string old_name;
                path.split_reverse(parent_path, old_name);
                json_object *parent = traverse_path(root, parent_path);

                json_object_object_add(parent, new_name.c_str(), obj);
                json_object_object_del(parent, old_name.c_str());
            }
            else
            {
                json_object_object_add(root, new_name.c_str(), obj);
                json_object_object_del(root, path.str().c_str());
            }
            break;
        }
        case FUNC_STRING_PREPEND:
        case FUNC_STRING_APPEND:
        {
            const e::slice& key = funcs[i].arg2;
            const e::slice& val = funcs[i].arg1;

            json_path path(key.c_str());

            const std::string arg(val.cdata(), val.size());

            root = root ? root : to_json(old_value);

            json_object *parent, *obj;
            std::string obj_name;

            get_end(root, path, parent, obj, obj_name);

            std::string str = obj ? json_object_get_string(obj) : "";

            if(func->name == FUNC_STRING_APPEND)
            {
                str = str + arg;
            }
            else
            {
                str = arg + str;
            }

            json_object* new_elem = json_object_new_string(str.c_str());
            json_object_object_add(parent, obj_name.c_str(), new_elem);
            break;
        }
        case FUNC_NUM_ADD:
        case FUNC_NUM_SUB:
        case FUNC_NUM_DIV:
        case FUNC_NUM_MUL:
        case FUNC_NUM_XOR:
        case FUNC_NUM_OR:
        case FUNC_NUM_AND:
        case FUNC_NUM_MOD:
        case FUNC_NUM_MAX:
        case FUNC_NUM_MIN:
        {
            json_path path(funcs[i].arg2.c_str());

            const int64_t arg = *reinterpret_cast<const int64_t*>(funcs[i].arg1.data());

            root = root ? root : to_json(old_value);

            json_object *parent, *obj;
            std::string obj_name;

            get_end(root, path, parent, obj, obj_name);

            int64_t number = obj ? json_object_get_int64(obj) : 0;
            bool success = false;

            if(func->name == FUNC_NUM_ADD)
            {
                success = e::safe_add(number, arg, &number);
            }
            else if(func->name == FUNC_NUM_SUB)
            {
                success = e::safe_sub(number, arg, &number);
            }
            else if(func->name == FUNC_NUM_DIV)
            {
                success = e::safe_div(number, arg, &number);
            }
            else if(func->name == FUNC_NUM_MUL)
            {
                success = e::safe_mul(number, arg, &number);
            }
            else if(func->name == FUNC_NUM_MOD)
            {
                number = number % arg;
                success = true;
            }
            else if(func->name == FUNC_NUM_XOR)
            {
                number = number ^ arg;
                success = true;
            }
            else if(func->name == FUNC_NUM_AND)
            {
                number = number & arg;
                success = true;
            }
            else if(func->name == FUNC_NUM_OR)
            {
                number = number | arg;
                success = true;
            }
            else if(func->name == FUNC_NUM_MAX)
            {
                number = std::max(number, arg);
                success = true;
            }
            else if(func->name == FUNC_NUM_MIN)
            {
                number = std::min(number, arg);
                success = true;
            }

            if(success)
            {
                json_object* new_elem = json_object_new_int64(number);
                json_object_object_add(parent, obj_name.c_str(), new_elem);
            }
            else
            {
                json_object_put(root);
                return NULL;
            }
            break;
        }
        case FUNC_FAIL:
        case FUNC_LIST_LPUSH:
        case FUNC_LIST_RPUSH:
        case FUNC_SET_ADD:
        case FUNC_SET_INTERSECT:
        case FUNC_SET_REMOVE:
        case FUNC_SET_UNION:
        case FUNC_MAP_ADD:
        case FUNC_MAP_REMOVE:
        default:
            abort();
        }
    }

    if(root)
    {
        new_value = json_object_to_json_string(root);
        //json_object_put(root);
    }

    memmove(writeto, new_value.data(), new_value.size());
    return writeto + new_value.size();
}

void
datatype_document :: get_end(const json_object* root, const json_path& path,
                                json_object*& parent, json_object*& obj, std::string& obj_name) const
{
    json_path parent_path;

    if(path.has_subtree())
    {
        path.split_reverse(parent_path, obj_name);

        // Apperantly, there is no easier way in json-c to get the parent
        parent = traverse_path(root, parent_path);
    }
    else
    {
        parent = const_cast<json_object*>(root);
        obj_name = path.str();
    }

    if(!parent)
    {
        // Recursively build the path
        // This might be a little inefficent but should rarely be needed
        json_object *grandparent = NULL;
        std::string parent_name_;
        get_end(root, parent_path, grandparent, parent, parent_name_);

        assert(grandparent != NULL);
        parent = json_object_new_object();
        json_object_object_add(grandparent, parent_name_.c_str(), parent);
    }

    obj = traverse_path(root, path);
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

json_object*
datatype_document :: get_last_elem_in_path(const json_object* parent, const json_path& path, json_path& child_path) const
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

    child_path.append(childname);

    // json_object_object_get_ex also checks if parent is an object
    // for some reason this function want a non-const pointer
    // let us hack around it...
    json_object* child;
    if (!json_object_object_get_ex(const_cast<json_object*>(parent), childname.c_str(), &child))
    {
        return const_cast<json_object*>(parent);
    }

    if(subpath.empty())
    {
        return child;
    }
    else
    {
        return get_last_elem_in_path(child, subpath, child_path);
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
