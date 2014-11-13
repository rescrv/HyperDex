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
    // FIXME!!
    return true;

    if(value.size() == 0)
    {
        // default value is empty
        // not part of the BSON standard but this is how HyperDex behaves
        return true;
    }

    bson_t b;
    return bson_init_static(&b, reinterpret_cast<const uint8_t*>(value.cdata()), value.size());
}

bool
datatype_document :: validate_old_values(const std::vector<e::slice>& old_values, const funcall& func) const
{
    // we only need to check old values for atomic operations
    switch(func.name)
    {
    case FUNC_DOC_SET:
    {
        json_path path = func.arg2.c_str();

        bson_t b;
        bool inited = bson_init_static(&b, old_values[0].data(), old_values[0].size());

        if(!inited)
        {
            return false;
        }

        bson_iter_t iter, baz;
        assert(bson_iter_init (&iter, &b));

        bool exists = bson_iter_find_descendant(&iter, path.str().c_str(), &baz);
        return !exists;
    }
    case FUNC_DOC_RENAME:
    case FUNC_DOC_UNSET:
    {
        json_path path = func.arg2.c_str();

        bson_t b;
        bool inited = bson_init_static(&b, old_values[0].data(), old_values[0].size());

        if(!inited)
        {
            return false;
        }

        bson_iter_t iter, baz1, baz2;
        assert(bson_iter_init (&iter, &b));

        bool exists = bson_iter_find_descendant(&iter, path.str().c_str(), &baz1);

        if (func.name == FUNC_DOC_UNSET)
        {
            return exists;
        }
        else
        {
            std::string new_name(func.arg2.c_str());

            json_path parent_path;
            std::string obj_name;
            path.split_reverse(parent_path, obj_name);

            parent_path.append(new_name);

            bool other_exists = bson_iter_find_descendant(&iter, path.str().c_str(), &baz2);

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
        json_path path = func.arg2.c_str();

        bson_t b;
        bson_init_static(&b, old_values[0].data(), old_values[0].size());

        bson_iter_t iter, baz;
        assert(bson_iter_init (&iter, &b));

        if (bson_iter_find_descendant (&iter, path.str().c_str(), &baz))
        {
            return (bson_iter_type(&baz) == BSON_TYPE_INT64 || bson_iter_type(&baz) == BSON_TYPE_INT32);
        }
        else
        {
            while(!path.empty() && path.has_subtree())
            {
                std::string child_name;
                path.split_reverse(path, child_name);

                bson_iter_init (&iter, &b);
                if(bson_iter_find_descendant (&iter, path.str().c_str(), &baz))
                {
                    return BSON_ITER_HOLDS_DOCUMENT(&baz);
                }
            }

            return true;
        }
        break;
    }
    case FUNC_STRING_APPEND:
    case FUNC_STRING_PREPEND:
    {
        json_path path = func.arg2.c_str();

        bson_t b;
        bson_init_static(&b, old_values[0].data(), old_values[0].size());

        bson_iter_t iter, baz;
        assert(bson_iter_init (&iter, &b));

        if (bson_iter_find_descendant (&iter, path.str().c_str(), &baz))
        {
            return (bson_iter_type(&baz) == BSON_TYPE_UTF8);
        }
        else
        {
            while(!path.empty() && path.has_subtree())
            {
                std::string child_name;
                path.split_reverse(path, child_name);

                bson_iter_init (&iter, &b);
                if(bson_iter_find_descendant (&iter, path.str().c_str(), &baz))
                {
                    return BSON_ITER_HOLDS_DOCUMENT(&baz);
                }
            }

            return true;
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
    case FUNC_DOC_SET:
    {
        if (!func.arg2_datatype == HYPERDATATYPE_STRING)
        {
            // not valid path
            return false;
        }

        return func.arg1_datatype == HYPERDATATYPE_INT64 || func.arg1_datatype == HYPERDATATYPE_STRING || func.arg1_datatype == HYPERDATATYPE_DOCUMENT;
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

bson_t*
datatype_document :: add_or_replace_string(const bson_t* old_document, const json_path& path, const std::string& new_value) const
{
    bson_iter_t iter;

    if(!bson_iter_init (&iter, old_document))
    {
        return NULL;
    }

    bson_t *new_doc = bson_new();

    add_or_replace_string_recurse(path, new_value, new_doc, &iter);
    return new_doc;
}

bson_t*
datatype_document :: rename_value(const bson_t* old_document, const json_path& path, const std::string& new_name) const
{
    bson_iter_t iter;

    if(!bson_iter_init (&iter, old_document))
    {
        return NULL;
    }

    bson_t *new_doc = bson_new();

    rename_value_recurse(path, new_name, new_doc, &iter);
    return new_doc;
}

void
datatype_document :: rename_value_recurse(const json_path& path, const std::string& new_name, bson_t* parent, bson_iter_t* iter) const
{
    while (iter && bson_iter_next(iter))
    {
        bson_type_t type = bson_iter_type(iter);
        std::string key = bson_iter_key(iter);

        if(key == path.str())
        {
            // found it!
            const bson_value_t *value = bson_iter_value(iter);
            bson_append_value(parent, new_name.c_str(), new_name.size(), value);
        }
        else if(type == BSON_TYPE_DOCUMENT)
        {
            json_path subpath;
            std::string root_name;

            bson_iter_t sub_iter;
            bson_iter_recurse(iter, &sub_iter);

            bson_t *child = bson_new();
            bson_append_document_begin(parent, key.c_str(), key.size(), child);

            if(path.has_subtree() && path.split(root_name, subpath)
                && root_name == key)
            {
                rename_value_recurse(subpath, new_name, child, &sub_iter);
            }
            else
            {
                // This is not the path you are looking for...
                rename_value_recurse("", "", child, &sub_iter);
            }

            bson_append_document_end(parent, child);
        }
        else
        {
            const bson_value_t *value = bson_iter_value(iter);
            bson_append_value(parent, key.c_str(), key.size(), value);
        }
    }
}

bson_t*
datatype_document :: unset_value(const bson_t* old_document, const json_path& path) const
{
    bson_iter_t iter;

    if(!bson_iter_init (&iter, old_document))
    {
        return NULL;
    }

    bson_t *new_doc = bson_new();

    unset_value_recurse(path, new_doc, &iter);
    return new_doc;
}

void
datatype_document :: unset_value_recurse(const json_path& path, bson_t* parent, bson_iter_t* iter) const
{
    while (iter && bson_iter_next(iter))
    {
        bson_type_t type = bson_iter_type(iter);
        std::string key = bson_iter_key(iter);

        if(key == path.str())
        {
            // skip
        }
        else if(type == BSON_TYPE_DOCUMENT)
        {
            json_path subpath;
            std::string root_name;

            bson_iter_t sub_iter;
            bson_iter_recurse(iter, &sub_iter);

            bson_t *child = bson_new();
            bson_append_document_begin(parent, key.c_str(), key.size(), child);

            if(path.has_subtree() && path.split(root_name, subpath)
                && root_name == key)
            {
                unset_value_recurse(subpath, child, &sub_iter);
            }
            else
            {
                // This is not the path you are looking for...
                unset_value_recurse("", child, &sub_iter);
            }

            bson_append_document_end(parent, child);
        }
        else
        {
            const bson_value_t *value = bson_iter_value(iter);
            bson_append_value(parent, key.c_str(), key.size(), value);
        }
    }
}

bson_t*
datatype_document :: add_or_replace_document(const bson_t* old_document, const json_path& path, const bson_value_t *new_value) const
{
    bson_iter_t iter;

    if(!bson_iter_init (&iter, old_document))
    {
        return NULL;
    }

    bson_t *new_doc = bson_new();

    add_or_replace_document_recurse(path, new_value, new_doc, &iter);
    return new_doc;
}

void
datatype_document :: add_or_replace_document_recurse(const json_path& path, const bson_value_t *value,
                                    bson_t* parent, bson_iter_t* iter) const
{
    bool found = false;

    // There might be not iterator if we have to create the subtree
    while (iter && bson_iter_next(iter))
    {
        bson_type_t type = bson_iter_type(iter);
        std::string key = bson_iter_key(iter);

        if(type == BSON_TYPE_DOCUMENT)
        {
            json_path subpath;
            std::string root_name;
            assert(path.split(root_name, subpath));

            bson_iter_t sub_iter;
            bson_iter_recurse(iter, &sub_iter);

            bson_t *child = bson_new();
            bson_append_document_begin(parent, key.c_str(), key.size(), child);

            if(root_name == key)
            {
                found = true;
                add_or_replace_int64_recurse(subpath, new_value, child, &sub_iter);
            }
            else
            {
                add_or_replace_int64_recurse("", new_value, child, &sub_iter);
            }

            bson_append_document_end(parent, child);
        }
        else if(type == BSON_TYPE_ARRAY || type == BSON_TYPE_INT32 || type == BSON_TYPE_DOUBLE || type == BSON_TYPE_UTF8)
        {
            const bson_value_t *value = bson_iter_value(iter);
            bson_append_value(parent, key.c_str(), key.size(), value);
        }
        else
        {
            abort();
        }
    }

    if(!found && !path.empty())
    {
        if(path.has_subtree())
        {
            json_path subpath;
            std::string root_name;
            path.split(root_name, subpath);

            bson_t *child = bson_new();

            bson_append_document_begin(parent, root_name.c_str(), root_name.size(), child);
            replace_int64_recurse(subpath, new_value, child, NULL);
            bson_append_document_end(parent, child);
        }
        else
        {
            bson_append_int64(parent, path.str().c_str(), path.str().size(), new_value);
        }
    }
}

bson_t*
datatype_document :: add_or_replace_int64(const bson_t* old_document, const json_path& path, const int64_t new_value) const
{
    bson_iter_t iter;

    if(!bson_iter_init (&iter, old_document))
    {
        return NULL;
    }

    bson_t *new_doc = bson_new();

    replace_int64_recurse(path, new_value, new_doc, &iter);
    return new_doc;
}

void
datatype_document :: replace_int64_recurse(const json_path& path, const int64_t new_value,
                                    bson_t* parent, bson_iter_t* iter) const
{
    bool found = false;

    // There might be not iterator if we have to create the subtree
    while (iter && bson_iter_next(iter))
    {
        bson_type_t type = bson_iter_type(iter);
        std::string key = bson_iter_key(iter);

        if(type == BSON_TYPE_INT64)
        {
            if(path.str() == key)
            {
                // We found it!
                found = true;
                bson_append_int64(parent, key.c_str(), key.size(), new_value);
            }
            else
            {
                bson_append_int64(parent, key.c_str(), key.size(), bson_iter_int64(iter));
            }
        }
        else if(type == BSON_TYPE_DOCUMENT)
        {
            json_path subpath;
            std::string root_name;
            assert(path.split(root_name, subpath));

            bson_iter_t sub_iter;
            bson_iter_recurse(iter, &sub_iter);

            bson_t *child = bson_new();
            bson_append_document_begin(parent, key.c_str(), key.size(), child);

            if(root_name == key)
            {
                found = true;
                replace_int64_recurse(subpath, new_value, child, &sub_iter);
            }
            else
            {
                replace_int64_recurse("", new_value, child, &sub_iter);
            }

            bson_append_document_end(parent, child);
        }
        else if(type == BSON_TYPE_ARRAY || type == BSON_TYPE_INT32 || type == BSON_TYPE_DOUBLE || type == BSON_TYPE_UTF8)
        {
            const bson_value_t *value = bson_iter_value(iter);
            bson_append_value(parent, key.c_str(), key.size(), value);
        }
        else
        {
            abort();
        }
    }

    if(!found && !path.empty())
    {
        if(path.has_subtree())
        {
            json_path subpath;
            std::string root_name;
            path.split(root_name, subpath);

            bson_t *child = bson_new();

            bson_append_document_begin(parent, root_name.c_str(), root_name.size(), child);
            replace_int64_recurse(subpath, new_value, child, NULL);
            bson_append_document_end(parent, child);
        }
        else
        {
            bson_append_int64(parent, path.str().c_str(), path.str().size(), new_value);
        }
    }
}

void
datatype_document :: replace_string_recurse(const json_path& path, const std::string& new_value,
                                    bson_t* parent, bson_iter_t* iter) const
{
    bool found = false;

    // There might be not iterator if we have to create the subtree
    while (iter && bson_iter_next(iter))
    {
        bson_type_t type = bson_iter_type(iter);
        std::string key = bson_iter_key(iter);

        if(type == BSON_TYPE_UTF8)
        {
            if(path.str() == key)
            {
                // We found it!
                found = true;
                bson_append_utf8(parent, key.c_str(), key.size(), new_value.c_str(), new_value.size());
            }
            else
            {
                uint32_t len = 0;
                const char* str = bson_iter_utf8(iter, &len);
                bson_append_utf8(parent, key.c_str(), key.size(), str, len);
            }
        }
        else if(type == BSON_TYPE_DOCUMENT)
        {
            json_path subpath;
            std::string root_name;

            bson_iter_t sub_iter;
            bson_iter_recurse(iter, &sub_iter);

            bson_t *child = bson_new();
            bson_append_document_begin(parent, key.c_str(), key.size(), child);

            if(path.has_subtree() && path.split(root_name, subpath)
                && root_name == key)
            {
                found = true;
                replace_string_recurse(subpath, new_value, child, &sub_iter);
            }
            else
            {
                // This is not the path you are looking for...
                replace_string_recurse("", new_value, child, &sub_iter);
            }

            bson_append_document_end(parent, child);
        }
        else if(type == BSON_TYPE_ARRAY || type == BSON_TYPE_INT32 || type == BSON_TYPE_INT64 || type == BSON_TYPE_DOUBLE)
        {
            const bson_value_t *value = bson_iter_value(iter);
            bson_append_value(parent, key.c_str(), key.size(), value);
        }
        else
        {
            abort();
        }
    }

    if(!found && !path.empty())
    {
        if(path.has_subtree())
        {
            json_path subpath;
            std::string root_name;
            path.split(root_name, subpath);

            bson_t *child = bson_new();

            bson_append_document_begin(parent, root_name.c_str(), root_name.size(), child);
            replace_string_recurse(subpath, new_value, child, NULL);
            bson_append_document_end(parent, child);
        }
        else
        {
            bson_append_utf8(parent, path.str().c_str(), path.str().size(), new_value.c_str(), new_value.size());
        }
    }
}

uint8_t*
datatype_document :: apply(const e::slice& old_value,
                           const funcall* funcs, size_t funcs_sz,
                           uint8_t* writeto)
{
    // Prevent copying on SET
    if(funcs_sz == 1 && funcs[0].name == FUNC_SET)
    {
        size_t len = funcs[0].arg1.size();
        memmove(writeto, funcs[0].arg1.data(), len);
        return writeto + len;
    }

    e::slice new_value = old_value;

    // To support multiple updates on the same document
    // we reuse the same object
    bson_t *bson_root = NULL;

    for (size_t i = 0; i < funcs_sz; ++i)
    {
        const funcall& func = *(funcs + i);

        switch(func.name)
        {
        case FUNC_SET:
        {
            // this is not supported
            // there should be only a single func call when set happens
            abort();
            break;
        }
        case FUNC_DOC_SET:
        {
            bson_root = bson_root ? bson_root : bson_new_from_data(old_value.data(), old_value.size());
            std::string path = func.arg2.c_str();

            bson_t *new_doc = NULL;

            if(func.arg1_datatype == HYPERDATATYPE_STRING)
            {
                std::string arg(func.arg1.cdata(), func.arg1.size());
                new_doc = add_or_replace_string(bson_root, path, arg);
            }
            else if(func.arg1_datatype == HYPERDATATYPE_INT64)
            {
                int64_t arg = reinterpret_cast<const int64_t*>(func.arg1.data())[0];
                new_doc = add_or_replace_int64(bson_root, path, arg);
            }
            else if(func.arg1_datatype == HYPERDATATYPE_DOCUMENT)
            {
                bson_t bson;
                bson_init_static(&bson, func.arg1.data(), func.arg1.size());

                bson_iter_t iter;
                bson_init_iter(&bson, &iter);

                bson_iter_value()

                new_doc = add_or_replace_document(bson_root, path, )
            }

            if(new_doc == NULL)
            {
                abort();
            }
            else
            {
                bson_destroy(bson_root);
                bson_root = new_doc;
            }
            break;
        }
        case FUNC_DOC_UNSET:
        {
            bson_root = bson_root ? bson_root : bson_new_from_data(old_value.data(), old_value.size());
            std::string path = func.arg2.c_str();
            bson_t* new_doc = unset_value(bson_root, path);

            if(!new_doc)
            {
                abort();
            }

            bson_destroy(bson_root);
            bson_root = new_doc;
            break;
        }
        case FUNC_DOC_RENAME:
        {
            std::string new_name(func.arg1.cdata(), func.arg1.size());
            std::string path = func.arg2.c_str();

            bson_root = bson_root ? bson_root : bson_new_from_data(old_value.data(), old_value.size());
            bson_t* new_doc = rename_value(bson_root, path, new_name);

            if(!new_doc)
            {
                abort();
            }

            bson_destroy(bson_root);
            bson_root = new_doc;
            break;
        }
        case FUNC_STRING_PREPEND:
        case FUNC_STRING_APPEND:
        {
            std::string arg(func.arg1.cdata(), func.arg1.size());
            json_path path = func.arg2.c_str();

            bson_root = bson_root ? bson_root : bson_new_from_data(old_value.data(), old_value.size());

            bson_iter_t iter, baz;
            assert(bson_iter_init (&iter, bson_root));

            uint32_t size = 0;
            std::string str = "";

            if (bson_iter_find_descendant (&iter, path.str().c_str(), &baz))
            {
                str = bson_iter_utf8(&baz, &size);
            }

            if(func.name == FUNC_STRING_APPEND)
            {
                str = str + arg;
            }
            else
            {
                str = arg + str;
            }

            bson_root = bson_root ? bson_root : bson_new_from_data(old_value.data(), old_value.size());
            bson_t* new_doc = add_or_replace_string(bson_root, path, str);

            if(!new_doc)
            {
                abort();
            }

            bson_destroy(bson_root);
            bson_root = new_doc;
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
            int64_t arg = reinterpret_cast<const int64_t*>(func.arg1.data())[0];
            json_path path = func.arg2.c_str();

            bson_root = bson_root ? bson_root : bson_new_from_data(old_value.data(), old_value.size());

            bson_iter_t iter, baz;
            assert(bson_iter_init (&iter, bson_root));

            int64_t number = 0;
            bool is32bit = false;
            bool field_exists = bson_iter_find_descendant (&iter, path.str().c_str(), &baz);

            if (field_exists)
            {
                if(BSON_ITER_HOLDS_INT64(&baz))
                {
                    number = bson_iter_int64(&baz);
                }
                else if(BSON_ITER_HOLDS_INT32(&baz))
                {
                    number = bson_iter_int32(&baz);
                    is32bit = true;
                }
                else
                {
                    abort();
                }
            }

            bool success = false;

            if(func.name == FUNC_NUM_ADD)
            {
                success = e::safe_add(number, arg, &number);
            }
            else if(func.name == FUNC_NUM_SUB)
            {
                success = e::safe_sub(number, arg, &number);
            }
            else if(func.name == FUNC_NUM_DIV)
            {
                success = e::safe_div(number, arg, &number);
            }
            else if(func.name == FUNC_NUM_MUL)
            {
                success = e::safe_mul(number, arg, &number);
            }
            else if(func.name == FUNC_NUM_MOD)
            {
                number = number % arg;
                success = true;
            }
            else if(func.name == FUNC_NUM_XOR)
            {
                number = number ^ arg;
                success = true;
            }
            else if(func.name == FUNC_NUM_AND)
            {
                number = number & arg;
                success = true;
            }
            else if(func.name == FUNC_NUM_OR)
            {
                number = number | arg;
                success = true;
            }
            else if(func.name == FUNC_NUM_MAX)
            {
                number = std::max(number, arg);
                success = true;
            }
            else if(func.name == FUNC_NUM_MIN)
            {
                number = std::min(number, arg);
                success = true;
            }

            if(success)
            {
                if(field_exists)
                {
                    if(is32bit)
                    {
                        //FIXME check for buffer overflow
                        bson_iter_overwrite_int32(&baz, (int32_t)number);
                    }
                    else
                    {
                        bson_iter_overwrite_int64(&baz, number);
                    }
                }
                else
                {
                    bson_t* new_doc = add_or_replace_int64(bson_root, path, number);

                    if(!new_doc)
                    {
                        abort();
                    }

                    bson_destroy(bson_root);
                    bson_root = new_doc;
                }
            }
            else
            {
                bson_destroy(bson_root);
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

    if(bson_root)
    {
        size_t len = bson_root->len;
        const uint8_t* data = bson_get_data(bson_root);

        memmove(writeto, data, len);
        bson_destroy(bson_root);

        return writeto + len;
    }
    else
    {
        abort();
        return NULL;
    }
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

        std::string path(std::string(cstr, path_sz));

        hyperdatatype type;
        std::vector<char> scratch;
        e::slice value;

        if (!extract_value(path, doc, &type, &scratch, &value))
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
datatype_document :: extract_value(const std::string& path,
                                const e::slice& doc,
                                hyperdatatype* type,
                                std::vector<char>* scratch,
                                e::slice* value)
{
    bson_t b;
    bson_iter_t iter, baz;

    bson_init_static(&b, doc.data(), doc.size());

    if(!bson_iter_init (&iter, &b))
    {
        return false;
    }

    if(!bson_iter_find_descendant (&iter, path.c_str(), &baz))
    {
        return false;
    }

    bson_type_t btype = bson_iter_type(&baz);

    if (btype == BSON_TYPE_DOUBLE ||
        btype == BSON_TYPE_INT64)
    {
        const size_t number_sz = sizeof(double) > sizeof(int64_t)
                               ? sizeof(double) : sizeof(int64_t);

        if (scratch->size() < number_sz)
        {
            scratch->resize(number_sz);
        }

        if (btype == BSON_TYPE_INT64)
        {
            int64_t i = bson_iter_int64(&baz);
            e::pack64le(i, &(*scratch)[0]);
            *type = HYPERDATATYPE_INT64;
            *value = e::slice(&(*scratch)[0], sizeof(int64_t));
            return true;
        }
        else
        {
            double d = bson_iter_double(&baz);
            e::packdoublele(d, &(*scratch)[0]);
            *type = HYPERDATATYPE_FLOAT;
            *value = e::slice(&(*scratch)[0], sizeof(double));
            return true;
        }
    }
    else if (btype == BSON_TYPE_UTF8)
    {
        uint32_t str_sz = 0;
        const char* str = bson_iter_utf8(&baz, &str_sz);

        if (scratch->size() < str_sz)
        {
            scratch->resize(str_sz);
        }

        memmove(&scratch->front(), str, str_sz);
        *type = HYPERDATATYPE_STRING;
        *value = e::slice(&(*scratch)[0], str_sz);
        return true;
    }
    else
    {
        // Unsupported BSON type
        return false;
    }
}
