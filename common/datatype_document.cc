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

// e
#include <e/guard.h>

// Treadstone
#include <treadstone.h>

// HyperDex
#include "common/documents.h"
#include "common/datatype_document.h"
#include "common/datatype_int64.h"
#include "common/datatype_float.h"

inline bool is_numeral(const hyperdatatype& t)
{
    return t == HYPERDATATYPE_FLOAT || t == HYPERDATATYPE_INT64;
}

inline bool is_document_primitive(const hyperdatatype& t)
{
    return is_numeral(t) || t == HYPERDATATYPE_STRING || t == HYPERDATATYPE_DOCUMENT;
}

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
    if (value.size() == 0)
    {
        // empty == "object"
        return true;
    }

    return treadstone_binary_validate(value.data(), value.size()) == 0;
}

bool
datatype_document :: check_args(const funcall& func) const
{
    // A transformation that either sets (all/part of) the document, or
    // pushes/pops an array (that is/within) the document.
    if (is_document_primitive(func.arg1_datatype) &&
        (func.name == FUNC_SET ||
         func.name == FUNC_LIST_LPUSH ||
         func.name == FUNC_LIST_RPUSH))
    {
        datatype_info* di = datatype_info::lookup(func.arg1_datatype);
        return (func.arg2.empty() ||
                (func.arg2_datatype == HYPERDATATYPE_STRING &&
                 is_document_path(func.arg2))) &&
               di && di->validate(func.arg1);
    }
    // Remove a particular path (arg2)
    else if (func.arg2_datatype == HYPERDATATYPE_STRING &&
             func.name == FUNC_DOC_UNSET)
    {
        return is_document_path(func.arg2);
    }
    // Rename a particular path (arg1)->(arg2)
    else if (func.arg1_datatype == HYPERDATATYPE_STRING &&
             func.arg2_datatype == HYPERDATATYPE_STRING &&
             func.name == FUNC_DOC_RENAME)
    {
        return is_document_path(func.arg1) &&
               is_document_path(func.arg2);
    }
    // Perform a nested operation on primitives
    else if (func.arg2_datatype == HYPERDATATYPE_STRING &&
             is_document_primitive(func.arg1_datatype) &&
             func.arg1_datatype != HYPERDATATYPE_DOCUMENT)
    {
        datatype_info* di = datatype_info::lookup(func.arg1_datatype);
        return is_document_path(func.arg2) && di->check_args(func);
    }
    // No other cases
    else
    {
        return false;
    }
}

void
free_if_allocated(unsigned char** x)
{
    if (*x)
    {
        free(*x);
    }
}

bool
datatype_document :: apply(const e::slice& old_value,
                           const funcall* funcs, size_t funcs_sz,
                           e::arena* new_memory,
                           e::slice* new_value) const
{
    struct treadstone_transformer* trans = NULL;

    if (old_value.empty())
    {
        trans = treadstone_transformer_create(reinterpret_cast<const unsigned char*>("\x40\x00"), 2);
    }
    else
    {
        trans = treadstone_transformer_create(old_value.data(), old_value.size());
    }

    if (!trans)
    {
        return false;
    }

    e::guard transg = e::makeguard(treadstone_transformer_destroy, trans);

    for (size_t idx = 0; idx < funcs_sz; ++idx)
    {
        const funcall& func = funcs[idx];

        if (is_document_primitive(func.arg1_datatype) &&
            (func.name == FUNC_SET ||
             func.name == FUNC_LIST_LPUSH ||
             func.name == FUNC_LIST_RPUSH))
        {
            std::vector<char> scratch;
            e::slice v;
            coerce_primitive_to_binary(func.arg1_datatype, func.arg1, &scratch, &v);
            std::string path(func.arg2.cdata(), func.arg2.size());

            if (func.name == FUNC_SET)
            {
                if (treadstone_transformer_set_value(trans, path.c_str(), v.data(), v.size()) < 0)
                {
                    return false;
                }
            }
            else if (func.name == FUNC_LIST_LPUSH)
            {
                if (treadstone_transformer_array_prepend_value(trans, path.c_str(), v.data(), v.size()) < 0)
                {
                    return false;
                }
            }
            else if (func.name == FUNC_LIST_RPUSH)
            {
                if (treadstone_transformer_array_append_value(trans, path.c_str(), v.data(), v.size()) < 0)
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }
        else if (func.arg2_datatype == HYPERDATATYPE_STRING &&
                 func.name == FUNC_DOC_UNSET)
        {
            std::string path(func.arg2.cdata(), func.arg2.size());

            if (treadstone_transformer_unset_value(trans, path.c_str()) < 0)
            {
                return false;
            }
        }
        else if (func.arg1_datatype == HYPERDATATYPE_STRING &&
                 func.arg2_datatype == HYPERDATATYPE_STRING &&
                 func.name == FUNC_DOC_RENAME)
        {
            std::string src(func.arg2.cdata(), func.arg2.size());
            std::string dst(func.arg1.cdata(), func.arg1.size());

            unsigned char* value = NULL;
            size_t value_sz = 0;
            e::guard g = e::makeguard(free_if_allocated, &value);

            if (treadstone_transformer_extract_value(trans, src.c_str(), &value, &value_sz) < 0)
            {
                return false;
            }

            if (treadstone_transformer_unset_value(trans, src.c_str()) < 0)
            {
                return false;
            }

            if (treadstone_transformer_set_value(trans, dst.c_str(), value, value_sz) < 0)
            {
                return false;
            }
        }
        else if (func.arg2_datatype == HYPERDATATYPE_STRING &&
                 is_document_primitive(func.arg1_datatype) &&
                 func.arg1_datatype != HYPERDATATYPE_DOCUMENT)
        {
            unsigned char* value = NULL;
            size_t value_sz = 0;
            e::guard g = e::makeguard(free_if_allocated, &value);
            std::string path(func.arg2.cdata(), func.arg2.size());
            hyperdatatype type;
            std::vector<char> scratch;
            e::slice v;

            if (treadstone_transformer_extract_value(trans, path.c_str(), &value, &value_sz) < 0)
            {
                // Value doesn't exist yet
                type = func.arg1_datatype;
                v = e::slice();
            }
            else
            {
                // Extract existing value from the document
                if (!coerce_binary_to_primitive(e::slice(value, value_sz), &type, &scratch, &v))
                {
                    return false;
                }


                if (type == HYPERDATATYPE_INT64 &&
                    func.arg1_datatype == HYPERDATATYPE_FLOAT)
                {
                    int64_t x = datatype_int64::unpack(v);
                    double d = x;
                    datatype_float::pack(d, &scratch, &v);
                    type = HYPERDATATYPE_FLOAT;
                }

                // Both types are numerals (integer or float)
                const bool numeral = is_numeral(type) && is_numeral(func.arg1_datatype);

                if (func.name != FUNC_SET &&
                    func.name != FUNC_DOC_UNSET &&
                    func.name != FUNC_DOC_RENAME &&
                    !numeral)
                {
                    // More complex modifications (string append etc)
                    // only work with the same type
                    if(type != func.arg1_datatype)
                    {
                        return false;
                    }
                }
            }

            datatype_info* di = datatype_info::lookup(type);
            e::slice tmp_value;

            if (!di->check_args(func))
            {
                return false;
            }

            // Pass funcall down to underlying datatype (string, integer etc...)
            if (!di->apply(v, &func, 1, new_memory, &tmp_value))
            {
                return false;
            }

            std::vector<char> scratch_binary;
            e::slice binary;
            coerce_primitive_to_binary(type, tmp_value, &scratch_binary, &binary);

            if (treadstone_transformer_set_value(trans, path.c_str(), binary.data(), binary.size()) < 0)
            {
                return false;
            }
        }
        else
        {
            abort();
        }
    }

    unsigned char* final_doc = NULL;
    size_t final_doc_sz = 0;
    e::guard g = e::makeguard(free_if_allocated, &final_doc);

    if (treadstone_transformer_output(trans, &final_doc, &final_doc_sz) < 0)
    {
        return false;
    }

    new_memory->takeover(final_doc);
    g.dismiss();
    *new_value = e::slice(final_doc, final_doc_sz);
    return true;
}

bool
datatype_document :: client_to_server(const e::slice& client,
                                      e::arena* new_memory,
                                      e::slice* server) const
{
    unsigned char* binary;
    size_t binary_sz;
    std::string tmp(client.cdata(), client.size());

    if (treadstone_json_to_binary(tmp.c_str(), &binary, &binary_sz) < 0)
    {
        return false;
    }

    new_memory->takeover(binary);
    *server = e::slice(binary, binary_sz);
    return true;
}

bool
datatype_document :: server_to_client(const e::slice& server,
                                      e::arena* new_memory,
                                      e::slice* client) const
{
    char* json;

    if (treadstone_binary_to_json(server.data(), server.size(), &json) < 0)
    {
        return false;
    }

    new_memory->takeover(json);
    *client = e::slice(json, strlen(json) + 1);
    return true;
}

bool
datatype_document :: document() const
{
    return true;
}

bool
datatype_document :: document_check(const attribute_check& check,
                                    const e::slice& doc) const
{
    // We expected the follwing format:
    // <path>\0\n<value>

    const char* path = reinterpret_cast<const char*>(check.value.data());
    size_t path_sz = strnlen(path, check.value.size());

    if (path_sz >= check.value.size())
    {
        return false;
    }

    hyperdatatype type;
    std::vector<char> scratch;
    e::slice value;

    if (!extract_value(path, doc, &type, &scratch, &value))
    {
        return false;
    }

    if(type == HYPERDATATYPE_DOCUMENT)
    {
        // Compare two subdocuments
        e::slice chk_value = check.value;
        chk_value.advance(path_sz + 1);
        return (value == chk_value);
    }
    else
    {
        // Pass down to underlying datatype
        attribute_check new_check;
        new_check.attr      = check.attr;
        new_check.value     = check.value;
        new_check.datatype  = check.datatype;
        new_check.predicate = check.predicate;
        new_check.value.advance(path_sz + 1);
        return passes_attribute_check(type, new_check, value);
    }
}

bool
datatype_document :: extract_value(const char* path,
                                   const e::slice& data,
                                   hyperdatatype* type,
                                   std::vector<char>* scratch,
                                   e::slice* value) const
{
    struct treadstone_transformer* trans = NULL;
    trans = treadstone_transformer_create(data.data(), data.size());

    if (!trans)
    {
        return false;
    }

    e::guard transg = e::makeguard(treadstone_transformer_destroy, trans);
    unsigned char* v = NULL;
    size_t v_sz = 0;
    e::guard g = e::makeguard(free_if_allocated, &v);

    if (treadstone_transformer_extract_value(trans, path, &v, &v_sz) < 0)
    {
        return false;
    }

    return coerce_binary_to_primitive(e::slice(v, v_sz), type, scratch, value);
}

void
datatype_document :: coerce_primitive_to_binary(hyperdatatype type,
                                                const e::slice& in,
                                                std::vector<char>* scratch,
                                                e::slice* value) const
{
    assert(is_document_primitive(type));
    unsigned char* v = NULL;
    size_t v_sz = 0;
    e::guard g = e::makeguard(free_if_allocated, &v);

    if (type == HYPERDATATYPE_STRING)
    {
        int rc = treadstone_string_to_binary(in.cdata(), in.size(), &v, &v_sz);
        assert(rc == 0);
    }
    else if (type == HYPERDATATYPE_INT64)
    {
        int rc = treadstone_integer_to_binary(datatype_int64::unpack(in), &v, &v_sz);
        assert(rc == 0);
    }
    else if (type == HYPERDATATYPE_FLOAT)
    {
        int rc = treadstone_double_to_binary(datatype_float::unpack(in), &v, &v_sz);
        assert(rc == 0);
    }
    else if (type == HYPERDATATYPE_DOCUMENT)
    {
        *value = in;
        return;
    }
    else
    {
        abort();
    }

    scratch->resize(v_sz);
    memmove(&(*scratch)[0], v, v_sz);
    *value = e::slice(&(*scratch)[0], v_sz);
}

bool
datatype_document :: coerce_binary_to_primitive(const e::slice& in,
                                                hyperdatatype* type,
                                                std::vector<char>* scratch,
                                                e::slice* value) const
{
    if (treadstone_binary_validate(in.data(), in.size()) < 0)
    {
        return false;
    }

    if (treadstone_binary_is_string(in.data(), in.size()) == 0)
    {
        *type = HYPERDATATYPE_STRING;
        size_t sz = treadstone_binary_string_bytes(in.data(), in.size());
        scratch->resize(sz);
        treadstone_binary_to_string(in.data(), in.size(), &(*scratch)[0]);
        *value = e::slice(&(*scratch)[0], sz);
    }
    else if (treadstone_binary_is_integer(in.data(), in.size()) == 0)
    {
        *type = HYPERDATATYPE_INT64;
        int64_t num = treadstone_binary_to_integer(in.data(), in.size());
        datatype_int64::pack(num, scratch, value);
    }
    else if (treadstone_binary_is_double(in.data(), in.size()) == 0)
    {
        *type = HYPERDATATYPE_FLOAT;
        double num = treadstone_binary_to_double(in.data(), in.size());
        datatype_float::pack(num, scratch, value);
    }
    else
    {
        *type = HYPERDATATYPE_DOCUMENT;
        scratch->resize(in.size());
        memmove(scratch->data(), in.data(), in.size());
        *value = e::slice(&(*scratch)[0], in.size());
    }

    return true;
}
