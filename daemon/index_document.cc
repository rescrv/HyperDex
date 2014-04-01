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

// json-c
#include <json/json.h>

// e
#include <e/endian.h>
#include <e/guard.h>
#include <e/varint.h>

// HyperDex
#include "daemon/index_document.h"

using hyperdex::index_document;

index_document :: index_document()
{
}

index_document :: ~index_document() throw ()
{
}

hyperdatatype
index_document :: datatype()
{
    return HYPERDATATYPE_DOCUMENT;
}

void
index_document :: index_changes(const index* idx,
                                const region_id& ri,
                                index_encoding* key_ie,
                                const e::slice& key,
                                const e::slice* old_document,
                                const e::slice* new_document,
                                leveldb::WriteBatch* updates)
{
    type t;
    std::vector<char> scratch_value;
    e::slice value;
    std::vector<char> scratch_entry;
    leveldb::Slice entry;

    if (old_document && new_document && *old_document == *new_document)
    {
        return;
    }

    if (old_document && parse_path(idx, *old_document, &t, &scratch_value, &value))
    {
        index_entry(ri, idx->id, t, key_ie, key, value, &scratch_entry, &entry);
        updates->Delete(entry);
    }

    if (new_document && parse_path(idx, *new_document, &t, &scratch_value, &value))
    {
        index_entry(ri, idx->id, t, key_ie, key, value, &scratch_entry, &entry);
        updates->Put(entry, leveldb::Slice());
    }
}

bool
index_document :: parse_path(const index* idx,
                             const e::slice& document,
                             type* t,
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
    const char* data = reinterpret_cast<const char*>(document.data());
    json_object* obj = json_tokener_parse_ex(tok, data, document.size());

    if (!obj)
    {
        return false;
    }

    e::guard gobj = e::makeguard(json_object_put, obj);
    gobj.use_variable();

    if (json_tokener_get_error(tok) != json_tokener_success ||
        tok->char_offset != (ssize_t)document.size())
    {
        return false;
    }

    const char* path = reinterpret_cast<const char*>(idx->extra.data());
    const char* const end = path + idx->extra.size();
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
        double d = json_object_get_double(parent);

        if (scratch->size() < sizeof(double))
        {
            scratch->resize(sizeof(double));
        }

        e::packdoublele(d, &scratch->front());
        *t = NUMBER;
        *value = e::slice(&scratch->front(), sizeof(double));
        return true;
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
        *t = STRING;
        *value = e::slice(&scratch->front(), str_sz);
        return true;
    }
    else
    {
        return false;
    }
}

void
index_document :: index_entry(const region_id& ri,
                              const index_id& ii,
                              type t,
                              index_encoding* key_ie,
                              const e::slice& key,
                              const e::slice& value,
                              std::vector<char>* scratch,
                              leveldb::Slice* slice)
{
    index_encoding* val_ie = t == STRING ? index_encoding::lookup(HYPERDATATYPE_STRING)
                                         : index_encoding::lookup(HYPERDATATYPE_FLOAT);
    size_t key_sz = key_ie->encoded_size(key);
    size_t val_sz = val_ie->encoded_size(value);
    bool variable = !key_ie->encoding_fixed() && !val_ie->encoding_fixed();
    size_t sz = sizeof(uint8_t)
              + e::varint_length(ri.get())
              + e::varint_length(ii.get())
              + sizeof(uint8_t)
              + val_sz
              + key_sz
              + (variable ? sizeof(uint32_t) : 0);

    if (scratch->size() < sz)
    {
        scratch->resize(sz);
    }

    uint8_t t8 = t == STRING ? 's' : 'i';
    char* ptr = &scratch->front();
    ptr = e::pack8be('i', ptr);
    ptr = e::packvarint64(ri.get(), ptr);
    ptr = e::packvarint64(ii.get(), ptr);
    ptr = e::pack8be(t8, ptr);
    ptr = val_ie->encode(value, ptr);
    ptr = key_ie->encode(key, ptr);

    if (variable)
    {
        ptr = e::pack32be(key_sz, ptr);
    }

    assert(ptr == &scratch->front() + sz);
    *slice = leveldb::Slice(&scratch->front(), sz);
}
