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
#include "daemon/datalayer_iterator.h"
#include "daemon/index_document.h"

using hyperdex::index_document;

index_document :: index_document()
    : m_di()
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
    type_t t;
    std::vector<char> scratch_value;
    e::slice value;
    std::vector<char> scratch_entry;
    e::slice entry;

    if (old_document && new_document && *old_document == *new_document)
    {
        return;
    }

    if (old_document && parse_path(idx, *old_document, &t, &scratch_value, &value))
    {
        index_entry(ri, idx->id, t, key_ie, key, value, &scratch_entry, &entry);
        updates->Delete(leveldb::Slice(reinterpret_cast<const char*>(entry.data()), entry.size()));
    }

    if (new_document && parse_path(idx, *new_document, &t, &scratch_value, &value))
    {
        index_entry(ri, idx->id, t, key_ie, key, value, &scratch_entry, &entry);
        updates->Put(leveldb::Slice(reinterpret_cast<const char*>(entry.data()), entry.size()), leveldb::Slice());
    }
}

hyperdex::datalayer::index_iterator*
index_document :: iterator_from_check(leveldb_snapshot_ptr snap,
                                      const region_id& ri,
                                      const index_id& ii,
                                      const attribute_check& check,
                                      index_encoding* key_ie)
{
    const char* path = reinterpret_cast<const char*>(check.value.data());
    size_t path_sz = strnlen(path, check.value.size());

    if (path_sz >= check.value.size())
    {
        return NULL;
    }

    if (check.datatype != HYPERDATATYPE_STRING &&
        check.datatype != HYPERDATATYPE_INT64 &&
        check.datatype != HYPERDATATYPE_FLOAT)
    {
        return NULL;
    }

    char scratch_v[sizeof(int64_t) + sizeof(double)];
    e::slice value(path + path_sz + 1, check.value.size() - path_sz - 1);

    if (check.datatype == HYPERDATATYPE_INT64)
    {
        memset(scratch_v, 0, sizeof(int64_t));
        memmove(scratch_v, value.data(), std::min(value.size(), sizeof(int64_t)));
        int64_t val_i;
        e::unpack64le(scratch_v, &val_i);
        double val_d = val_i;
        e::pack64le(val_d, scratch_v);
        value = e::slice(scratch_v, sizeof(double));
    }

    size_t range_prefix_sz = index_entry_prefix_size(ri, ii);
    std::vector<char> scratch_a;
    std::vector<char> scratch_b;
    e::slice a;
    e::slice b;
    e::slice start;
    e::slice limit;
    bool has_start;
    bool has_limit;
    type_t t = check.datatype == HYPERDATATYPE_STRING ? STRING : NUMBER;

    index_entry(ri, ii, t, value, &scratch_a, &a);
    index_entry(ri, ii, t, &scratch_b, &b);

    switch (check.predicate)
    {
        case HYPERPREDICATE_EQUALS:
            start = a;
            limit = a;
            break;
        case HYPERPREDICATE_LESS_THAN:
        case HYPERPREDICATE_LESS_EQUAL:
            start = b;
            limit = a;
            break;
        case HYPERPREDICATE_GREATER_EQUAL:
        case HYPERPREDICATE_GREATER_THAN:
            start = a;
            limit = b;
            break;
        case HYPERPREDICATE_FAIL:
        case HYPERPREDICATE_CONTAINS_LESS_THAN:
        case HYPERPREDICATE_REGEX:
        case HYPERPREDICATE_LENGTH_EQUALS:
        case HYPERPREDICATE_LENGTH_LESS_EQUAL:
        case HYPERPREDICATE_LENGTH_GREATER_EQUAL:
        case HYPERPREDICATE_CONTAINS:
        default:
            return NULL;
    }

    index_encoding* ie;

    switch (t)
    {
        case STRING:
            ie = index_encoding::lookup(HYPERDATATYPE_STRING);
            break;
        case NUMBER:
            ie = index_encoding::lookup(HYPERDATATYPE_FLOAT);
            break;
        default:
            abort();
    }

    has_start = a.data() == start.data();
    has_limit = a.data() == limit.data();
    return new datalayer::range_index_iterator(snap, range_prefix_sz,
                                               start, limit,
                                               has_start, has_limit,
                                               ie, key_ie);
}

bool
index_document :: parse_path(const index* idx,
                             const e::slice& document,
                             type_t* t,
                             std::vector<char>* scratch,
                             e::slice* value)
{
    const char* path = reinterpret_cast<const char*>(idx->extra.data());
    const char* const end = path + idx->extra.size();
    hyperdatatype type;

    if (m_di.parse_path(path, end, document, HYPERDATATYPE_GENERIC, &type, scratch, value))
    {
        if (type == HYPERDATATYPE_STRING)
        {
            *t = STRING;
            return true;
        }
        else if (type == HYPERDATATYPE_INT64 || type == HYPERDATATYPE_FLOAT)
        {
            *t = NUMBER;
            return true;
        }
    }

    return false;
}

size_t
index_document :: index_entry_prefix_size(const region_id& ri, const index_id& ii)
{
    return sizeof(uint8_t)
         + e::varint_length(ri.get())
         + e::varint_length(ii.get())
         + sizeof(uint8_t);
}

void
index_document :: index_entry(const region_id& ri,
                              const index_id& ii,
                              type_t t,
                              std::vector<char>* scratch,
                              e::slice* slice)
{
    size_t sz = sizeof(uint8_t)
              + e::varint_length(ri.get())
              + e::varint_length(ii.get())
              + sizeof(uint8_t);

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
    assert(ptr == &scratch->front() + sz);
    *slice = e::slice(&scratch->front(), sz);
}

void
index_document :: index_entry(const region_id& ri,
                              const index_id& ii,
                              type_t t,
                              const e::slice& value,
                              std::vector<char>* scratch,
                              e::slice* slice)
{
    index_encoding* val_ie = t == STRING ? index_encoding::lookup(HYPERDATATYPE_STRING)
                                         : index_encoding::lookup(HYPERDATATYPE_FLOAT);
    size_t val_sz = val_ie->encoded_size(value);
    size_t sz = sizeof(uint8_t)
              + e::varint_length(ri.get())
              + e::varint_length(ii.get())
              + sizeof(uint8_t)
              + val_sz;

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
    assert(ptr == &scratch->front() + sz);
    *slice = e::slice(&scratch->front(), sz);
}

void
index_document :: index_entry(const region_id& ri,
                              const index_id& ii,
                              type_t t,
                              index_encoding* key_ie,
                              const e::slice& key,
                              const e::slice& value,
                              std::vector<char>* scratch,
                              e::slice* slice)
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
    *slice = e::slice(&scratch->front(), sz);
}
