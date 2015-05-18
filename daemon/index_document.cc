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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// e
#include <e/endian.h>
#include <e/guard.h>
#include <e/varint.h>

// HyperDex
#include "daemon/datalayer_iterator.h"
#include "daemon/datalayer_encodings.h"
#include "daemon/index_document.h"

using hyperdex::index_document;
using hyperdex::index_encoding_document;

inline leveldb::Slice e2level(const e::slice& s) { return leveldb::Slice(reinterpret_cast<const char*>(s.data()), s.size()); }
inline e::slice level2e(const leveldb::Slice& s) { return e::slice(s.data(), s.size()); }

index_document :: index_document()
    : m_di()
{
}

index_document :: ~index_document() throw ()
{
}

hyperdatatype
index_document :: datatype() const
{
    return HYPERDATATYPE_DOCUMENT;
}

void
index_document :: index_changes(const index* idx,
                                const region_id& ri,
                                const index_encoding* key_ie,
                                const e::slice& key,
                                const e::slice* old_document,
                                const e::slice* new_document,
                                leveldb::WriteBatch* updates) const
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
                                      const index_encoding* key_ie) const
{
    const char* path = reinterpret_cast<const char*>(check.value.data());
    size_t path_sz = strnlen(path, check.value.size());

    if (path_sz >= check.value.size())
    {
        return NULL;
    }

    if (check.datatype != HYPERDATATYPE_STRING &&
        check.datatype != HYPERDATATYPE_INT64 &&
        check.datatype != HYPERDATATYPE_FLOAT &&
        check.datatype != HYPERDATATYPE_DOCUMENT)
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

    type_t t;

    if(check.datatype == HYPERDATATYPE_STRING)
    {
        t = STRING;
    }
    else if(check.datatype == HYPERDATATYPE_DOCUMENT)
    {
        if(check.predicate != HYPERPREDICATE_EQUALS)
        {
            return NULL;
        }

        t = DOCUMENT;
    }
    else
    {
        t = NUMBER;
    }

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

    const index_encoding* ie = lookup_encoding(t);

    has_start = a.data() == start.data();
    has_limit = a.data() == limit.data();
    return new datalayer::range_index_iterator(snap, range_prefix_sz,
                                               start, limit,
                                               has_start, has_limit,
                                               ie, key_ie);
}

const hyperdex::index_encoding*
index_document :: lookup_encoding(type_t t) const
{
    switch (t)
    {
        case STRING:
            return index_encoding::lookup(HYPERDATATYPE_STRING);
        case NUMBER:
            return index_encoding::lookup(HYPERDATATYPE_FLOAT);
        case DOCUMENT:
            return index_encoding::lookup(HYPERDATATYPE_DOCUMENT);
        default:
            abort();
    }

    return NULL;
}

bool
index_document :: parse_path(const index* idx,
                             const e::slice& document,
                             type_t* t,
                             std::vector<char>* scratch,
                             e::slice* value) const
{
    hyperdatatype type;

    if (m_di.extract_value(idx->extra.cdata(), document, &type, scratch, value))
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
        else if (type == HYPERDATATYPE_DOCUMENT)
        {
            *t = DOCUMENT;
            return true;
        }
    }

    return false;
}

size_t
index_document :: index_entry_prefix_size(const region_id& ri, const index_id& ii) const
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
                              e::slice* slice) const
{
    size_t sz = sizeof(uint8_t)
              + e::varint_length(ri.get())
              + e::varint_length(ii.get())
              + sizeof(uint8_t);

    if (scratch->size() < sz)
    {
        scratch->resize(sz);
    }

    uint8_t t8;
    if(t == STRING)
    {
        t8 = 's';
    }
    else if(t == NUMBER)
    {
        t8 = 'i';
    }
    else
    {
        t8 = 'd';
    }

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
                              e::slice* slice) const
{
    const index_encoding* val_ie = lookup_encoding(t);

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

    uint8_t t8;
    if(t == STRING)
    {
        t8 = 's';
    }
    else if(t == NUMBER)
    {
        t8 = 'i';
    }
    else
    {
        t8 = 'd';
    }

    char* ptr = &scratch->front();
    ptr = e::pack8be('i', ptr);
    ptr = e::packvarint64(ri.get(), ptr);
    ptr = e::packvarint64(ii.get(), ptr);
    ptr = e::pack8be(t8, ptr);
    ptr = val_ie->encode(value, ptr);
    assert(ptr == &scratch->front() + sz);
    *slice = e::slice(&scratch->front(), sz);
}

hyperdex::datalayer::index_iterator*
index_document :: iterator_for_keys(leveldb_snapshot_ptr snap,
                                     const region_id& ri) const
{
    range scan;
    scan.attr = 0;
    scan.type = HYPERDATATYPE_DOCUMENT;
    scan.has_start = false;
    scan.has_end = false;
    scan.invalid = false;
    const index_encoding* ie = index_encoding::lookup(scan.type);
    return iterator_key(snap, ri, scan, ie);
}

hyperdex::datalayer::index_iterator*
index_document :: iterator_key(leveldb_snapshot_ptr snap,
                                const region_id& ri,
                                const range& r,
                                const index_encoding* key_ie) const
{
    std::vector<char> scratch_start;
    std::vector<char> scratch_limit;
    e::slice start;
    e::slice limit;

    size_t range_prefix_sz = object_prefix_sz(ri);

    if (r.has_start)
    {
        leveldb::Slice _start;
        encode_key(ri, r.type, r.start, &scratch_start, &_start);
        start = level2e(_start);
    }
    else
    {
        leveldb::Slice _start;
        encode_object_region(ri, &scratch_start, &_start);
        start = level2e(_start);
    }

    if (r.has_end)
    {
        leveldb::Slice _limit;
        encode_key(ri, r.type, r.end, &scratch_limit, &_limit);
        limit = level2e(_limit);
    }
    else
    {
        leveldb::Slice _limit;
        encode_object_region(ri, &scratch_limit, &_limit);
        limit = level2e(_limit);
    }

    return new hyperdex::datalayer::range_index_iterator(snap, range_prefix_sz,
                                               start, limit,
                                               r.has_start, r.has_end,
                                               NULL, key_ie);
}

void
index_document :: index_entry(const region_id& ri,
                              const index_id& ii,
                              type_t t,
                              const index_encoding* key_ie,
                              const e::slice& key,
                              const e::slice& value,
                              std::vector<char>* scratch,
                              e::slice* slice) const
{
    const index_encoding* val_ie = lookup_encoding(t);

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

    uint8_t t8;
    if(t == STRING)
    {
        t8 = 's';
    }
    else if(t == NUMBER)
    {
        t8 = 'i';
    }
    else
    {
        t8 = 'd';
    }

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

index_encoding_document :: index_encoding_document()
{
}

index_encoding_document :: ~index_encoding_document() throw ()
{
}

bool
index_encoding_document :: encoding_fixed() const
{
    return false;
}

size_t
index_encoding_document :: encoded_size(const e::slice& decoded) const
{
    return decoded.size();
}

char*
index_encoding_document :: encode(const e::slice& decoded, char* encoded) const
{
    memmove(encoded, decoded.data(), decoded.size());
    return encoded + decoded.size();
}

size_t
index_encoding_document :: decoded_size(const e::slice& encoded) const
{
    return encoded.size();
}

char*
index_encoding_document :: decode(const e::slice& encoded, char* decoded) const
{
    memmove(decoded, encoded.data(), encoded.size());
    return decoded + encoded.size();
}
