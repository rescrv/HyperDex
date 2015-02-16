// Copyright (c) 2013, Cornell University
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
#include <e/endian.h>
#include <e/varint.h>

// HyperDex
#include "daemon/datalayer_encodings.h"
#include "daemon/datalayer_iterator.h"
#include "daemon/index_primitive.h"

using hyperdex::datalayer;
using hyperdex::index_primitive;

inline leveldb::Slice e2level(const e::slice& s) { return leveldb::Slice(reinterpret_cast<const char*>(s.data()), s.size()); }
inline e::slice level2e(const leveldb::Slice& s) { return e::slice(s.data(), s.size()); }

index_primitive :: index_primitive(const index_encoding* ie)
    : m_ie(ie)
{
}

index_primitive :: ~index_primitive() throw ()
{
}

void
index_primitive :: index_changes(const index* idx,
                                 const region_id& ri,
                                 const index_encoding* key_ie,
                                 const e::slice& key,
                                 const e::slice* old_value,
                                 const e::slice* new_value,
                                 leveldb::WriteBatch* updates) const
{
    std::vector<char> scratch;
    e::slice slice;

    if (old_value && new_value && *old_value == *new_value)
    {
        return;
    }

    if (old_value)
    {
        index_entry(ri, idx->id, key_ie, key, *old_value, &scratch, &slice);
        updates->Delete(e2level(slice));
    }

    if (new_value)
    {
        index_entry(ri, idx->id, key_ie, key, *new_value, &scratch, &slice);
        updates->Put(e2level(slice), leveldb::Slice());
    }
}

datalayer::index_iterator*
index_primitive :: iterator_for_keys(leveldb_snapshot_ptr snap,
                                     const region_id& ri) const
{
    range scan;
    scan.attr = 0;
    scan.type = this->datatype();
    scan.has_start = false;
    scan.has_end = false;
    scan.invalid = false;
    const index_encoding* ie = index_encoding::lookup(scan.type);
    return iterator_key(snap, ri, scan, ie);
}

datalayer::index_iterator*
index_primitive :: iterator_from_range(leveldb_snapshot_ptr snap,
                                       const region_id& ri,
                                       const index_id& ii,
                                       const range& r,
                                       const index_encoding* key_ie) const
{
    if (r.invalid)
    {
        return NULL;
    }

    if (r.attr != 0)
    {
        return iterator_attr(snap, ri, ii, r, key_ie);
    }
    else
    {
        return iterator_key(snap, ri, r, key_ie);
    }
}

datalayer::index_iterator*
index_primitive :: iterator_key(leveldb_snapshot_ptr snap,
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

    return new datalayer::range_index_iterator(snap, range_prefix_sz,
                                               start, limit,
                                               r.has_start, r.has_end,
                                               NULL, key_ie);
}

datalayer::index_iterator*
index_primitive :: iterator_attr(leveldb_snapshot_ptr snap,
                                 const region_id& ri,
                                 const index_id& ii,
                                 const range& r,
                                 const index_encoding* key_ie) const
{
    std::vector<char> scratch_start;
    std::vector<char> scratch_limit;
    e::slice start;
    e::slice limit;

    size_t range_prefix_sz = index_entry_prefix_size(ri, ii);

    if (r.has_start)
    {
        index_entry(ri, ii, r.start, &scratch_start, &start);
    }
    else
    {
        index_entry(ri, ii, &scratch_start, &start);
    }

    if (r.has_end)
    {
        index_entry(ri, ii, r.end, &scratch_limit, &limit);
    }
    else
    {
        index_entry(ri, ii, &scratch_limit, &limit);
    }

    return new datalayer::range_index_iterator(snap, range_prefix_sz,
                                               start, limit,
                                               r.has_start, r.has_end,
                                               m_ie, key_ie);
}

size_t
index_primitive :: index_entry_prefix_size(const region_id& ri, const index_id& ii) const
{
    return sizeof(uint8_t)
         + e::varint_length(ri.get())
         + e::varint_length(ii.get());
}

void
index_primitive :: index_entry(const region_id& ri,
                               const index_id& ii,
                               std::vector<char>* scratch,
                               e::slice* slice) const
{
    size_t sz = sizeof(uint8_t)
              + e::varint_length(ri.get())
              + e::varint_length(ii.get());

    if (scratch->size() < sz)
    {
        scratch->resize(sz);
    }

    char* ptr = &scratch->front();
    ptr = e::pack8be('i', ptr);
    ptr = e::packvarint64(ri.get(), ptr);
    ptr = e::packvarint64(ii.get(), ptr);
    assert(ptr == &scratch->front() + sz);
    *slice = e::slice(&scratch->front(), sz);
}

void
index_primitive :: index_entry(const region_id& ri,
                               const index_id& ii,
                               const e::slice& value,
                               std::vector<char>* scratch,
                               e::slice* slice) const
{
    size_t val_sz = m_ie->encoded_size(value);
    size_t sz = sizeof(uint8_t)
              + e::varint_length(ri.get())
              + e::varint_length(ii.get())
              + val_sz;

    if (scratch->size() < sz)
    {
        scratch->resize(sz);
    }

    char* ptr = &scratch->front();
    ptr = e::pack8be('i', ptr);
    ptr = e::packvarint64(ri.get(), ptr);
    ptr = e::packvarint64(ii.get(), ptr);
    ptr = m_ie->encode(value, ptr);
    assert(ptr == &scratch->front() + sz);
    *slice = e::slice(&scratch->front(), sz);
}

void
index_primitive :: index_entry(const region_id& ri,
                               const index_id& ii,
                               const e::slice& internal_key,
                               const e::slice& value,
                               std::vector<char>* scratch,
                               e::slice* slice) const
{
    size_t key_sz = internal_key.size();
    size_t val_sz = m_ie->encoded_size(value);
    size_t sz = sizeof(uint8_t)
              + e::varint_length(ri.get())
              + e::varint_length(ii.get())
              + val_sz
              + key_sz;

    if (scratch->size() < sz)
    {
        scratch->resize(sz);
    }

    char* ptr = &scratch->front();
    ptr = e::pack8be('i', ptr);
    ptr = e::packvarint64(ri.get(), ptr);
    ptr = e::packvarint64(ii.get(), ptr);
    ptr = m_ie->encode(value, ptr);
    memmove(ptr, internal_key.data(), key_sz);
    ptr += key_sz;
    assert(ptr == &scratch->front() + sz);
    *slice = e::slice(&scratch->front(), sz);
}

void
index_primitive :: index_entry(const region_id& ri,
                               const index_id& ii,
                               const index_encoding* key_ie,
                               const e::slice& key,
                               const e::slice& value,
                               std::vector<char>* scratch,
                               e::slice* slice) const
{
    size_t key_sz = key_ie->encoded_size(key);
    size_t val_sz = m_ie->encoded_size(value);
    bool variable = !key_ie->encoding_fixed() && !m_ie->encoding_fixed();
    size_t sz = sizeof(uint8_t)
              + e::varint_length(ri.get())
              + e::varint_length(ii.get())
              + val_sz
              + key_sz
              + (variable ? sizeof(uint32_t) : 0);

    if (scratch->size() < sz)
    {
        scratch->resize(sz);
    }

    char* ptr = &scratch->front();
    ptr = e::pack8be('i', ptr);
    ptr = e::packvarint64(ri.get(), ptr);
    ptr = e::packvarint64(ii.get(), ptr);
    ptr = m_ie->encode(value, ptr);
    ptr = key_ie->encode(key, ptr);

    if (variable)
    {
        ptr = e::pack32be(key_sz, ptr);
    }

    assert(ptr == &scratch->front() + sz);
    *slice = e::slice(&scratch->front(), sz);
}
