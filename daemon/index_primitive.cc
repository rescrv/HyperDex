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

// HyperDex
#include "daemon/datalayer_encodings.h"
#include "daemon/datalayer_iterator.h"
#include "daemon/index_primitive.h"

using hyperdex::datalayer;
using hyperdex::index_primitive;

index_primitive :: index_primitive()
{
}

index_primitive :: ~index_primitive() throw ()
{
}

void
index_primitive :: index_changes(const region_id& ri,
                                 uint16_t attr,
                                 index_info* key_ii,
                                 const e::slice& key,
                                 const e::slice* old_value,
                                 const e::slice* new_value,
                                 leveldb::WriteBatch* updates)
{
    std::vector<char> scratch;
    leveldb::Slice slice;

    if (old_value && new_value && *old_value == *new_value)
    {
        return;
    }

    if (old_value)
    {
        index_entry(ri, attr, key_ii, key, *old_value, &scratch, &slice);
        updates->Delete(slice);
    }

    if (new_value)
    {
        index_entry(ri, attr, key_ii, key, *new_value, &scratch, &slice);
        updates->Put(slice, leveldb::Slice());
    }
}

void
index_primitive :: index_entry(const region_id& ri,
                               uint16_t attr,
                               std::vector<char>* scratch,
                               leveldb::Slice* slice)
{
    size_t sz = sizeof(uint8_t)
              + sizeof(uint64_t)
              + sizeof(uint16_t);

    if (scratch->size() < sz)
    {
        scratch->resize(sz);
    }

    char* ptr = &scratch->front();
    ptr = e::pack8be('i', ptr);
    ptr = e::pack64be(ri.get(), ptr);
    ptr = e::pack16be(attr, ptr);
    assert(ptr == &scratch->front() + sz);
    *slice = leveldb::Slice(&scratch->front(), sz);
}

void
index_primitive :: index_entry(const region_id& ri,
                               uint16_t attr,
                               const e::slice& value,
                               std::vector<char>* scratch,
                               leveldb::Slice* slice)
{
    size_t val_sz = this->encoded_size(value);
    size_t sz = sizeof(uint8_t)
              + sizeof(uint64_t)
              + sizeof(uint16_t)
              + val_sz;

    if (scratch->size() < sz)
    {
        scratch->resize(sz);
    }

    char* ptr = &scratch->front();
    ptr = e::pack8be('i', ptr);
    ptr = e::pack64be(ri.get(), ptr);
    ptr = e::pack16be(attr, ptr);
    ptr = this->encode(value, ptr);
    assert(ptr == &scratch->front() + sz);
    *slice = leveldb::Slice(&scratch->front(), sz);
}

void
index_primitive :: index_entry(const region_id& ri,
                               uint16_t attr,
                               index_info* key_ii,
                               const e::slice& key,
                               const e::slice& value,
                               std::vector<char>* scratch,
                               leveldb::Slice* slice)
{
    size_t key_sz = key_ii->encoded_size(key);
    size_t val_sz = this->encoded_size(value);
    bool variable = !key_ii->encoding_fixed() && !this->encoding_fixed();
    size_t sz = sizeof(uint8_t)
              + sizeof(uint64_t)
              + sizeof(uint16_t)
              + val_sz
              + key_sz
              + (variable ? sizeof(uint32_t) : 0);

    if (scratch->size() < sz)
    {
        scratch->resize(sz);
    }

    char* ptr = &scratch->front();
    ptr = e::pack8be('i', ptr);
    ptr = e::pack64be(ri.get(), ptr);
    ptr = e::pack16be(attr, ptr);
    ptr = this->encode(value, ptr);
    ptr = key_ii->encode(key, ptr);

    if (variable)
    {
        ptr = e::pack32be(key_sz, ptr);
    }

    assert(ptr == &scratch->front() + sz);
    *slice = leveldb::Slice(&scratch->front(), sz);
}

namespace
{

using hyperdex::index_info;
using hyperdex::leveldb_iterator_ptr;
using hyperdex::leveldb_snapshot_ptr;
using hyperdex::range;
using hyperdex::region_id;

e::slice
level2e(const leveldb::Slice& s)
{
    return e::slice(s.data(), s.size());
}

bool
decode_entry(const leveldb::Slice& in,
             index_info* val_ii,
             index_info* key_ii,
             region_id* ri,
             uint16_t* attr,
             e::slice* val,
             e::slice* key)
{
    const size_t prefix_sz = sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint16_t);

    if (in.size() < prefix_sz)
    {
        return false;
    }

    const char* ptr = in.data();
    uint8_t type;
    uint64_t region;
    ptr = e::unpack8be(ptr, &type);
    ptr = e::unpack64be(ptr, &region);
    ptr = e::unpack16be(ptr, attr);

    if (type != 'i')
    {
        return false;
    }

    *ri = region_id(region);
    size_t rem = in.size() - prefix_sz;

    if (val_ii->encoding_fixed())
    {
        size_t sz = val_ii->encoded_size(e::slice());

        if (sz > rem)
        {
            return false;
        }

        *val = e::slice(in.data() + prefix_sz, sz);
        *key = e::slice(in.data() + prefix_sz + val->size(), rem - sz);
    }
    else if (key_ii->encoding_fixed())
    {
        size_t sz = key_ii->encoded_size(e::slice());

        if (sz > rem)
        {
            return false;
        }

        *val = e::slice(in.data() + prefix_sz, rem - sz);
        *key = e::slice(in.data() + prefix_sz + val->size(), sz);
    }
    else
    {
        if (rem < sizeof(uint32_t))
        {
            return false;
        }

        uint32_t key_sz;
        e::unpack32be(in.data() + in.size() - sizeof(uint32_t), &key_sz);

        if (key_sz + sizeof(uint32_t) > rem)
        {
            return false;
        }

        *val = e::slice(in.data() + prefix_sz, rem - sizeof(uint32_t) - key_sz);
        *key = e::slice(in.data() + prefix_sz + val->size(), key_sz);
    }

    return true;
}

class range_iterator : public datalayer::index_iterator
{
    public:
        range_iterator(leveldb_snapshot_ptr snap,
                       const region_id& ri, 
                       const range& r,
                       index_primitive* val_ii,
                       index_info* key_ii);
        virtual ~range_iterator() throw ();

    public:
        virtual bool valid();
        virtual void next();
        virtual uint64_t cost(leveldb::DB*);
        virtual e::slice key();
        virtual std::ostream& describe(std::ostream&) const;
        virtual e::slice internal_key();
        virtual bool sorted();
        virtual void seek(const e::slice& internal_key);

    private:
        range_iterator(const range_iterator&);
        range_iterator& operator = (const range_iterator&);

    private:
        leveldb_iterator_ptr m_iter;
        region_id m_ri;
        range m_range;
        index_primitive* m_val_ii;
        index_info* m_key_ii;
        std::vector<char> m_scratch;
        bool m_invalid;
};

range_iterator :: range_iterator(leveldb_snapshot_ptr s,
                                 const region_id& ri, 
                                 const range& r,
                                 index_primitive* val_ii,
                                 index_info* key_ii)
    : index_iterator(s)
    , m_iter()
    , m_ri(ri)
    , m_range(r)
    , m_val_ii(val_ii)
    , m_key_ii(key_ii)
    , m_scratch()
    , m_invalid(false)
{
    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    opts.snapshot = s.get();
    m_iter.reset(s, s.db()->NewIterator(opts));

    leveldb::Slice slice;

    if (m_range.has_start)
    {
        m_val_ii->index_entry(m_ri, m_range.attr, m_range.start, &m_scratch, &slice);
    }
    else
    {
        m_val_ii->index_entry(m_ri, m_range.attr, &m_scratch, &slice);
    }

    m_iter->Seek(slice);
}

range_iterator :: ~range_iterator() throw ()
{
}

bool
range_iterator :: valid()
{
    while (!m_invalid && m_iter->Valid())
    {
        leveldb::Slice _k = m_iter->key();
        region_id ri;
        uint16_t attr;
        e::slice v;
        e::slice k;

        if (!decode_entry(_k, m_val_ii, m_key_ii, &ri, &attr, &v, &k))
        {
            m_invalid = true;
            return false;
        }

        if (m_ri < ri || m_range.attr < attr)
        {
            m_invalid = true;
            return false;
        }

        if (!m_range.has_end)
        {
            return true;
        }

        size_t sz = std::min(m_range.end.size(), v.size());
        int cmp = memcmp(m_range.end.data(), v.data(), sz);

        if (cmp > 0)
        {
            m_invalid = true;
            return false;
        }

        // if v > end
        if (cmp == 0 && m_range.end.size() < v.size())
        {
            m_iter->Next();
            continue;
        }

        return true;
    }

    return false;
}

void
range_iterator :: next()
{
    m_iter->Next();
}

uint64_t
range_iterator :: cost(leveldb::DB* db)
{
    assert(this->sorted());
    leveldb::Slice upper;
    m_val_ii->index_entry(m_ri, m_range.attr, m_range.end, &m_scratch, &upper);
    hyperdex::encode_bump(&m_scratch.front(), &m_scratch.front() + m_scratch.size());
    // create the range
    leveldb::Range r;
    r.start = m_iter->key();
    r.limit = upper;
    // ask leveldb for the size of the range
    uint64_t ret;
    db->GetApproximateSizes(&r, 1, &ret);
    return ret;
}

e::slice
range_iterator :: key()
{
    e::slice ik = this->internal_key();
    size_t decoded_sz = m_key_ii->decoded_size(ik);

    if (m_scratch.size() < decoded_sz)
    {
        m_scratch.resize(decoded_sz);
    }

    m_key_ii->decode(ik, &m_scratch.front());
    return e::slice(&m_scratch.front(), decoded_sz);
}

std::ostream&
range_iterator :: describe(std::ostream& out) const
{
    return out << "primitive range_iterator()";
}

e::slice
range_iterator :: internal_key()
{
    leveldb::Slice _k = m_iter->key();
    region_id ri;
    uint16_t attr;
    e::slice v;
    e::slice k;
    decode_entry(_k, m_val_ii, m_key_ii, &ri, &attr, &v, &k);
    return k;
}

bool
range_iterator :: sorted()
{
    return m_range.has_start && m_range.has_end && m_range.start == m_range.end;
}

void
range_iterator :: seek(const e::slice& ik)
{
    assert(sorted());
    leveldb::Slice slice;
    m_val_ii->index_entry(m_ri, m_range.attr, m_key_ii, m_range.start, ik, &m_scratch, &slice);
    m_iter->Seek(slice);
}

class key_iterator : public datalayer::index_iterator
{
    public:
        key_iterator(leveldb_snapshot_ptr snap,
                     const region_id& ri, 
                     const range& r,
                     index_info* key_ii);
        virtual ~key_iterator() throw ();

    public:
        virtual bool valid();
        virtual void next();
        virtual uint64_t cost(leveldb::DB*);
        virtual e::slice key();
        virtual std::ostream& describe(std::ostream&) const;
        virtual e::slice internal_key();
        virtual bool sorted();
        virtual void seek(const e::slice& internal_key);

    private:
        key_iterator(const key_iterator&);
        key_iterator& operator = (const key_iterator&);

    private:
        leveldb_iterator_ptr m_iter;
        region_id m_ri;
        range m_range;
        index_info* m_key_ii;
        std::vector<char> m_scratch;
        bool m_invalid;
};

key_iterator :: key_iterator(leveldb_snapshot_ptr s,
                             const region_id& ri, 
                             const range& r,
                             index_info* key_ii)
    : index_iterator(s)
    , m_iter()
    , m_ri(ri)
    , m_range(r)
    , m_key_ii(key_ii)
    , m_scratch()
    , m_invalid(false)
{
    assert(m_range.attr == 0);
    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    opts.snapshot = s.get();
    m_iter.reset(s, s.db()->NewIterator(opts));

    leveldb::Slice slice;

    if (m_range.has_start)
    {
        encode_key(m_ri, m_range.type, m_range.start, &m_scratch, &slice);
    }
    else
    {
        encode_object_region(m_ri, &m_scratch, &slice);
    }

    m_iter->Seek(slice);
}

key_iterator :: ~key_iterator() throw ()
{
}

bool
key_iterator :: valid()
{
    while (!m_invalid && m_iter->Valid())
    {
        leveldb::Slice _k = m_iter->key();
        region_id ri;
        e::slice k;

        if (!decode_key(_k, &ri, &k) ||
            m_ri < ri)
        {
            m_invalid = true;
            return false;
        }

        if (!m_range.has_end)
        {
            return true;
        }

        size_t sz = std::min(m_range.end.size(), k.size());
        int cmp = memcmp(m_range.end.data(), k.data(), sz);

        if (cmp > 0)
        {
            m_invalid = true;
            return false;
        }

        // if k > end
        if (cmp == 0 && m_range.end.size() < k.size())
        {
            m_iter->Next();
            continue;
        }

        return true;
    }

    return false;
}

void
key_iterator :: next()
{
    m_iter->Next();
}

uint64_t
key_iterator :: cost(leveldb::DB* db)
{
    assert(this->sorted());
    leveldb::Slice upper;
    encode_key(m_ri, m_range.type, m_range.end, &m_scratch, &upper);
    hyperdex::encode_bump(&m_scratch.front(), &m_scratch.front() + m_scratch.size());
    // create the range
    leveldb::Range r;
    r.start = m_iter->key();
    r.limit = upper;
    // ask leveldb for the size of the range
    uint64_t ret;
    db->GetApproximateSizes(&r, 1, &ret);
    return ret;
}

e::slice
key_iterator :: key()
{
    e::slice ik = this->internal_key();
    size_t decoded_sz = m_key_ii->decoded_size(ik);

    if (m_scratch.size() < decoded_sz)
    {
        m_scratch.resize(decoded_sz);
    }

    m_key_ii->decode(ik, &m_scratch.front());
    return e::slice(&m_scratch.front(), decoded_sz);
}

std::ostream&
key_iterator :: describe(std::ostream& out) const
{
    return out << "key_iterator()";
}

e::slice
key_iterator :: internal_key()
{
    region_id ri;
    e::slice k;
    leveldb::Slice _k = m_iter->key();
    decode_key(_k, &ri, &k);
    return k;
}

bool
key_iterator :: sorted()
{
    return true;
}

void
key_iterator :: seek(const e::slice& ik)
{
    leveldb::Slice slice;
    encode_key(m_ri, m_range.type, ik, &m_scratch, &slice);
    m_iter->Seek(slice);
}

} // namespace

datalayer::index_iterator*
index_primitive :: iterator_from_range(leveldb_snapshot_ptr snap,
                                       const region_id& ri, 
                                       const range& r,
                                       index_info* key_ii)
{
    if (r.invalid)
    {
        return NULL;
    }

    if (r.attr != 0)
    {
        return new range_iterator(snap, ri, r, this, key_ii);
    }
    else
    {
        return new key_iterator(snap, ri, r, key_ii);
    }
}
