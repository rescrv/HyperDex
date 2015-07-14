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

#define __STDC_LIMIT_MACROS

// e
#include <e/endian.h>
#include <e/varint.h>

// HyperDex
#include "daemon/daemon.h"
#include "daemon/datalayer_encodings.h"
#include "daemon/datalayer_iterator.h"

using hyperdex::datalayer;
using hyperdex::leveldb_snapshot_ptr;

inline leveldb::Slice e2level(const e::slice& s) { return leveldb::Slice(reinterpret_cast<const char*>(s.data()), s.size()); }
inline e::slice level2e(const leveldb::Slice& s) { return e::slice(s.data(), s.size()); }

namespace
{

int
internal_key_compare(const e::slice& lhs, const e::slice& rhs)
{
    int cmp = memcmp(lhs.data(), rhs.data(), std::min(lhs.size(), rhs.size()));

    if (cmp == 0)
    {
        if (lhs.size() < rhs.size())
        {
            return -1;
        }

        if (lhs.size() > rhs.size())
        {
            return 1;
        }

        return 0;
    }

    return cmp;
}

} // namespace

//////////////////////////////// class iterator ////////////////////////////////

datalayer :: iterator :: iterator(leveldb_snapshot_ptr s)
    : m_ref(0)
    , m_snap(s)
{
}

leveldb_snapshot_ptr
datalayer :: iterator :: snap()
{
    return m_snap;
}

datalayer :: iterator :: ~iterator() throw ()
{
}

///////////////////////////// class replay_iterator ////////////////////////////

datalayer :: replay_iterator :: replay_iterator(const region_id& ri,
                                                leveldb_replay_iterator_ptr ptr,
                                                const index_encoding* ie)
    : m_ri(ri)
    , m_iter(ptr.get())
    , m_ptr(ptr)
    , m_decoded()
    , m_ie(ie)
{
}

bool
datalayer :: replay_iterator :: valid()
{
    char buf[sizeof(uint8_t) + VARINT_64_MAX_SIZE];
    char* ptr = buf;
    ptr = e::pack8be('o', ptr);
    ptr = e::packvarint64(m_ri.get(), ptr);
    leveldb::Slice prefix(buf, ptr - buf);

    while (m_iter->Valid())
    {
        if (m_iter->key().starts_with(prefix))
        {
            return true;
        }
        else if (m_iter->key().compare(prefix) < 0)
        {
            m_iter->SkipTo(prefix);
        }
        else
        {
            m_iter->SkipToLast();
        }
    }

    return false;
}

void
datalayer :: replay_iterator :: next()
{
    m_iter->Next();
}

bool
datalayer :: replay_iterator :: has_value()
{
    return m_iter->HasValue();
}

e::slice
datalayer :: replay_iterator :: key()
{
    const size_t sz = object_prefix_sz(m_ri);
    leveldb::Slice _k = m_iter->key();
    e::slice k = e::slice(_k.data() + sz, _k.size() - sz);
    size_t decoded_sz = m_ie->decoded_size(k);

    if (m_decoded.size() < decoded_sz)
    {
        m_decoded.resize(decoded_sz);
    }

    m_ie->decode(k, &m_decoded.front());
    return e::slice(&m_decoded.front(), decoded_sz);
}

datalayer::returncode
datalayer :: replay_iterator :: unpack_value(std::vector<e::slice>* value,
                                             uint64_t* version,
                                             reference* ref)
{
    ref->m_backing.assign(m_iter->value().data(), m_iter->value().size());
    e::slice v(ref->m_backing.data(), ref->m_backing.size());
    return decode_value(v, value, version);
}

leveldb::Status
datalayer :: replay_iterator :: status()
{
    return m_iter->status();
}

///////////////////////////// class dummy_iterator /////////////////////////////

datalayer :: dummy_iterator :: dummy_iterator()
    : iterator(leveldb_snapshot_ptr())
{
}

bool
datalayer :: dummy_iterator :: valid()
{
    return false;
}

void
datalayer :: dummy_iterator :: next()
{
}

uint64_t
datalayer :: dummy_iterator :: cost(leveldb::DB*)
{
    return 0;
}

e::slice
datalayer :: dummy_iterator :: key()
{
    return e::slice();
}

std::ostream&
datalayer :: dummy_iterator :: describe(std::ostream& out) const
{
    return out << "dummy_iterator()";
}

datalayer :: dummy_iterator :: ~dummy_iterator() throw ()
{
}

///////////////////////////// class region_iterator ////////////////////////////

datalayer :: region_iterator :: region_iterator(leveldb_iterator_ptr iter,
                                                const region_id& ri,
                                                const index_encoding* ie)
    : iterator(iter.snap())
    , m_iter(iter)
    , m_ri(ri)
    , m_decoded()
    , m_ie(ie)
{
    char buf[sizeof(uint8_t) + VARINT_64_MAX_SIZE];
    char* ptr = buf;
    ptr = e::pack8be('o', ptr);
    ptr = e::packvarint64(ri.get(), ptr);
    m_iter->Seek(leveldb::Slice(buf, ptr - buf));
}

datalayer :: region_iterator :: ~region_iterator() throw ()
{
}

std::ostream&
datalayer :: region_iterator :: describe(std::ostream& out) const
{
    return out << "region_iterator(" << m_ri << ")";
}

bool
datalayer :: region_iterator :: valid()
{
    if (!m_iter->Valid())
    {
        return false;
    }

    leveldb::Slice k = m_iter->key();

    if (k.size() < 2 || k.data()[0] != 'o')
    {
        return false;
    }

    uint64_t ri;

    if (!e::varint64_decode(k.data() + sizeof(uint8_t), k.data() + k.size(), &ri))
    {
        return false;
    }

    return region_id(ri) == m_ri;
}

void
datalayer :: region_iterator :: next()
{
    m_iter->Next();
}

uint64_t
datalayer :: region_iterator :: cost(leveldb::DB* db)
{
    const size_t sz = object_prefix_sz(m_ri);
    char buf[2 * (sizeof(uint8_t) + VARINT_64_MAX_SIZE)];
    char* ptr = buf;
    ptr = encode_object_prefix(m_ri, ptr);
    assert(ptr == buf + sz);
    ptr = encode_object_prefix(m_ri, ptr);
    assert(ptr == buf + sz + sz);
    encode_bump(buf + sz, buf + 2 * sz);
    // create the range
    leveldb::Range r;
    r.start = leveldb::Slice(buf, sz);
    r.limit = leveldb::Slice(buf + sz, sz);
    // ask leveldb for the size of the range
    uint64_t ret;
    db->GetApproximateSizes(&r, 1, &ret);
    return ret;
}

e::slice
datalayer :: region_iterator :: key()
{
    // first pull out the internal key
    leveldb::Slice _k = m_iter->key();
    const char* end = _k.data() + _k.size();
    const char* ptr = _k.data() + sizeof(uint8_t);
    uint64_t region;
    ptr = e::varint64_decode(ptr, end, &region);
    assert(ptr);

    // now convert that into its decoded form
    e::slice k = e::slice(ptr, end - ptr);
    size_t decoded_sz = m_ie->decoded_size(k);

    if (m_decoded.size() < decoded_sz)
    {
        m_decoded.resize(decoded_sz);
    }

    m_ie->decode(k, &m_decoded.front());
    return e::slice(&m_decoded.front(), decoded_sz);
}

///////////////////////////// class index_iterator /////////////////////////////

datalayer :: index_iterator :: index_iterator(leveldb_snapshot_ptr s)
    : iterator(s)
{
}

datalayer :: index_iterator :: ~index_iterator() throw ()
{
}

////////////////////////// class range_index_iterator //////////////////////////

datalayer :: range_index_iterator :: range_index_iterator(leveldb_snapshot_ptr s,
                                                          size_t prefix_sz,
                                                          const e::slice& range_lower,
                                                          const e::slice& range_upper,
                                                          bool has_lower,
                                                          bool has_upper,
                                                          const index_encoding* val_ie,
                                                          const index_encoding* key_ie)
    : index_iterator(s)
    , m_iter()
    , m_val_ie(val_ie)
    , m_key_ie(key_ie)
    , m_prefix()
    , m_range_lower()
    , m_range_upper()
    , m_value_lower()
    , m_value_upper()
    , m_range_buf(range_lower.size() + range_upper.size())
    , m_scratch()
    , m_has_lower(has_lower)
    , m_has_upper(has_upper)
    , m_invalid(false)
{
    // setup the iterator
    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    opts.snapshot = s.get();
    m_iter.reset(s, s.db()->NewIterator(opts));

    // copy the lower/upper into our buffer
    memmove(&m_range_buf[0], range_lower.data(), range_lower.size());
    memmove(&m_range_buf[0] + range_lower.size(), range_upper.data(), range_upper.size());
    m_range_lower = e::slice(&m_range_buf[0], range_lower.size());
    m_range_upper = e::slice(&m_range_buf[0] + range_lower.size(), range_upper.size());
    assert(m_range_lower.size() >= prefix_sz);
    assert(m_range_upper.size() >= prefix_sz);
    assert(memcmp(m_range_lower.data(), m_range_upper.data(), prefix_sz) == 0);
    m_prefix = e::slice(m_range_lower.data(), prefix_sz);

    // pull the value lower/upper
    if (m_has_lower)
    {
        m_invalid = !decode_entry_keyless(m_range_lower, &m_value_lower) || m_invalid;
    }

    // pull the value lower/upper
    if (m_has_upper)
    {
        m_invalid = !decode_entry_keyless(m_range_upper, &m_value_upper) || m_invalid;
    }

    m_iter->Seek(e2level(m_range_lower));
}

datalayer :: range_index_iterator :: ~range_index_iterator() throw ()
{
}

bool
datalayer :: range_index_iterator :: valid()
{
    while (!m_invalid && m_iter->Valid())
    {
        if (!m_iter->key().starts_with(e2level(m_prefix)))
        {
            m_invalid = true;
            return false;
        }

        e::slice current = level2e(m_iter->key());
        e::slice iv;
        e::slice ik;

        if (!decode_entry(current, &iv, &ik))
        {
            m_invalid = true;
            return false;
        }

        size_t sz = std::min(m_range_upper.size(), current.size());

        if (m_has_upper && memcmp(m_range_upper.data(), current.data(), sz) < 0)
        {
            m_invalid = true;
            return false;
        }

        if (m_has_lower && internal_key_compare(m_value_lower, iv) > 0)
        {
            m_iter->Next();
            continue;
        }

        if (m_has_upper && internal_key_compare(m_value_upper, iv) < 0)
        {
            m_iter->Next();
            continue;
        }

        return true;
    }

    return false;
}

void
datalayer :: range_index_iterator :: next()
{
    m_iter->Next();
}

uint64_t
datalayer :: range_index_iterator :: cost(leveldb::DB* db)
{
    if (m_scratch.size() < m_range_upper.size())
    {
        m_scratch.resize(m_range_upper.size());
    }

    memmove(&m_scratch[0], m_range_upper.data(), m_range_upper.size());
    hyperdex::encode_bump(&m_scratch[0], &m_scratch[0] + m_range_upper.size());
    // create the range
    leveldb::Range r;
    r.start = m_iter->key();
    r.limit = leveldb::Slice(&m_scratch[0], m_range_upper.size());
    // ask leveldb for the size of the range
    uint64_t ret;
    db->GetApproximateSizes(&r, 1, &ret);
    return ret;
}

e::slice
datalayer :: range_index_iterator :: key()
{
    e::slice ik = this->internal_key();
    size_t decoded_sz = m_key_ie->decoded_size(ik);

    if (m_scratch.size() < decoded_sz)
    {
        m_scratch.resize(decoded_sz);
    }

    m_key_ie->decode(ik, &m_scratch.front());
    return e::slice(&m_scratch.front(), decoded_sz);
}

std::ostream&
datalayer :: range_index_iterator :: describe(std::ostream& out) const
{
    return out << "range_iterator()";
}

e::slice
datalayer :: range_index_iterator :: internal_key()
{
    leveldb::Slice in = m_iter->key();
    e::slice v;
    e::slice k;
    decode_entry(level2e(in), &v, &k);
    return k;
}

bool
datalayer :: range_index_iterator :: sorted()
{
    return m_has_lower && m_has_upper && m_value_lower == m_value_upper;
}

void
datalayer :: range_index_iterator :: seek(const e::slice& ik)
{
    leveldb::Slice in = m_iter->key();
    e::slice v;
    e::slice k;
    decode_entry(level2e(in), &v, &k);
    encode_entry(v, ik, &m_scratch, &k);
    m_iter->Seek(e2level(k));
}

bool
datalayer :: range_index_iterator :: decode_entry(const e::slice& in, e::slice* v, e::slice* k)
{
    assert(in.starts_with(m_prefix));
    const char* ptr = reinterpret_cast<const char*>(in.data());
    const char* const end = ptr + in.size();
    ptr += m_prefix.size();
    size_t rem = end - ptr;

    if (!m_val_ie)
    {
        *k = e::slice(ptr, rem);
        *v = *k;
    }
    else if (m_val_ie->encoding_fixed())
    {
        size_t sz = m_val_ie->encoded_size(e::slice());

        if (sz > rem)
        {
            return false;
        }

        *v = e::slice(ptr, sz);
        *k = e::slice(ptr + sz, rem - sz);
    }
    else if (m_key_ie->encoding_fixed())
    {
        size_t sz = m_key_ie->encoded_size(e::slice());

        if (sz > rem)
        {
            return false;
        }

        *v = e::slice(ptr, rem - sz);
        *k = e::slice(ptr + rem - sz, sz);
    }
    else
    {
        if (rem < sizeof(uint32_t))
        {
            return false;
        }

        uint32_t k_sz;
        e::unpack32be(end - sizeof(uint32_t), &k_sz);

        if (k_sz + sizeof(uint32_t) > rem)
        {
            return false;
        }

        *v = e::slice(ptr, rem - sizeof(uint32_t) - k_sz);
        *k = e::slice(ptr + v->size(), k_sz);
    }

    return true;
}

bool
datalayer :: range_index_iterator :: decode_entry_keyless(const e::slice& in, e::slice* val)
{
    assert(in.starts_with(m_prefix));
    *val = e::slice(in.data() + m_prefix.size(), in.size() - m_prefix.size());
    return true;
}

void
datalayer :: range_index_iterator :: encode_entry(const e::slice& v,
                                                  const e::slice& k,
                                                  std::vector<char>* scratch,
                                                  e::slice* slice)
{
    if (!m_val_ie)
    {
        size_t sz = m_prefix.size() + k.size();

        if (scratch->size() < sz)
        {
            scratch->resize(sz);
        }

        char* ptr = &(*scratch)[0];
        memmove(ptr, m_prefix.data(), m_prefix.size());
        ptr += m_prefix.size();
        memmove(ptr, k.data(), k.size());
        ptr += k.size();
        *slice = e::slice(&(*scratch)[0], ptr - &(*scratch)[0]);
    }
    else
    {
        size_t sz = m_prefix.size() + v.size() + k.size() + sizeof(uint32_t);

        if (scratch->size() < sz)
        {
            scratch->resize(sz);
        }

        char* ptr = &(*scratch)[0];
        memmove(ptr, m_prefix.data(), m_prefix.size());
        ptr += m_prefix.size();
        memmove(ptr, v.data(), v.size());
        ptr += v.size();
        memmove(ptr, k.data(), k.size());
        ptr += k.size();

        if (!m_val_ie->encoding_fixed() && !m_key_ie->encoding_fixed())
        {
            ptr = e::pack32be(k.size(), ptr);
        }

        *slice = e::slice(&(*scratch)[0], ptr - &(*scratch)[0]);
    }
}

//////////////////////////// class intersect_iterator ////////////////////////////

datalayer :: intersect_iterator :: intersect_iterator(leveldb_snapshot_ptr s,
                                                      const std::vector<e::intrusive_ptr<index_iterator> >& iterators)
    : index_iterator(s)
    , m_iters()
    , m_cost(0)
    , m_invalid(false)
{
    assert(!iterators.empty());
    std::vector<std::pair<uint64_t, e::intrusive_ptr<index_iterator> > > iters;

    for (size_t i = 0; i < iterators.size(); ++i)
    {
        assert(iterators[i]->sorted());
        iters.push_back(std::make_pair(iterators[i]->cost(s.db()), iterators[i]));
    }

    std::sort(iters.begin(), iters.end());
    m_iters.resize(iters.size());

    for (size_t i = 0; i < iters.size(); ++i)
    {
        m_cost += iters[i].first;
        m_iters[i] = iters[i].second;
    }

    for (size_t i = 0; i < m_iters.size(); ++i)
    {
        if (!m_iters[i]->valid())
        {
            m_invalid = true;
        }
    }
}

datalayer :: intersect_iterator :: ~intersect_iterator() throw ()
{
}

bool
datalayer :: intersect_iterator :: valid()
{
    while (!m_invalid && m_iters[0]->valid())
    {
        bool retry = false;

        for (size_t i = 1; i < m_iters.size(); ++i)
        {
            m_iters[i]->seek(m_iters[0]->internal_key());

            if (!m_iters[i]->valid())
            {
                m_invalid = true;
                return false;
            }

            int cmp = internal_key_compare(m_iters[0]->internal_key(),
                                           m_iters[i]->internal_key());

            if (cmp < 0)
            {
                m_iters[0]->seek(m_iters[i]->internal_key());
                retry = true;
                break;
            }

            assert(cmp == 0);
        }

        if (!retry)
        {
            return true;
        }
    }

    m_invalid = true;
    return false;
}

void
datalayer :: intersect_iterator :: next()
{
    m_iters[0]->next();
}

uint64_t
datalayer :: intersect_iterator :: cost(leveldb::DB*)
{
    return m_cost;
}

e::slice
datalayer :: intersect_iterator :: key()
{
    return m_iters[0]->key();
}

std::ostream&
datalayer :: intersect_iterator :: describe(std::ostream& out) const
{
    out << "intersect_iterator(";

    for (size_t i = 0; i < m_iters.size(); ++i)
    {
        if (i > 0)
        {
            out << ", ";
        }

        out << *m_iters[i];
    }

    return out << ")";
}

e::slice
datalayer :: intersect_iterator :: internal_key()
{
    return m_iters[0]->internal_key();
}

bool
datalayer :: intersect_iterator :: sorted()
{
    return true;
}

void
datalayer :: intersect_iterator :: seek(const e::slice& k)
{
    return m_iters[0]->seek(k);
}

///////////////////////////// class search_iterator ////////////////////////////

datalayer :: search_iterator :: search_iterator(datalayer* dl,
                                                const region_id& ri,
                                                e::intrusive_ptr<index_iterator> iter,
                                                std::ostringstream* ostr,
                                                const std::vector<attribute_check>* checks)
    : iterator(iter->snap())
    , m_dl(dl)
    , m_ri(ri)
    , m_iter(iter)
    , m_error(SUCCESS)
    , m_ostr(ostr)
    , m_num_gets(0)
    , m_checks(checks)
{
}

datalayer :: search_iterator :: ~search_iterator() throw ()
{
}

std::ostream&
datalayer :: search_iterator :: describe(std::ostream& out) const
{
    return out << "search_iterator(" << *m_iter << ")";
}

bool
datalayer :: search_iterator :: valid()
{
    if (m_error != SUCCESS)
    {
        return false;
    }

    // Don't try to optimize by replacing m_ri with a const schema* because it
    // won't persist across reconfigurations
    const schema& sc(*m_dl->m_daemon->m_config.get_schema(m_ri));

    uint64_t version;
    std::vector<e::slice> value;
    reference ref;

    // while the most selective iterator is valid and not past the end
    while (m_iter->valid())
    {
        leveldb::ReadOptions opts;
        opts.fill_cache = true;
        opts.verify_checksums = true;
        std::vector<char> kbacking;
        leveldb::Slice lkey;
        encode_key(m_ri, sc.attrs[0].type, m_iter->key(), &kbacking, &lkey);
        leveldb::Status st = m_dl->m_db->Get(opts, lkey, &ref.m_backing);

        if (st.ok())
        {
            e::slice v(ref.m_backing.data(), ref.m_backing.size());
            datalayer::returncode rc = decode_value(v, &value, &version);

            if (rc != SUCCESS)
            {
                m_error = rc;
                return false;
            }

            ++m_num_gets;
        }
        else
        {
            m_error = m_dl->handle_error(st);
            return false;
        }

        if (passes_attribute_checks(sc, *m_checks, m_iter->key(), value) == m_checks->size())
        {
            return true;
        }
        else
        {
            m_iter->next();
        }
    }

    if (m_ostr) *m_ostr << " iterator retrieved " << m_num_gets << " objects from disk\n";
    return false;
}

void
datalayer :: search_iterator :: next()
{
    m_iter->next();
}

uint64_t
datalayer :: search_iterator :: cost(leveldb::DB* db)
{
    return m_iter->cost(db);
}

e::slice
datalayer :: search_iterator :: key()
{
    return m_iter->key();
}
