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

// LevelDB
#include <leveldb/write_batch.h>

// e
#include <e/endian.h>

// HyperDex
#include "daemon/datalayer_encodings.h"
#include "daemon/index_encode.h"

using hyperdex::datalayer;

void
hyperdex :: encode_key(const region_id& ri,
                       const e::slice& key,
                       std::vector<char>* backing,
                       leveldb::Slice* out)
{
    size_t sz = sizeof(uint8_t) + sizeof(uint64_t) + key.size();

    if (backing->size() < sz)
    {
        backing->clear();
        backing->resize(sz);
    }

    char* ptr = &backing->front();
    ptr = e::pack8be('o', ptr);
    ptr = e::pack64be(ri.get(), ptr);
    memmove(ptr, key.data(), key.size());
    *out = leveldb::Slice(&backing->front(), sz);
}

datalayer::returncode
hyperdex :: decode_key(const e::slice& in,
                       region_id* ri,
                       e::slice* key)
{
    const uint8_t* ptr = in.data();
    const uint8_t* end = ptr + in.size();

    if (ptr >= end || *ptr != 'o')
    {
        return datalayer::BAD_ENCODING;
    }

    ++ptr;
    uint64_t rid;

    if (ptr + sizeof(uint64_t) <= end)
    {
        ptr = e::unpack64be(ptr, &rid);
    }
    else
    {
        return datalayer::BAD_ENCODING;
    }

    *ri  = region_id(rid);
    *key = e::slice(ptr, end - ptr);
    return datalayer::SUCCESS;
}

void
hyperdex :: encode_value(const std::vector<e::slice>& attrs,
                         uint64_t version,
                         std::vector<char>* backing,
                         leveldb::Slice* out)
{
    assert(attrs.size() < 65536);
    size_t sz = sizeof(uint64_t) + sizeof(uint16_t);

    for (size_t i = 0; i < attrs.size(); ++i)
    {
        sz += sizeof(uint32_t) + attrs[i].size();
    }

    backing->resize(sz);
    char* ptr = &backing->front();
    ptr = e::pack64be(version, ptr);
    ptr = e::pack16be(attrs.size(), ptr);

    for (size_t i = 0; i < attrs.size(); ++i)
    {
        ptr = e::pack32be(attrs[i].size(), ptr);
        memmove(ptr, attrs[i].data(), attrs[i].size());
        ptr += attrs[i].size();
    }

    *out = leveldb::Slice(&backing->front(), sz);
}

datalayer::returncode
hyperdex :: decode_value(const e::slice& in,
                         std::vector<e::slice>* attrs,
                         uint64_t* version)
{
    const uint8_t* ptr = in.data();
    const uint8_t* end = ptr + in.size();

    if (ptr + sizeof(uint64_t) <= end)
    {
        ptr = e::unpack64be(ptr, version);
    }
    else
    {
        return datalayer::BAD_ENCODING;
    }

    uint16_t num_attrs;

    if (ptr + sizeof(uint16_t) <= end)
    {
        ptr = e::unpack16be(ptr, &num_attrs);
    }
    else
    {
        return datalayer::BAD_ENCODING;
    }

    attrs->clear();

    for (size_t i = 0; i < num_attrs; ++i)
    {
        uint32_t sz = 0;

        if (ptr + sizeof(uint32_t) <= end)
        {
            ptr = e::unpack32be(ptr, &sz);
        }
        else
        {
            return datalayer::BAD_ENCODING;
        }

        e::slice s(reinterpret_cast<const uint8_t*>(ptr), sz);
        ptr += sz;
        attrs->push_back(s);
    }

    return datalayer::SUCCESS;
}

void
hyperdex :: encode_acked(const region_id& ri, /*region we saw an ack for*/
                         const region_id& reg_id, /*region of the point leader*/
                         uint64_t seq_id,
                         char* out)
{
    char* ptr = out;
    ptr = e::pack8be('a', ptr);
    ptr = e::pack64be(reg_id.get(), ptr);
    ptr = e::pack64be(seq_id, ptr);
    ptr = e::pack64be(ri.get(), ptr);
}

datalayer::returncode
hyperdex :: decode_acked(const e::slice& in,
                         region_id* ri, /*region we saw an ack for*/
                         region_id* reg_id, /*region of the point leader*/
                         uint64_t* seq_id)
{
    if (in.size() != ACKED_BUF_SIZE)
    {
        return datalayer::BAD_ENCODING;
    }

    uint8_t _p;
    uint64_t _ri;
    uint64_t _reg_id;
    const uint8_t* ptr = in.data();
    ptr = e::unpack8be(ptr, &_p);
    ptr = e::unpack64be(ptr, &_reg_id);
    ptr = e::unpack64be(ptr, seq_id);
    ptr = e::unpack64be(ptr, &_ri);
    *ri = region_id(_ri);
    *reg_id = region_id(_reg_id);
    return _p == 'a' ? datalayer::SUCCESS : datalayer::BAD_ENCODING;
}

void
hyperdex :: encode_transfer(const capture_id& ci,
                            uint64_t count,
                            char* out)
{
    char* ptr = out;
    ptr = e::pack8be('t', ptr);
    ptr = e::pack64be(ci.get(), ptr);
    ptr = e::pack64be(count, ptr);
}

void
hyperdex :: encode_key_value(const e::slice& key,
                             /*pointer to make it optional*/
                             const std::vector<e::slice>* value,
                             uint64_t version,
                             std::vector<char>* backing, /*XXX*/
                             leveldb::Slice* out)
{
    size_t sz = sizeof(uint32_t) + key.size() + sizeof(uint64_t) + sizeof(uint16_t);

    for (size_t i = 0; value && i < value->size(); ++i)
    {
        sz += sizeof(uint32_t) + (*value)[i].size();
    }

    backing->resize(sz);
    char* ptr = &backing->front();
    *out = leveldb::Slice(ptr, sz);
    ptr = e::pack32be(key.size(), ptr);
    memmove(ptr, key.data(), key.size());
    ptr += key.size();
    ptr = e::pack64be(version, ptr);

    if (value)
    {
        ptr = e::pack16be(value->size(), ptr);

        for (size_t i = 0; i < value->size(); ++i)
        {
            ptr = e::pack32be((*value)[i].size(), ptr);
            memmove(ptr, (*value)[i].data(), (*value)[i].size());
            ptr += (*value)[i].size();
        }
    }
}

datalayer::returncode
hyperdex :: decode_key_value(const e::slice& in,
                             bool* has_value,
                             e::slice* key,
                             std::vector<e::slice>* value,
                             uint64_t* version)
{
    const uint8_t* ptr = in.data();
    const uint8_t* end = ptr + in.size();

    if (ptr >= end)
    {
        return datalayer::BAD_ENCODING;
    }

    uint32_t key_sz;

    if (ptr + sizeof(uint32_t) <= end)
    {
        ptr = e::unpack32be(ptr, &key_sz);
    }
    else
    {
        return datalayer::BAD_ENCODING;
    }

    if (ptr + key_sz <= end)
    {
        *key = e::slice(ptr, key_sz);
        ptr += key_sz;
    }
    else
    {
        return datalayer::BAD_ENCODING;
    }

    if (ptr + sizeof(uint64_t) <= end)
    {
        ptr = e::unpack64be(ptr, version);
    }
    else
    {
        return datalayer::BAD_ENCODING;
    }

    if (ptr == end)
    {
        *has_value = false;
        return datalayer::SUCCESS;
    }

    uint16_t num_attrs;

    if (ptr + sizeof(uint16_t) <= end)
    {
        ptr = e::unpack16be(ptr, &num_attrs);
    }
    else
    {
        return datalayer::BAD_ENCODING;
    }

    value->clear();

    for (size_t i = 0; i < num_attrs; ++i)
    {
        uint32_t sz = 0;

        if (ptr + sizeof(uint32_t) <= end)
        {
            ptr = e::unpack32be(ptr, &sz);
        }
        else
        {
            return datalayer::BAD_ENCODING;
        }

        e::slice s(ptr, sz);
        ptr += sz;
        value->push_back(s);
    }

    *has_value = true;
    return datalayer::SUCCESS;
}

void
hyperdex :: encode_index(const region_id& ri,
                         uint16_t attr,
                         std::vector<char>* backing)
{
    size_t sz = sizeof(uint8_t)
              + sizeof(uint64_t)
              + sizeof(uint16_t);
    backing->resize(sz);
    char* ptr = &backing->front();
    ptr = e::pack8be('i', ptr);
    ptr = e::pack64be(ri.get(), ptr);
    ptr = e::pack16be(attr, ptr);
}

void
hyperdex :: encode_index(const region_id& ri,
                         uint16_t attr,
                         hyperdatatype type,
                         const e::slice& value,
                         std::vector<char>* backing)
{
    size_t sz = sizeof(uint8_t)
              + sizeof(uint64_t)
              + sizeof(uint16_t);
    char* ptr = NULL;
    char buf_i[sizeof(int64_t)];
    char buf_d[sizeof(double)];
    int64_t tmp_i;
    double tmp_d;

    switch (type)
    {
        case HYPERDATATYPE_STRING:
            backing->resize(sz + value.size());
            ptr = &backing->front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            memmove(ptr, value.data(), value.size());
            break;
        case HYPERDATATYPE_INT64:
            backing->resize(sz + sizeof(uint64_t));
            ptr = &backing->front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            memset(buf_i, 0, sizeof(int64_t));
            memmove(buf_i, value.data(), std::min(value.size(), sizeof(int64_t)));
            e::unpack64le(buf_i, &tmp_i);
            ptr = index_encode_int64(tmp_i, ptr);
            break;
        case HYPERDATATYPE_FLOAT:
            backing->resize(sz + sizeof(double));
            ptr = &backing->front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            memset(buf_d, 0, sizeof(double));
            memmove(buf_d, value.data(), std::min(value.size(), sizeof(double)));
            e::unpackdoublele(buf_d, &tmp_d);
            ptr = index_encode_double(tmp_d, ptr);
            break;
        case HYPERDATATYPE_GENERIC:
        case HYPERDATATYPE_LIST_GENERIC:
        case HYPERDATATYPE_LIST_STRING:
        case HYPERDATATYPE_LIST_INT64:
        case HYPERDATATYPE_LIST_FLOAT:
        case HYPERDATATYPE_SET_GENERIC:
        case HYPERDATATYPE_SET_STRING:
        case HYPERDATATYPE_SET_INT64:
        case HYPERDATATYPE_SET_FLOAT:
        case HYPERDATATYPE_MAP_GENERIC:
        case HYPERDATATYPE_MAP_STRING_KEYONLY:
        case HYPERDATATYPE_MAP_STRING_STRING:
        case HYPERDATATYPE_MAP_STRING_INT64:
        case HYPERDATATYPE_MAP_STRING_FLOAT:
        case HYPERDATATYPE_MAP_INT64_KEYONLY:
        case HYPERDATATYPE_MAP_INT64_STRING:
        case HYPERDATATYPE_MAP_INT64_INT64:
        case HYPERDATATYPE_MAP_INT64_FLOAT:
        case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
        case HYPERDATATYPE_MAP_FLOAT_STRING:
        case HYPERDATATYPE_MAP_FLOAT_INT64:
        case HYPERDATATYPE_MAP_FLOAT_FLOAT:
        case HYPERDATATYPE_GARBAGE:
        default:
            abort();
    }
}

void
hyperdex :: encode_index(const region_id& ri,
                         uint16_t attr,
                         hyperdatatype type,
                         const e::slice& value,
                         const e::slice& key,
                         std::vector<char>* backing)
{
    size_t sz = sizeof(uint8_t)
              + sizeof(uint64_t)
              + sizeof(uint16_t);
    char* ptr = NULL;
    char buf_i[sizeof(int64_t)];
    char buf_d[sizeof(double)];
    int64_t tmp_i;
    double tmp_d;

    switch (type)
    {
        case HYPERDATATYPE_STRING:
            backing->resize(sz + value.size() + key.size() + sizeof(uint32_t));
            ptr = &backing->front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            memmove(ptr, value.data(), value.size());
            ptr += value.size();
            memmove(ptr, key.data(), key.size());
            ptr += key.size();
            ptr = e::pack32be(key.size(), ptr);
            break;
        case HYPERDATATYPE_INT64:
            backing->resize(sz + sizeof(uint64_t) + key.size());
            ptr = &backing->front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            memset(buf_i, 0, sizeof(int64_t));
            memmove(buf_i, value.data(), std::min(value.size(), sizeof(int64_t)));
            e::unpack64le(buf_i, &tmp_i);
            ptr = index_encode_int64(tmp_i, ptr);
            memmove(ptr, key.data(), key.size());
            break;
        case HYPERDATATYPE_FLOAT:
            backing->resize(sz + sizeof(double) + key.size());
            ptr = &backing->front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            memset(buf_d, 0, sizeof(double));
            memmove(buf_d, value.data(), std::min(value.size(), sizeof(double)));
            e::unpackdoublele(buf_d, &tmp_d);
            ptr = index_encode_double(tmp_d, ptr);
            memmove(ptr, key.data(), key.size());
            break;
        case HYPERDATATYPE_GENERIC:
        case HYPERDATATYPE_LIST_GENERIC:
        case HYPERDATATYPE_LIST_STRING:
        case HYPERDATATYPE_LIST_INT64:
        case HYPERDATATYPE_LIST_FLOAT:
        case HYPERDATATYPE_SET_GENERIC:
        case HYPERDATATYPE_SET_STRING:
        case HYPERDATATYPE_SET_INT64:
        case HYPERDATATYPE_SET_FLOAT:
        case HYPERDATATYPE_MAP_GENERIC:
        case HYPERDATATYPE_MAP_STRING_KEYONLY:
        case HYPERDATATYPE_MAP_STRING_STRING:
        case HYPERDATATYPE_MAP_STRING_INT64:
        case HYPERDATATYPE_MAP_STRING_FLOAT:
        case HYPERDATATYPE_MAP_INT64_KEYONLY:
        case HYPERDATATYPE_MAP_INT64_STRING:
        case HYPERDATATYPE_MAP_INT64_INT64:
        case HYPERDATATYPE_MAP_INT64_FLOAT:
        case HYPERDATATYPE_MAP_FLOAT_KEYONLY:
        case HYPERDATATYPE_MAP_FLOAT_STRING:
        case HYPERDATATYPE_MAP_FLOAT_INT64:
        case HYPERDATATYPE_MAP_FLOAT_FLOAT:
        case HYPERDATATYPE_GARBAGE:
        default:
            abort();
    }
}

void
hyperdex :: bump_index(std::vector<char>* backing)
{
    assert(!backing->empty());
    assert((*backing)[0] ^ 0x80);
    index_encode_bump(&backing->front(),
                      &backing->front() + backing->size());
}

bool
hyperdex :: parse_index_string(const leveldb::Slice& s, e::slice* k)
{
    size_t sz = sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint16_t) + sizeof(uint32_t);

    if (s.size() >= sz)
    {
        uint32_t key_sz;
        const char* ptr = s.data() + s.size() - sizeof(uint32_t);
        e::unpack32be(ptr, &key_sz);

        if (s.size() >= sz + key_sz)
        {
            *k = e::slice(s.data() + s.size() - sizeof(uint32_t) - key_sz, key_sz);
            return true;
        }
    }

    return false;
}

bool
hyperdex :: parse_index_sizeof8(const leveldb::Slice& s, e::slice* k)
{
    size_t sz = sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint16_t) + sizeof(uint64_t);

    if (s.size() >= sz && s.data()[0] == 'i')
    {
        *k = e::slice(s.data() + sz, s.size() - sz);
        return true;
    }

    return false;
}

bool
hyperdex :: parse_object_key(const leveldb::Slice& s, e::slice* k)
{
    region_id tmp;
    return decode_key(e::slice(s.data(), s.size()), &tmp, k) == datalayer::SUCCESS;
}

static void
generate_index(const hyperdex::region_id& ri,
               uint16_t attr,
               hyperdatatype type,
               const e::slice& value,
               const e::slice& key,
               std::vector<char>* backing,
               leveldb::Slice* idx)
{
    if (type == HYPERDATATYPE_STRING ||
        type == HYPERDATATYPE_INT64 ||
        type == HYPERDATATYPE_FLOAT)
    {
        encode_index(ri, attr, type, value, key, backing);
        *idx = leveldb::Slice(&backing->front(), backing->size());
    }
}

datalayer::returncode
hyperdex :: create_index_changes(const schema* sc,
                                 const subspace* su,
                                 const region_id& ri,
                                 const e::slice& key,
                                 const std::vector<e::slice>* old_value,
                                 const std::vector<e::slice>* new_value,
                                 leveldb::WriteBatch* updates)
{
    std::vector<char> backing;
    leveldb::Slice slice;
    leveldb::Slice empty("", 0);

    if (old_value && new_value)
    {
        assert(old_value->size() + 1 == sc->attrs_sz);
        assert(old_value->size() == new_value->size());

        for (size_t j = 0; j < su->attrs.size(); ++j)
        {
            size_t attr = su->attrs[j];
            assert(attr < sc->attrs_sz);

            if (attr > 0 && (*old_value)[attr - 1] != (*new_value)[attr - 1])
            {
                generate_index(ri, attr, sc->attrs[attr].type, (*old_value)[attr - 1], key, &backing, &slice);
                updates->Delete(slice);
                generate_index(ri, attr, sc->attrs[attr].type, (*new_value)[attr - 1], key, &backing, &slice);
                updates->Put(slice, empty);
            }
        }
    }
    else if (old_value)
    {
        assert(old_value->size() + 1 == sc->attrs_sz);

        for (size_t j = 0; j < su->attrs.size(); ++j)
        {
            size_t attr = su->attrs[j];
            assert(attr < sc->attrs_sz);

            if (attr > 0)
            {
                generate_index(ri, attr, sc->attrs[attr].type, (*old_value)[attr - 1], key, &backing, &slice);
                updates->Delete(slice);
            }
        }
    }
    else if (new_value)
    {
        assert(new_value->size() + 1 == sc->attrs_sz);

        for (size_t j = 0; j < su->attrs.size(); ++j)
        {
            size_t attr = su->attrs[j];
            assert(attr < sc->attrs_sz);

            if (attr > 0)
            {
                generate_index(ri, attr, sc->attrs[attr].type, (*new_value)[attr - 1], key, &backing, &slice);
                updates->Put(slice, empty);
            }
        }
    }

    return datalayer::SUCCESS;
}
