// Copyright (c) 2012, Cornell University
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

// STL
#include <string>

// Google Log
#include <glog/logging.h>

// LevelDB
#include <leveldb/write_batch.h>
#include <leveldb/filter_policy.h>

// e
#include <e/endian.h>

// HyperDex
#include "common/macros.h"
#include "daemon/daemon.h"
#include "daemon/datalayer.h"
#include "daemon/index_encode.h"
#include "datatypes/apply.h"
#include "datatypes/microerror.h"

using hyperdex::datalayer;
using hyperdex::reconfigure_returncode;

class datalayer::search_filter
{
    public:
        static bool sort_by_size(const datalayer::search_filter& lhs, const datalayer::search_filter& rhs);

    public:
        search_filter();
        search_filter(const search_filter&);
        ~search_filter() throw ();

    public:
        search_filter& operator = (const search_filter&);

    public:
        leveldb::Range range;
        void (*parse)(const leveldb::Slice& in, e::slice* out);
        uint64_t size;
        leveldb::Iterator* iter;
};

bool
datalayer :: search_filter :: sort_by_size(const datalayer::search_filter& lhs,
                                           const datalayer::search_filter& rhs)
{
    return lhs.size < rhs.size;
}

datalayer :: search_filter :: search_filter()
    : range()
    , parse(NULL)
    , size(0)
    , iter(NULL)
{
}

datalayer :: search_filter :: search_filter(const search_filter& other)
    : range(other.range)
    , parse(other.parse)
    , size(other.size)
    , iter(other.iter)
{
}

datalayer :: search_filter :: ~search_filter() throw ()
{
}

datalayer::search_filter&
datalayer :: search_filter :: operator = (const search_filter& rhs)
{
    range = rhs.range;
    size = rhs.size;
    iter = rhs.iter;
    return *this;
}

static void
parse_index_empty(const leveldb::Slice&, e::slice* k)
{
    *k = e::slice("", 0);
}

static void
parse_index_string(const leveldb::Slice& s, e::slice* k)
{
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(s.data());
    const uint8_t* end = ptr + s.size();
    uint32_t sz;
    e::unpack32be(end - sizeof(uint32_t), &sz);
    ptr = end - sizeof(uint32_t) - sz;
    *k = e::slice(ptr, sz);
}

static void
parse_index_sizeof8(const leveldb::Slice& s, e::slice* k)
{
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(s.data());
    const uint8_t* end = ptr + s.size();
    ptr += sizeof(uint8_t) + sizeof(uint64_t) + sizeof(uint32_t)
         + sizeof(uint16_t) + sizeof(uint8_t) + sizeof(uint16_t)
         + sizeof(uint64_t);
    *k = e::slice(ptr, end - ptr);
}

datalayer :: datalayer(daemon* d)
    : m_daemon(d)
    , m_db(NULL)
{
}

datalayer :: ~datalayer() throw ()
{
    // Intentionally leak m_db if destructed.  It must be released from
    // "teardown" to ensure we clean up outstanding snapshots first.
}

bool
datalayer :: setup(const po6::pathname& path,
                   bool* saved,
                   server_id* saved_us,
                   po6::net::location* saved_bind_to,
                   po6::net::hostname* saved_coordinator)
{
    leveldb::Options opts;
    opts.create_if_missing = true;
    opts.filter_policy = leveldb::NewBloomFilterPolicy(10);
    std::string name(path.get());
    leveldb::Status st = leveldb::DB::Open(opts, name, &m_db);

    if (!st.ok())
    {
        LOG(ERROR) << "could not open LevelDB: " << st.ToString();
        return false;
    }

    leveldb::ReadOptions ropts;
    ropts.fill_cache = true;
    ropts.verify_checksums = true;
    leveldb::WriteOptions wopts;
    wopts.sync = true;

    leveldb::Slice rk("hyperdex", 8);
    std::string rbacking;
    st = m_db->Get(ropts, rk, &rbacking);
    bool first_time = false;

    if (st.ok())
    {
        first_time = false;

        if (rbacking != PACKAGE_VERSION)
        {
            LOG(ERROR) << "could not restore from LevelDB because "
                       << "the existing data was created by "
                       << "HyperDex " << rbacking << " but "
                       << "this is version " << PACKAGE_VERSION;
            return false;
        }
    }
    else if (st.IsNotFound())
    {
        first_time = true;
        leveldb::Slice k("hyperdex", 8);
        leveldb::Slice v(PACKAGE_VERSION, strlen(PACKAGE_VERSION));
        st = m_db->Put(wopts, k, v);

        if (st.ok())
        {
            // fall through
        }
        else if (st.IsNotFound())
        {
            LOG(ERROR) << "could not restore from LevelDB because Put returned NotFound:  "
                       << st.ToString();
            return false;
        }
        else if (st.IsCorruption())
        {
            LOG(ERROR) << "could not restore from LevelDB because of corruption:  "
                       << st.ToString();
            return false;
        }
        else if (st.IsIOError())
        {
            LOG(ERROR) << "could not restore from LevelDB because of an IO error:  "
                       << st.ToString();
            return false;
        }
        else
        {
            LOG(ERROR) << "could not restore from LevelDB because it returned an "
                       << "unknown error that we don't know how to handle:  "
                       << st.ToString();
            return false;
        }
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "could not restore from LevelDB because of corruption:  "
                   << st.ToString();
        return false;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "could not restore from LevelDB because of an IO error:  "
                   << st.ToString();
        return false;
    }
    else
    {
        LOG(ERROR) << "could not restore from LevelDB because it returned an "
                   << "unknown error that we don't know how to handle:  "
                   << st.ToString();
        return false;
    }

    leveldb::Slice sk("state", 5);
    std::string sbacking;
    st = m_db->Get(ropts, sk, &sbacking);

    if (st.ok())
    {
        if (first_time)
        {
            LOG(ERROR) << "could not restore from LevelDB because a previous "
                       << "execution crashed and the database was tampered with; "
                       << "you're on your own with this one";
            return false;
        }
    }
    else if (st.IsNotFound())
    {
        if (!first_time)
        {
            LOG(ERROR) << "could not restore from LevelDB because a previous "
                       << "execution crashed; run the recovery program and try again";
            return false;
        }
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "could not restore from LevelDB because of corruption:  "
                   << st.ToString();
        return false;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "could not restore from LevelDB because of an IO error:  "
                   << st.ToString();
        return false;
    }
    else
    {
        LOG(ERROR) << "could not restore from LevelDB because it returned an "
                   << "unknown error that we don't know how to handle:  "
                   << st.ToString();
        return false;
    }

    if (first_time)
    {
        *saved = false;
        return true;
    }

    // XXX
    LOG(ERROR) << "UNIMPLMENTED RECOVERY CASE";
    abort();
#if 0

    *saved = true;
    // XXX inefficient, lazy hack
    std::auto_ptr<e::buffer> buf(e::buffer::create(sbacking.size()));
    memmove(buf->data(), sbacking.data(), sbacking.size());
    buf->resize(sbacking.size());
    e::buffer::unpacker up = buf->unpack_from(0);
    up = up >> *saved_us >> *saved_config_manager;

    if (up.error())
    {
        LOG(ERROR) << "could not restore from LevelDB because a previous "
                   << "execution saved invalid state; run the recovery program and try again";
        return false;
    }

    return true;
#endif
}

void
datalayer :: teardown()
{
    if (m_db)
    {
        delete m_db;
        m_db = NULL;
    }
}

reconfigure_returncode
datalayer :: prepare(const configuration&,
                     const configuration&,
                     const server_id&)
{
    // XXX
    return RECONFIGURE_SUCCESS;
}

reconfigure_returncode
datalayer :: reconfigure(const configuration&,
                         const configuration&,
                         const server_id&)
{
    // XXX
    return RECONFIGURE_SUCCESS;
}

reconfigure_returncode
datalayer :: cleanup(const configuration&,
                     const configuration&,
                     const server_id&)
{
    // XXX
    return RECONFIGURE_SUCCESS;
}

datalayer::returncode
datalayer :: get(const region_id& ri,
                 const e::slice& key,
                 std::vector<e::slice>* value,
                 uint64_t* version,
                 reference* ref)
{
    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    std::vector<char> kbacking;
    leveldb::Slice lkey;
    encode_key(ri, key, &kbacking, &lkey);
    leveldb::Status st = m_db->Get(opts, lkey, &ref->m_backing);

    if (st.ok())
    {
        e::slice v(ref->m_backing.data(), ref->m_backing.size());
        return decode_value(v, value, version);
    }
    else if (st.IsNotFound())
    {
        return NOT_FOUND;
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption at the disk layer: region=" << ri
                   << " key=0x" << key.hex() << " desc=" << st.ToString();
        return CORRUPTION;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error at the disk layer: region=" << ri
                   << " key=0x" << key.hex() << " desc=" << st.ToString();
        return IO_ERROR;
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
        return LEVELDB_ERROR;
    }
}

datalayer::returncode
datalayer :: put(const region_id& ri,
                 const e::slice& key,
                 const std::vector<e::slice>& value,
                 uint64_t version)
{
    const schema* sc = m_daemon->m_config.get_schema(ri);
    assert(sc);
    leveldb::WriteOptions opts;
    opts.sync = true;
    std::vector<char> kbacking;
    leveldb::Slice lkey;
    encode_key(ri, key, &kbacking, &lkey);
    std::vector<char> vbacking;
    leveldb::Slice lvalue;
    encode_value(value, version, &vbacking, &lvalue);
    backing_t backing;
    leveldb::WriteBatch updates;
    updates.Put(lkey, lvalue);
    returncode rc = create_index_changes(sc, ri, key, lkey, value, &backing, &updates);

    if (rc != SUCCESS)
    {
        return rc;
    }

    leveldb::Status st = m_db->Write(opts, &updates);

    if (st.ok())
    {
        return SUCCESS;
    }
    else if (st.IsNotFound())
    {
        return NOT_FOUND;
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption at the disk layer: region=" << ri
                   << " key=0x" << key.hex() << " desc=" << st.ToString();
        return CORRUPTION;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error at the disk layer: region=" << ri
                   << " key=0x" << key.hex() << " desc=" << st.ToString();
        return IO_ERROR;
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
        return LEVELDB_ERROR;
    }
}

datalayer::returncode
datalayer :: del(const region_id& ri,
                 const e::slice& key)
{
    const schema* sc = m_daemon->m_config.get_schema(ri);
    assert(sc);
    leveldb::WriteOptions opts;
    opts.sync = true;
    std::vector<char> kbacking;
    leveldb::Slice lkey;
    encode_key(ri, key, &kbacking, &lkey);
    backing_t backing;
    leveldb::WriteBatch updates;
    updates.Delete(lkey);
    returncode rc = create_index_changes(sc, ri, key, lkey, &backing, &updates);

    if (rc != SUCCESS)
    {
        return rc;
    }

    leveldb::Status st = m_db->Write(opts, &updates);

    if (st.ok())
    {
        return SUCCESS;
    }
    else if (st.IsNotFound())
    {
        return NOT_FOUND;
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption at the disk layer: region=" << ri
                   << " key=0x" << key.hex() << " desc=" << st.ToString();
        return CORRUPTION;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error at the disk layer: region=" << ri
                   << " key=0x" << key.hex() << " desc=" << st.ToString();
        return IO_ERROR;
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
        return LEVELDB_ERROR;
    }
}

void
datalayer :: encode_key(const region_id& ri,
                        const e::slice& key,
                        std::vector<char>* kbacking,
                        leveldb::Slice* lkey)
{
    size_t sz = sizeof(uint8_t) + sizeof(uint64_t) + key.size();
    kbacking->resize(sz);
    char* ptr = &kbacking->front();
    ptr = e::pack8be('o', ptr);
    ptr = e::pack64be(ri.get(), ptr);
    memmove(ptr, key.data(), key.size());
    *lkey = leveldb::Slice(&kbacking->front(), sz);
}

datalayer::returncode
datalayer :: decode_key(const e::slice& lkey,
                        region_id* ri,
                        e::slice* key)
{
    const uint8_t* ptr = lkey.data();
    const uint8_t* end = ptr + lkey.size();

    if (ptr >= end || *ptr != 'o')
    {
        return BAD_ENCODING;
    }

    ++ptr;
    uint64_t rid;

    if (ptr + sizeof(uint64_t) <= end)
    {
        ptr = e::unpack64be(ptr, &rid);
    }
    else
    {
        return BAD_ENCODING;
    }

    *ri  = region_id(rid);
    *key = e::slice(ptr, end - ptr);
    return SUCCESS;
}

void
datalayer :: encode_value(const std::vector<e::slice>& attrs,
                          uint64_t version,
                          std::vector<char>* backing,
                          leveldb::Slice* lvalue)
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

    *lvalue = leveldb::Slice(&backing->front(), sz);
}

datalayer::returncode
datalayer :: decode_value(const e::slice& value,
                          std::vector<e::slice>* attrs,
                          uint64_t* version)
{
    const uint8_t* ptr = value.data();
    const uint8_t* end = ptr + value.size();

    if (ptr + sizeof(uint64_t) <= end)
    {
        ptr = e::unpack64be(ptr, version);
    }
    else
    {
        return BAD_ENCODING;
    }

    uint16_t num_attrs;

    if (ptr + sizeof(uint64_t) <= end)
    {
        ptr = e::unpack16be(ptr, &num_attrs);
    }
    else
    {
        return BAD_ENCODING;
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
            return BAD_ENCODING;
        }

        e::slice s(reinterpret_cast<const uint8_t*>(ptr), sz);
        ptr += sz;
        attrs->push_back(s);
    }

    return SUCCESS;
}

void
datalayer :: generate_object_range(const region_id& ri,
                                   backing_t* backing,
                                   leveldb::Range* r)
{
    backing->push_back(std::vector<char>(sizeof(uint8_t) + sizeof(uint64_t)));
    char* ptr = &backing->back().front();
    ptr = e::pack8be('o', ptr);
    ptr = e::pack64be(ri.get(), ptr);
    leveldb::Slice start(&backing->back().front(), ptr - &backing->back().front());
    backing->push_back(backing->back());
    ptr = &backing->back().front();
    char* end = &backing->back().front() + start.size();
    index_encode_bump(ptr, end);
    leveldb::Slice limit(ptr, end - ptr);
    *r = leveldb::Range(start, limit);
}

void
datalayer :: generate_index(const region_id& ri,
                            uint16_t attr,
                            hyperdatatype type,
                            const e::slice& value,
                            const e::slice& key,
                            backing_t* backing,
                            std::vector<leveldb::Slice>* idxs)
{
    char* ptr = NULL;
    int64_t tmp_i;
    double tmp_d;

    switch (type)
    {
        case HYPERDATATYPE_GENERIC:
            break;
        case HYPERDATATYPE_STRING:
            backing->push_back(std::vector<char>());
            backing->back().resize(sizeof(uint8_t)
                                   + sizeof(uint64_t)
                                   + sizeof(uint16_t)
                                   + value.size()
                                   + key.size()
                                   + sizeof(uint32_t));
            ptr = &backing->back().front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            memmove(ptr, value.data(), value.size());
            ptr += value.size();
            memmove(ptr, key.data(), key.size());
            ptr += key.size();
            ptr = e::pack32be(key.size(), ptr);
            idxs->push_back(leveldb::Slice(&backing->back().front(), backing->back().size()));
            break;
        case HYPERDATATYPE_INT64:
            backing->push_back(std::vector<char>());
            backing->back().resize(sizeof(uint8_t)
                                   + sizeof(uint64_t)
                                   + sizeof(uint16_t)
                                   + sizeof(uint64_t)
                                   + key.size());
            ptr = &backing->back().front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            e::unpack64le(value.data(), &tmp_i);
            ptr = index_encode_int64(tmp_i, ptr);
            memmove(ptr, key.data(), key.size());
            idxs->push_back(leveldb::Slice(&backing->back().front(), backing->back().size()));
            break;
        case HYPERDATATYPE_FLOAT:
            backing->push_back(std::vector<char>());
            backing->back().resize(sizeof(uint8_t)
                                   + sizeof(uint64_t)
                                   + sizeof(uint16_t)
                                   + sizeof(double)
                                   + key.size());
            ptr = &backing->back().front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            e::unpackdoublele(value.data(), &tmp_d);
            ptr = index_encode_double(tmp_d, ptr);
            memmove(ptr, key.data(), key.size());
            idxs->push_back(leveldb::Slice(&backing->back().front(), backing->back().size()));
            break;
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
            break;
    }
}

void
datalayer :: generate_search_filters(const region_id& ri,
                                     attribute_check* check_ptr,
                                     attribute_check* check_end,
                                     backing_t* backing,
                                     std::vector<search_filter>* sf)
{
    e::slice first("\x00", 1);
    e::slice last("\xff", 1);
    bool init_type = false;
    uint16_t attr = UINT16_MAX;
    hyperdatatype type = HYPERDATATYPE_GARBAGE;

    while (check_ptr < check_end)
    {
        if (init_type && (check_ptr->attr != attr ||
                          check_ptr->datatype != type))
        {
            first = e::slice("", 0);
            last = e::slice("", 0);
            ++check_ptr;
            continue;
        }

        switch (check_ptr->predicate)
        {
            case HYPERPREDICATE_EQUALS:
                if (check_ptr->value < first ||
                    check_ptr->value > last)
                {
                    first = e::slice("", 0);
                    last = e::slice("", 0);
                }
                else
                {
                    first = check_ptr->value;
                    last = check_ptr->value;
                }
                break;
            case HYPERPREDICATE_LESS_EQUAL:
                if (check_ptr->value < first)
                {
                    first = e::slice("", 0);
                    last = e::slice("", 0);
                }
                else if (check_ptr->value < last)
                {
                    last = check_ptr->value;
                }
                break;
            case HYPERPREDICATE_GREATER_EQUAL:
                if (check_ptr->value > last)
                {
                    first = e::slice("", 0);
                    last = e::slice("", 0);
                }
                else if (check_ptr->value > first)
                {
                    first = check_ptr->value;
                }
                break;
            case HYPERPREDICATE_FAIL:
            default:
                sf->push_back(search_filter());
                sf->back().range = leveldb::Range(leveldb::Slice("", 0), leveldb::Slice("", 0));
                sf->back().parse = parse_index_empty;
                break;
        }

        init_type = true;
        attr = check_ptr->attr;
        type = check_ptr->datatype;
        ++check_ptr;
    }

    if (!init_type)
    {
        return;
    }

    char* ptr = NULL;
    sf->push_back(search_filter());
    int64_t tmp_i;
    double tmp_d;
    leveldb::Slice start;
    leveldb::Slice limit;

    switch (type)
    {
        case HYPERDATATYPE_STRING:
            backing->push_back(std::vector<char>());
            backing->back().resize(sizeof(uint8_t)
                                   + sizeof(uint64_t)
                                   + sizeof(uint16_t)
                                   + first.size());
            ptr = &backing->back().front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            memmove(ptr, first.data(), first.size());
            ptr += first.size();
            start = leveldb::Slice(&backing->back().front(), backing->back().size());
            backing->push_back(std::vector<char>());
            backing->back().resize(sizeof(uint8_t)
                                   + sizeof(uint64_t)
                                   + sizeof(uint16_t)
                                   + last.size());
            ptr = &backing->back().front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            memmove(ptr, last.data(), last.size());
            ptr += last.size();
            limit = leveldb::Slice(&backing->back().front(), backing->back().size());
            index_encode_bump(&backing->back().front(), &backing->back().front() + backing->back().size());
            sf->back().range = leveldb::Range(start, limit);
            sf->back().parse = parse_index_string;
            break;
        case HYPERDATATYPE_INT64:
            backing->push_back(std::vector<char>());
            backing->back().resize(sizeof(uint8_t)
                                   + sizeof(uint64_t)
                                   + sizeof(uint16_t)
                                   + sizeof(uint64_t));
            ptr = &backing->back().front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            e::unpack64le(first.data(), &tmp_i);
            ptr = index_encode_int64(tmp_i, ptr);
            start = leveldb::Slice(&backing->back().front(), backing->back().size());
            backing->push_back(std::vector<char>());
            backing->back().resize(sizeof(uint8_t)
                                   + sizeof(uint64_t)
                                   + sizeof(uint16_t)
                                   + sizeof(uint64_t));
            ptr = &backing->back().front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            e::unpack64le(last.data(), &tmp_i);
            ptr = index_encode_int64(tmp_i, ptr);
            limit = leveldb::Slice(&backing->back().front(), backing->back().size());
            index_encode_bump(&backing->back().front(), &backing->back().front() + backing->back().size());
            sf->back().range = leveldb::Range(start, limit);
            sf->back().parse = parse_index_sizeof8;
            break;
        case HYPERDATATYPE_FLOAT:
            backing->push_back(std::vector<char>());
            backing->back().resize(sizeof(uint8_t)
                                   + sizeof(uint64_t)
                                   + sizeof(uint16_t)
                                   + sizeof(double));
            ptr = &backing->back().front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            e::unpackdoublele(first.data(), &tmp_d);
            ptr = index_encode_double(tmp_d, ptr);
            start = leveldb::Slice(&backing->back().front(), backing->back().size());
            backing->push_back(std::vector<char>());
            backing->back().resize(sizeof(uint8_t)
                                   + sizeof(uint64_t)
                                   + sizeof(uint16_t)
                                   + sizeof(double));
            ptr = &backing->back().front();
            ptr = e::pack8be('i', ptr);
            ptr = e::pack64be(ri.get(), ptr);
            ptr = e::pack16be(attr, ptr);
            e::unpackdoublele(last.data(), &tmp_d);
            ptr = index_encode_double(tmp_d, ptr);
            limit = leveldb::Slice(&backing->back().front(), backing->back().size());
            index_encode_bump(&backing->back().front(), &backing->back().front() + backing->back().size());
            sf->back().range = leveldb::Range(start, limit);
            sf->back().parse = parse_index_sizeof8;
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
            break;
    }
}

datalayer::returncode
datalayer :: create_index_changes(const schema* sc,
                                  const region_id& ri,
                                  const e::slice& key,
                                  const leveldb::Slice& lkey,
                                  backing_t* backing,
                                  leveldb::WriteBatch* updates)
{
    std::string ref;
    leveldb::ReadOptions opts;
    opts.fill_cache = false;
    opts.verify_checksums = true;
    leveldb::Status st = m_db->Get(opts, lkey, &ref);
    std::vector<leveldb::Slice> old_idxs;

    if (st.ok())
    {
        std::vector<e::slice> old_value;
        uint64_t old_version;
        returncode rc = decode_value(e::slice(ref.data(), ref.size()),
                                     &old_value, &old_version);

        if (rc != SUCCESS)
        {
            return rc;
        }

        if (old_value.size() + 1 != sc->attrs_sz)
        {
            return BAD_ENCODING;
        }

        for (size_t i = 0; i + 1 < sc->attrs_sz; ++i)
        {
            generate_index(ri, i + 1, sc->attrs[i + 1].type, old_value[i], key, backing, &old_idxs);
        }
    }
    else if (st.IsNotFound())
    {
        // Nothing (no indices to remove)
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption at the disk layer: region=" << ri
                   << " key=0x" << key.hex() << " desc=" << st.ToString();
        return CORRUPTION;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error at the disk layer: region=" << ri
                   << " key=0x" << key.hex() << " desc=" << st.ToString();
        return IO_ERROR;
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
        return LEVELDB_ERROR;
    }

    for (size_t i = 0; i < old_idxs.size(); ++i)
    {
        if (!old_idxs.empty())
        {
            updates->Delete(old_idxs[i]);
        }
    }

    return SUCCESS;
}

static bool
sort_idxs(const leveldb::Slice& lhs, const leveldb::Slice& rhs)
{
    return lhs.compare(rhs) < 0;
}

datalayer::returncode
datalayer :: create_index_changes(const schema* sc,
                                  const region_id& ri,
                                  const e::slice& key,
                                  const leveldb::Slice& lkey,
                                  const std::vector<e::slice>& new_value,
                                  backing_t* backing,
                                  leveldb::WriteBatch* updates)
{
    std::string ref;
    leveldb::ReadOptions opts;
    opts.fill_cache = false;
    opts.verify_checksums = true;
    leveldb::Status st = m_db->Get(opts, lkey, &ref);
    std::vector<leveldb::Slice> old_idxs;

    if (st.ok())
    {
        std::vector<e::slice> old_value;
        uint64_t old_version;
        returncode rc = decode_value(e::slice(ref.data(), ref.size()),
                                     &old_value, &old_version);

        if (rc != SUCCESS)
        {
            return rc;
        }

        if (old_value.size() + 1 != sc->attrs_sz)
        {
            return BAD_ENCODING;
        }

        for (size_t i = 0; i + 1 < sc->attrs_sz; ++i)
        {
            generate_index(ri, i + 1, sc->attrs[i + 1].type, old_value[i], key, backing, &old_idxs);
        }
    }
    else if (st.IsNotFound())
    {
        // Nothing (no indices to remove)
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption at the disk layer: region=" << ri
                   << " key=0x" << key.hex() << " desc=" << st.ToString();
        return CORRUPTION;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error at the disk layer: region=" << ri
                   << " key=0x" << key.hex() << " desc=" << st.ToString();
        return IO_ERROR;
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
        return LEVELDB_ERROR;
    }

    std::vector<leveldb::Slice> new_idxs;

    for (size_t i = 0; i + 1 < sc->attrs_sz; ++i)
    {
        generate_index(ri, i + 1, sc->attrs[i + 1].type, new_value[i], key, backing, &new_idxs);
    }

    std::sort(old_idxs.begin(), old_idxs.end(), sort_idxs);
    std::sort(new_idxs.begin(), new_idxs.end(), sort_idxs);

    size_t old_i = 0;
    size_t new_i = 0;
    leveldb::Slice empty("", 0);

    while (old_i < old_idxs.size() && new_i < new_idxs.size())
    {
        int cmp = old_idxs[old_i].compare(new_idxs[new_i]);

        // Erase common indexes because we need not insert or remove them
        if (cmp == 0)
        {
            ++old_i;
            ++new_i;
        }
        if (cmp < 0)
        {
            updates->Delete(old_idxs[old_i]);
            ++old_i;
        }
        if (cmp > 0)
        {
            updates->Put(new_idxs[new_i], empty);
            ++new_i;
        }
    }

    while (old_i < old_idxs.size())
    {
        updates->Delete(old_idxs[old_i]);
        ++old_i;
    }

    while (new_i < new_idxs.size())
    {
        updates->Put(new_idxs[new_i], empty);
        ++new_i;
    }

    return SUCCESS;
}

datalayer::returncode
datalayer :: make_snapshot(const region_id& ri,
                           attribute_check* checks, size_t checks_sz,
                           snapshot* snap)
{
    std::stable_sort(checks, checks + checks_sz);
    attribute_check* check_ptr = checks;
    attribute_check* check_end = checks + checks_sz;

    while (check_ptr < check_end)
    {
        attribute_check* check_tmp = check_ptr;

        while (check_tmp < check_end && check_tmp->attr == check_ptr->attr)
        {
            ++check_tmp;
        }

        generate_search_filters(ri, check_ptr, check_tmp, &snap->m_backing, &snap->m_sfs);
        check_ptr = check_tmp;
    }

    std::vector<leveldb::Range> ranges;
    ranges.reserve(snap->m_sfs.size() + 1);
    std::vector<uint64_t> sizes(snap->m_sfs.size() + 1, 0);

    for (size_t i = 0; i < snap->m_sfs.size(); ++i)
    {
        ranges.push_back(snap->m_sfs[i].range);
    }

    ranges.push_back(leveldb::Range());
    generate_object_range(ri, &snap->m_backing, &ranges[snap->m_sfs.size()]);
    m_db->GetApproximateSizes(&ranges.front(), ranges.size(), &sizes.front());
    size_t sum = 0;

    for (size_t i = 0; i < snap->m_sfs.size(); ++i)
    {
        sum += sizes[i];
        snap->m_sfs[i].size = sizes[i];
    }

    std::sort(snap->m_sfs.begin(), snap->m_sfs.end(), search_filter::sort_by_size);

    while (!snap->m_sfs.empty() && sum > sizes[snap->m_sfs.size()] / 10)
    {
        sum -= snap->m_sfs.back().size;
        snap->m_sfs.pop_back();
    }

    snap->m_dl = this;
    snap->m_snap = m_db->GetSnapshot();
    snap->m_checks = checks;
    snap->m_checks_sz = checks_sz;
    leveldb::ReadOptions opts;
    opts.fill_cache = false;
    opts.verify_checksums = true;
    opts.snapshot = snap->m_snap;
    snap->m_ri = ri;
    snap->m_obj_range = ranges.back();
    snap->m_iter = m_db->NewIterator(opts);
    snap->m_iter->Seek(snap->m_obj_range.start);

    snap->m_iter = m_db->NewIterator(opts);
    snap->m_iter->Seek(snap->m_obj_range.start);

    for (size_t i = 0; i < snap->m_sfs.size(); ++i)
    {
        snap->m_sfs[i].iter = m_db->NewIterator(opts);
        snap->m_sfs[i].iter->Seek(snap->m_sfs[i].range.start);
    }

    return SUCCESS;
}

datalayer :: reference :: reference()
    : m_backing()
{
}

datalayer :: reference :: ~reference() throw ()
{
}

void
datalayer :: reference :: swap(reference* ref)
{
    m_backing.swap(ref->m_backing);
}

std::ostream&
hyperdex :: operator << (std::ostream& lhs, datalayer::returncode rhs)
{
    switch (rhs)
    {
        STRINGIFY(datalayer::SUCCESS);
        STRINGIFY(datalayer::NOT_FOUND);
        STRINGIFY(datalayer::BAD_ENCODING);
        STRINGIFY(datalayer::CORRUPTION);
        STRINGIFY(datalayer::IO_ERROR);
        STRINGIFY(datalayer::LEVELDB_ERROR);
        default:
            lhs << "unknown returncode";
    }

    return lhs;
}

datalayer :: snapshot :: snapshot()
    : m_dl(NULL)
    , m_snap(NULL)
    , m_backing()
    , m_sfs()
    , m_checks()
    , m_checks_sz()
    , m_ri()
    , m_obj_range()
    , m_iter(NULL)
    , m_error(SUCCESS)
    , m_key()
    , m_value()
    , m_version()
{
}

datalayer :: snapshot :: ~snapshot() throw ()
{
    if (m_iter)
    {
        delete m_iter;
    }

    for (size_t i = 0; i < m_sfs.size(); ++i)
    {
        if (m_sfs[i].iter)
        {
            delete m_sfs[i].iter;
        }
    }

    if (m_dl && m_dl->m_db && m_snap)
    {
        m_dl->m_db->ReleaseSnapshot(m_snap);
    }
}

bool
datalayer :: snapshot :: has_next()
{
    if (m_error != SUCCESS)
    {
        return false;
    }

    // Don't try to optimize by replacing m_ri with a const schema* because it
    // won't persist across reconfigurations
    const schema* sc = m_dl->m_daemon->m_config.get_schema(m_ri);
    assert(sc);

    while (m_iter->Valid())
    {
        if (!m_sfs.empty())
        {
            bool retry_whole = false;

            for (size_t i = 0; !retry_whole && i < m_sfs.size(); ++i)
            {
                bool retry_this = false;

                do
                {
                    retry_this = false;

                    if (!m_sfs[i].iter->Valid())
                    {
                        return false;
                    }

                    if (m_sfs[i].iter->key().compare(m_sfs[i].range.limit) >= 0)
                    {
                        return false;
                    }

                    if (i > 0)
                    {
                        e::slice outer_it;
                        e::slice inner_it;
                        m_sfs[i - 1].parse(m_sfs[i - 1].iter->key(), &outer_it);
                        m_sfs[i - 0].parse(m_sfs[i - 0].iter->key(), &inner_it);

                        if (outer_it < inner_it)
                        {
                            // 0->1->...->i-1 are all the same key.  Bump one, bump
                            // all.  The first is the most selective
                            m_sfs[0].iter->Next();
                            retry_whole = true;
                            break;
                        }
                        if (inner_it < outer_it)
                        {
                            m_sfs[i].iter->Next();
                            retry_this = true;
                            break;
                        }
                    }
                }
                while (retry_this);
            }

            if (retry_whole)
            {
                continue;
            }

            // At this point we are guaranteed that m_sfs's iterators are all
            // pointing to an index entry for the same key.  We need to seek
            // m_iter to that key.

            e::slice tmp_ekey;
            leveldb::Slice tmp_lkey;
            std::vector<char> tmp_backing;
            m_sfs[0].parse(m_sfs[0].iter->key(), &tmp_ekey);
            m_dl->encode_key(m_ri, tmp_ekey, &tmp_backing, &tmp_lkey);
            m_iter->Seek(tmp_lkey);

            if (m_iter->key().compare(tmp_lkey) != 0)
            {
                m_sfs[0].iter->Next();
                continue;
            }
        }

        leveldb::Slice lkey = m_iter->key();
        leveldb::Slice lval = m_iter->value();
        e::slice ekey(reinterpret_cast<const uint8_t*>(lkey.data()), lkey.size());
        e::slice eval(reinterpret_cast<const uint8_t*>(lval.data()), lval.size());

        if (m_obj_range.limit.compare(lkey) <= 0)
        {
            return false;
        }

        if (!ekey.size() || ekey.data()[0] != 'o')
        {
            if (m_sfs.empty())
            {
                m_iter->Next();
            }
            else
            {
                m_sfs[0].iter->Next();
            }

            continue;
        }

        region_id ri;
        returncode rc;
        rc = m_dl->decode_key(ekey, &ri, &m_key);

        if (ri != m_ri)
        {
            if (m_sfs.empty())
            {
                m_iter->Next();
            }
            else
            {
                m_sfs[0].iter->Next();
            }

            continue;
        }

        switch (rc)
        {
            case SUCCESS:
                break;
            case BAD_ENCODING:
            case NOT_FOUND:
            case CORRUPTION:
            case IO_ERROR:
            case LEVELDB_ERROR:
            default:
                m_error = BAD_ENCODING;
                return false;
        }

        rc = m_dl->decode_value(eval, &m_value, &m_version);

        switch (rc)
        {
            case SUCCESS:
                break;
            case BAD_ENCODING:
            case NOT_FOUND:
            case CORRUPTION:
            case IO_ERROR:
            case LEVELDB_ERROR:
            default:
                m_error = BAD_ENCODING;
                return false;
        }

        bool passes_checks = true;

        for (size_t i = 0; passes_checks && i < m_checks_sz; ++i)
        {
            if (m_checks[i].attr >= sc->attrs_sz)
            {
                passes_checks = false;
            }
            else if (m_checks[i].attr == 0)
            {
                microerror e;
                passes_checks = passes_attribute_check(sc->attrs[0].type, m_checks[i], m_key, &e);
            }
            else
            {
                hyperdatatype type = sc->attrs[m_checks[i].attr].type;
                microerror e;
                passes_checks = passes_attribute_check(type, m_checks[i], m_value[m_checks[i].attr - 1], &e);
            }
        }

        if (!passes_checks)
        {
            if (m_sfs.empty())
            {
                m_iter->Next();
            }
            else
            {
                m_sfs[0].iter->Next();
            }

            continue;
        }

        m_error = SUCCESS;
        return true;
    }

    return false;
}

void
datalayer :: snapshot :: next()
{
    m_error = SUCCESS;

    if (m_sfs.empty())
    {
        m_iter->Next();
    }
    else
    {
        m_sfs[0].iter->Next();
    }
}

void
datalayer :: snapshot :: get(e::slice* key, std::vector<e::slice>* val, uint64_t* ver)
{
    *key = m_key;
    *val = m_value;
    *ver = m_version;
}

void
datalayer :: snapshot :: get(e::slice* key, std::vector<e::slice>* val, uint64_t* ver, reference* ref)
{
    ref->m_backing = std::string();
    ref->m_backing += std::string(reinterpret_cast<const char*>(m_key.data()), m_key.size());

    for (size_t i = 0; i < m_value.size(); ++i)
    {
        ref->m_backing += std::string(reinterpret_cast<const char*>(m_value[i].data()), m_value[i].size());
    }

    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(ref->m_backing.data());
    *key = e::slice(ptr, m_key.size());
    ptr += m_key.size();
    val->resize(m_value.size());

    for (size_t i = 0; i < m_value.size(); ++i)
    {
        (*val)[i] = e::slice(ptr, m_value[i].size());
        ptr += m_value[i].size();
    }

    *ver = m_version;
}
