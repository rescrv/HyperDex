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

// STL
#include <string>

// Google Log
#include <glog/logging.h>

// LevelDB
#include <leveldb/filter_policy.h>

// e
#include <e/endian.h>

// HyperDex
#include "common/macros.h"
#include "daemon/daemon.h"
#include "daemon/datalayer.h"
#include "datatypes/apply.h"
#include "datatypes/microerror.h"

using hyperdex::datalayer;
using hyperdex::reconfigure_returncode;

datalayer :: datalayer(daemon* d)
    : m_daemon(d)
    , m_db(NULL)
{
}

datalayer :: ~datalayer() throw ()
{
    close();
}

reconfigure_returncode
datalayer :: open(const po6::pathname& path)
{
    leveldb::Options opts;
    opts.create_if_missing = true;
    opts.write_buffer_size = 16777216;
    opts.compression = leveldb::kSnappyCompression;
    opts.filter_policy = leveldb::NewBloomFilterPolicy(10);
    std::string name(path.get());
    leveldb::Status st = leveldb::DB::Open(opts, name, &m_db);

    if (!st.ok())
    {
        LOG(ERROR) << "could not open LevelDB: " << st.ToString();
        return RECONFIGURE_FATAL;
    }

    return RECONFIGURE_SUCCESS;
}

reconfigure_returncode
datalayer :: close()
{
    if (m_db)
    {
        delete m_db;
        m_db = NULL;
    }

    return RECONFIGURE_SUCCESS;
}

reconfigure_returncode
datalayer :: prepare(const configuration&,
                     const configuration&,
                     const instance&)
{
    // XXX
    return RECONFIGURE_SUCCESS;
}

reconfigure_returncode
datalayer :: reconfigure(const configuration&,
                         const configuration&,
                         const instance&)
{
    // XXX
    return RECONFIGURE_SUCCESS;
}

reconfigure_returncode
datalayer :: cleanup(const configuration&,
                     const configuration&,
                     const instance&)
{
    // XXX
    return RECONFIGURE_SUCCESS;
}

datalayer::returncode
datalayer :: get(const regionid& ri,
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
datalayer :: put(const regionid& ri,
                 const e::slice& key,
                 const std::vector<e::slice>& value,
                 uint64_t version)
{
    leveldb::WriteOptions opts;
    opts.sync = false;
    std::vector<char> kbacking;
    leveldb::Slice lkey;
    encode_key(ri, key, &kbacking, &lkey);
    std::vector<char> vbacking;
    leveldb::Slice lvalue;
    encode_value(value, version, &vbacking, &lvalue);
    leveldb::Status st = m_db->Put(opts, lkey, lvalue);

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
datalayer :: del(const regionid& ri,
                 const e::slice& key)
{
    leveldb::WriteOptions opts;
    opts.sync = false;
    std::vector<char> kbacking;
    leveldb::Slice lkey;
    encode_key(ri, key, &kbacking, &lkey);
    leveldb::Status st = m_db->Delete(opts, lkey);

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
datalayer :: encode_key(const regionid& ri,
                        const e::slice& key,
                        std::vector<char>* kbacking,
                        leveldb::Slice* lkey)
{
    size_t sz = sizeof(uint8_t)
              + sizeof(uint64_t)
              + sizeof(uint32_t)
              + sizeof(uint16_t)
              + sizeof(uint8_t)
              + key.size();
    kbacking->resize(sz);
    char* ptr = &kbacking->front();
    ptr = e::pack8be('o', ptr);
    ptr = e::pack64be(ri.mask, ptr);
    ptr = e::pack32be(ri.space, ptr);
    ptr = e::pack16be(ri.subspace, ptr);
    ptr = e::pack8be(ri.prefix, ptr);
    memmove(ptr, key.data(), key.size());
    *lkey = leveldb::Slice(&kbacking->front(), sz);
}

datalayer::returncode
datalayer :: decode_key(const e::slice& lkey,
                        regionid* ri,
                        e::slice* key)
{
    const uint8_t* ptr = lkey.data();
    const uint8_t* end = ptr + lkey.size();

    if (ptr + sizeof(uint8_t) <= end)
    {
        uint8_t o;
        ptr = e::unpack8be(ptr, &o);

        if (o != 'o')
        {
            return BAD_ENCODING;
        }
    }
    else
    {
        return BAD_ENCODING;
    }

    if (ptr + sizeof(uint64_t) <= end)
    {
        ptr = e::unpack64be(ptr, &ri->mask);
    }
    else
    {
        return BAD_ENCODING;
    }

    if (ptr + sizeof(uint32_t) <= end)
    {
        ptr = e::unpack32be(ptr, &ri->space);
    }
    else
    {
        return BAD_ENCODING;
    }

    if (ptr + sizeof(uint16_t) <= end)
    {
        ptr = e::unpack16be(ptr, &ri->subspace);
    }
    else
    {
        return BAD_ENCODING;
    }

    if (ptr + sizeof(uint8_t) <= end)
    {
        ptr = e::unpack8be(ptr, &ri->prefix);
    }
    else
    {
        return BAD_ENCODING;
    }

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

datalayer::returncode
datalayer :: make_snapshot(const regionid& ri,
                           const attribute_check* checks, size_t checks_sz,
                           snapshot* snap)
{
    snap->m_dl = this;
    snap->m_snap = m_db->GetSnapshot();
    leveldb::ReadOptions opts;
    opts.fill_cache = false;
    opts.verify_checksums = true;
    opts.snapshot = snap->m_snap;
    snap->m_iter = m_db->NewIterator(opts);
    snap->m_iter->SeekToFirst();
    snap->m_ri = ri;
    snap->m_checks = checks;
    snap->m_checks_sz = checks_sz;
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
    , m_iter(NULL)
    , m_ri()
    , m_checks()
    , m_checks_sz()
    , m_valid(false)
    , m_error(SUCCESS)
    , m_key()
    , m_value()
    , m_version()
{
}

datalayer :: snapshot :: ~snapshot() throw ()
{
    if (m_dl && m_dl->m_db && m_snap)
    {
        m_dl->m_db->ReleaseSnapshot(m_snap);
        m_snap = NULL;
    }

    assert(m_snap = NULL);
}

bool
datalayer :: snapshot :: has_next()
{
    if (m_valid)
    {
        return true;
    }

    if (m_error != SUCCESS)
    {
        return false;
    }

    schema* sc = m_dl->m_daemon->m_config.get_schema(m_ri.get_space());
    assert(sc);

    while (m_iter->Valid())
    {
        if (!m_iter->status().ok())
        {
            if (m_iter->status().IsNotFound())
            {
                m_error = NOT_FOUND;
            }
            else if (m_iter->status().IsCorruption())
            {
                LOG(ERROR) << "corruption at the disk layer during iteration "
                           << m_iter->status().ToString();
                m_error = CORRUPTION;
            }
            else if (m_iter->status().IsIOError())
            {
                LOG(ERROR) << "IO error at the disk layer during iteration "
                           << m_iter->status().ToString();
                m_error = IO_ERROR;
            }
            else
            {
                LOG(ERROR) << "LevelDB returned an unknown error during iteration that we don't know how to handle";
                m_error = LEVELDB_ERROR;
            }

            return false;
        }

        leveldb::Slice lkey = m_iter->key();
        leveldb::Slice lval = m_iter->value();
        e::slice ekey(reinterpret_cast<const uint8_t*>(lkey.data()), lkey.size());
        e::slice eval(reinterpret_cast<const uint8_t*>(lval.data()), lval.size());

        if (!ekey.size() || ekey.data()[0] != 'o')
        {
            m_iter->Next();
            continue;
        }

        regionid ri;
        returncode rc;
        rc = m_dl->decode_key(ekey, &ri, &m_key);

        if (ri != m_ri)
        {
            m_iter->Next();
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
            m_iter->Next();
            continue;
        }

        m_error = SUCCESS;
        return true;
    }

    m_error = SUCCESS;
    return false;
}

void
datalayer :: snapshot :: next()
{
    m_valid = false;
    m_iter->Next();
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
