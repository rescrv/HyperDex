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

using hyperdex::datalayer;
using hyperdex::reconfigure_returncode;

datalayer :: datalayer(daemon*)
    : m_db(NULL)
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
datalayer :: make_snapshot(const regionid& /*XXX ri*/,
                           const predicate* /*XXX pred*/,
                           snapshot* snap)
{
    snap->m_dl = this;
    snap->m_snap = m_db->GetSnapshot();
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
        stringify(datalayer::SUCCESS);
        stringify(datalayer::NOT_FOUND);
        stringify(datalayer::BAD_ENCODING);
        stringify(datalayer::CORRUPTION);
        stringify(datalayer::IO_ERROR);
        stringify(datalayer::LEVELDB_ERROR);
        default:
            lhs << "unknown returncode";
    }

    return lhs;
}

datalayer :: snapshot :: snapshot()
    : m_dl(NULL)
    , m_snap(NULL)
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
