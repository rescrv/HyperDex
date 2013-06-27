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

// POSIX
#include <signal.h>

// STL
#include <algorithm>
#include <sstream>
#include <string>

// Google Log
#include <glog/logging.h>

// LevelDB
#include <hyperleveldb/write_batch.h>
#include <hyperleveldb/filter_policy.h>

// e
#include <e/endian.h>

// HyperDex
#include "common/datatypes.h"
#include "common/macros.h"
#include "common/range_searches.h"
#include "common/serialization.h"
#include "daemon/daemon.h"
#include "daemon/datalayer.h"
#include "daemon/datalayer_encodings.h"
#include "daemon/datalayer_iterator.h"

// ASSUME:  all keys put into leveldb have a first byte without the high bit set

using hyperdex::datalayer;
using hyperdex::reconfigure_returncode;

datalayer :: datalayer(daemon* d)
    : m_daemon(d)
    , m_db()
    , m_counters()
    , m_cleaner(std::tr1::bind(&datalayer::cleaner, this))
    , m_block_cleaner()
    , m_wakeup_cleaner(&m_block_cleaner)
    , m_wakeup_reconfigurer(&m_block_cleaner)
    , m_need_cleaning(false)
    , m_shutdown(true)
    , m_need_pause(false)
    , m_paused(false)
    , m_state_transfer_captures()
{
}

datalayer :: ~datalayer() throw ()
{
    shutdown();
}

bool
datalayer :: setup(const po6::pathname& path,
                   bool* saved,
                   server_id* saved_us,
                   po6::net::location* saved_bind_to,
                   po6::net::hostname* saved_coordinator)
{
    leveldb::Options opts;
    opts.write_buffer_size = 64ULL * 1024ULL * 1024ULL;
    opts.create_if_missing = true;
    opts.filter_policy = leveldb::NewBloomFilterPolicy(10);
    std::string name(path.get());
    leveldb::DB* tmp_db;
    leveldb::Status st = leveldb::DB::Open(opts, name, &tmp_db);

    if (!st.ok())
    {
        LOG(ERROR) << "could not open LevelDB: " << st.ToString();
        return false;
    }

    m_db.reset(tmp_db);
    leveldb::ReadOptions ropts;
    ropts.fill_cache = true;
    ropts.verify_checksums = true;

    leveldb::Slice rk("hyperdex", 8);
    std::string rbacking;
    st = m_db->Get(ropts, rk, &rbacking);
    bool first_time = false;

    if (st.ok())
    {
        first_time = false;

        if (rbacking != PACKAGE_VERSION &&
            rbacking != "1.0.rc1" &&
            rbacking != "1.0.rc2")
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

    {
        po6::threads::mutex::hold hold(&m_block_cleaner);
        m_cleaner.start();
        m_shutdown = false;
    }

    if (first_time)
    {
        *saved = false;
        return true;
    }

    uint64_t us;
    *saved = true;
    e::unpacker up(sbacking.data(), sbacking.size());
    up = up >> us >> *saved_bind_to >> *saved_coordinator;
    *saved_us = server_id(us);

    if (up.error())
    {
        LOG(ERROR) << "could not restore from LevelDB because a previous "
                   << "execution saved invalid state; run the recovery program and try again";
        return false;
    }

    return true;
}

void
datalayer :: teardown()
{
    shutdown();
}

bool
datalayer :: initialize()
{
    leveldb::WriteOptions wopts;
    wopts.sync = true;
    leveldb::Status st = m_db->Put(wopts, leveldb::Slice("hyperdex", 8),
                                   leveldb::Slice(PACKAGE_VERSION, strlen(PACKAGE_VERSION)));

    if (st.ok())
    {
        return true;
    }
    else if (st.IsNotFound())
    {
        LOG(ERROR) << "could not initialize LevelDB because Put returned NotFound:  "
                   << st.ToString();
        return false;
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "could not initialize LevelDB because of corruption:  "
                   << st.ToString();
        return false;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "could not initialize LevelDB because of an IO error:  "
                   << st.ToString();
        return false;
    }
    else
    {
        LOG(ERROR) << "could not initialize LevelDB because it returned an "
                   << "unknown error that we don't know how to handle:  "
                   << st.ToString();
        return false;
    }
}

bool
datalayer :: save_state(const server_id& us,
                        const po6::net::location& bind_to,
                        const po6::net::hostname& coordinator)
{
    leveldb::WriteOptions wopts;
    wopts.sync = true;
    leveldb::Status st = m_db->Put(wopts,
                                   leveldb::Slice("dirty", 5),
                                   leveldb::Slice("", 0));

    if (st.ok())
    {
        // Yay
    }
    else if (st.IsNotFound())
    {
        LOG(ERROR) << "could not set dirty bit: "
                   << st.ToString();
        return false;
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption at the disk layer: "
                   << "could not set dirty bit: "
                   << st.ToString();
        return false;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error at the disk layer: "
                   << "could not set dirty bit: "
                   << st.ToString();
        return false;
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't "
                   << "know how to handle: could not set dirty bit";
        return false;
    }

    size_t sz = sizeof(uint64_t)
              + pack_size(bind_to)
              + pack_size(coordinator);
    std::auto_ptr<e::buffer> state(e::buffer::create(sz));
    *state << us << bind_to << coordinator;
    st = m_db->Put(wopts, leveldb::Slice("state", 5),
                   leveldb::Slice(reinterpret_cast<const char*>(state->data()), state->size()));

    if (st.ok())
    {
        return true;
    }
    else if (st.IsNotFound())
    {
        LOG(ERROR) << "could not save state: "
                   << st.ToString();
        return false;
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption at the disk layer: "
                   << "could not save state: "
                   << st.ToString();
        return false;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error at the disk layer: "
                   << "could not save state: "
                   << st.ToString();
        return false;
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't "
                   << "know how to handle: could not save state";
        return false;
    }
}

bool
datalayer :: clear_dirty()
{
    leveldb::WriteOptions wopts;
    wopts.sync = true;
    leveldb::Slice key("dirty", 5);
    leveldb::Status st = m_db->Delete(wopts, key);

    if (st.ok() || st.IsNotFound())
    {
        return true;
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption at the disk layer: "
                   << "could not clear dirty bit: "
                   << st.ToString();
        return false;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error at the disk layer: "
                   << "could not clear dirty bit: "
                   << st.ToString();
        return false;
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't "
                   << "know how to handle: could not clear dirty bit";
        return false;
    }
}

void
datalayer :: pause()
{
    po6::threads::mutex::hold hold(&m_block_cleaner);
    assert(!m_need_pause);
    m_need_pause = true;
}

void
datalayer :: unpause()
{
    po6::threads::mutex::hold hold(&m_block_cleaner);
    assert(m_need_pause);
    m_wakeup_cleaner.broadcast();
    m_need_pause = false;
    m_need_cleaning = true;
}

void
datalayer :: reconfigure(const configuration&,
                         const configuration& new_config,
                         const server_id& us)
{
    {
        po6::threads::mutex::hold hold(&m_block_cleaner);
        assert(m_need_pause);

        while (!m_paused)
        {
            m_wakeup_reconfigurer.wait();
        }
    }

    std::vector<capture> captures;
    new_config.captures(&captures);
    std::vector<region_id> regions;
    regions.reserve(captures.size());

    for (size_t i = 0; i < captures.size(); ++i)
    {
        if (new_config.get_virtual(captures[i].rid, us) != virtual_server_id())
        {
            regions.push_back(captures[i].rid);
        }
    }

    std::sort(regions.begin(), regions.end());
    m_counters.adopt(regions);
}

bool
datalayer :: get_property(const e::slice& property,
                          std::string* value)
{
    leveldb::Slice prop(reinterpret_cast<const char*>(property.data()), property.size());
    return m_db->GetProperty(prop, value);
}

uint64_t
datalayer :: approximate_size()
{
    leveldb::Slice start("\x00", 1);
    leveldb::Slice limit("\xff", 1);
    leveldb::Range r(start, limit);
    uint64_t ret = 0;
    m_db->GetApproximateSizes(&r, 1, &ret);
    return ret;
}

datalayer::returncode
datalayer :: get(const region_id& ri,
                 const e::slice& key,
                 std::vector<e::slice>* value,
                 uint64_t* version,
                 reference* ref)
{
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<char> scratch;

    // create the encoded key
    leveldb::Slice lkey;
    encode_key(ri, sc.attrs[0].type, key, &scratch, &lkey);

    // perform the read
    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
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
    else
    {
        return handle_error(st);
    }
}

datalayer::returncode
datalayer :: del(const region_id& ri,
                 const region_id& reg_id,
                 uint64_t seq_id,
                 const e::slice& key,
                 const std::vector<e::slice>& old_value)
{
    leveldb::WriteBatch updates;
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<char> scratch;

    // create the encoded key
    leveldb::Slice lkey;
    encode_key(ri, sc.attrs[0].type, key, &scratch, &lkey);

    // delete the actual object
    updates.Delete(lkey);

    // delete the index entries
    const subspace& sub(*m_daemon->m_config.get_subspace(ri));
    create_index_changes(sc, sub, ri, key, &old_value, NULL, &updates);

    // Mark acked as part of this batch write
    if (seq_id != 0)
    {
        char abacking[ACKED_BUF_SIZE];
        seq_id = UINT64_MAX - seq_id;
        encode_acked(ri, reg_id, seq_id, abacking);
        leveldb::Slice akey(abacking, ACKED_BUF_SIZE);
        leveldb::Slice aval("", 0);
        updates.Put(akey, aval);
    }

    uint64_t count;

    // If this is a captured region, then we must log this transfer
    if (m_counters.lookup(ri, &count))
    {
        char tbacking[TRANSFER_BUF_SIZE];
        capture_id cid = m_daemon->m_config.capture_for(ri);
        assert(cid != capture_id());
        leveldb::Slice tkey(tbacking, TRANSFER_BUF_SIZE);
        leveldb::Slice tval;
        encode_transfer(cid, count, tbacking);
        encode_key_value(key, NULL, 0, &scratch, &tval);
        updates.Put(tkey, tval);
    }

    // Perform the write
    leveldb::WriteOptions opts;
    opts.sync = false;
    leveldb::Status st = m_db->Write(opts, &updates);

    if (st.ok())
    {
        return SUCCESS;
    }
    else if (st.IsNotFound())
    {
        return NOT_FOUND;
    }
    else
    {
        return handle_error(st);
    }
}

datalayer::returncode
datalayer :: put(const region_id& ri,
                 const region_id& reg_id,
                 uint64_t seq_id,
                 const e::slice& key,
                 const std::vector<e::slice>& new_value,
                 uint64_t version)
{
    leveldb::WriteBatch updates;
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<char> scratch1;
    std::vector<char> scratch2;

    // create the encoded key
    leveldb::Slice lkey;
    encode_key(ri, sc.attrs[0].type, key, &scratch1, &lkey);

    // create the encoded value
    leveldb::Slice lval;
    encode_value(new_value, version, &scratch2, &lval);

    // put the actual object
    updates.Put(lkey, lval);

    // put the index entries
    const subspace& sub(*m_daemon->m_config.get_subspace(ri));
    create_index_changes(sc, sub, ri, key, NULL, &new_value, &updates);

    // Mark acked as part of this batch write
    if (seq_id != 0)
    {
        char abacking[ACKED_BUF_SIZE];
        seq_id = UINT64_MAX - seq_id;
        encode_acked(ri, reg_id, seq_id, abacking);
        leveldb::Slice akey(abacking, ACKED_BUF_SIZE);
        leveldb::Slice aval("", 0);
        updates.Put(akey, aval);
    }

    uint64_t count;

    // If this is a captured region, then we must log this transfer
    if (m_counters.lookup(ri, &count))
    {
        char tbacking[TRANSFER_BUF_SIZE];
        capture_id cid = m_daemon->m_config.capture_for(ri);
        assert(cid != capture_id());
        leveldb::Slice tkey(tbacking, TRANSFER_BUF_SIZE);
        leveldb::Slice tval;
        encode_transfer(cid, count, tbacking);
        encode_key_value(key, &new_value, version, &scratch1, &tval);
        updates.Put(tkey, tval);
    }

    // Perform the write
    leveldb::WriteOptions opts;
    opts.sync = false;
    leveldb::Status st = m_db->Write(opts, &updates);

    if (st.ok())
    {
        return SUCCESS;
    }
    else
    {
        return handle_error(st);
    }
}

datalayer::returncode
datalayer :: overput(const region_id& ri,
                     const region_id& reg_id,
                     uint64_t seq_id,
                     const e::slice& key,
                     const std::vector<e::slice>& old_value,
                     const std::vector<e::slice>& new_value,
                     uint64_t version)
{
    leveldb::WriteBatch updates;
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<char> scratch1;
    std::vector<char> scratch2;

    // create the encoded key
    leveldb::Slice lkey;
    encode_key(ri, sc.attrs[0].type, key, &scratch1, &lkey);

    // create the encoded value
    leveldb::Slice lval;
    encode_value(new_value, version, &scratch2, &lval);

    // put the actual object
    updates.Put(lkey, lval);

    // put the index entries
    const subspace& sub(*m_daemon->m_config.get_subspace(ri));
    create_index_changes(sc, sub, ri, key, &old_value, &new_value, &updates);

    // Mark acked as part of this batch write
    if (seq_id != 0)
    {
        char abacking[ACKED_BUF_SIZE];
        seq_id = UINT64_MAX - seq_id;
        encode_acked(ri, reg_id, seq_id, abacking);
        leveldb::Slice akey(abacking, ACKED_BUF_SIZE);
        leveldb::Slice aval("", 0);
        updates.Put(akey, aval);
    }

    uint64_t count;

    // If this is a captured region, then we must log this transfer
    if (m_counters.lookup(ri, &count))
    {
        char tbacking[TRANSFER_BUF_SIZE];
        capture_id cid = m_daemon->m_config.capture_for(ri);
        assert(cid != capture_id());
        leveldb::Slice tkey(tbacking, TRANSFER_BUF_SIZE);
        leveldb::Slice tval;
        encode_transfer(cid, count, tbacking);
        encode_key_value(key, &new_value, version, &scratch1, &tval);
        updates.Put(tkey, tval);
    }

    // Perform the write
    leveldb::WriteOptions opts;
    opts.sync = false;
    leveldb::Status st = m_db->Write(opts, &updates);

    if (st.ok())
    {
        return SUCCESS;
    }
    else
    {
        return handle_error(st);
    }
}

datalayer::returncode
datalayer :: uncertain_del(const region_id& ri,
                           const e::slice& key)
{
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<char> scratch;

    // create the encoded key
    leveldb::Slice lkey;
    encode_key(ri, sc.attrs[0].type, key, &scratch, &lkey);

    // perform the read
    std::string ref;
    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    leveldb::Status st = m_db->Get(opts, lkey, &ref);

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

        if (old_value.size() + 1 != sc.attrs_sz)
        {
            return BAD_ENCODING;
        }

        return del(ri, region_id(), 0, key, old_value);
    }
    else if (st.IsNotFound())
    {
        return SUCCESS;
    }
    else
    {
        return handle_error(st);
    }
}

datalayer::returncode
datalayer :: uncertain_put(const region_id& ri,
                           const e::slice& key,
                           const std::vector<e::slice>& new_value,
                           uint64_t version)
{
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<char> scratch;

    // create the encoded key
    leveldb::Slice lkey;
    encode_key(ri, sc.attrs[0].type, key, &scratch, &lkey);

    // perform the read
    std::string ref;
    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    leveldb::Status st = m_db->Get(opts, lkey, &ref);

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

        if (old_value.size() + 1 != sc.attrs_sz)
        {
            return BAD_ENCODING;
        }

        return overput(ri, region_id(), 0, key, old_value, new_value, version);
    }
    else if (st.IsNotFound())
    {
        return put(ri, region_id(), 0, key, new_value, version);
    }
    else
    {
        return handle_error(st);
    }
}

datalayer::returncode
datalayer :: get_transfer(const region_id& ri,
                          uint64_t seq_no,
                          bool* has_value,
                          e::slice* key,
                          std::vector<e::slice>* value,
                          uint64_t* version,
                          reference* ref)
{
    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    char tbacking[TRANSFER_BUF_SIZE];
    capture_id cid = m_daemon->m_config.capture_for(ri);
    assert(cid != capture_id());
    leveldb::Slice lkey(tbacking, TRANSFER_BUF_SIZE);
    encode_transfer(cid, seq_no, tbacking);
    leveldb::Status st = m_db->Get(opts, lkey, &ref->m_backing);

    if (st.ok())
    {
        e::slice v(ref->m_backing.data(), ref->m_backing.size());
        return decode_key_value(v, has_value, key, value, version);
    }
    else if (st.IsNotFound())
    {
        return NOT_FOUND;
    }
    else
    {
        return handle_error(st);
    }
}

bool
datalayer :: check_acked(const region_id& ri,
                         const region_id& reg_id,
                         uint64_t seq_id)
{
    // make it so that increasing seq_ids are ordered in reverse in the KVS
    seq_id = UINT64_MAX - seq_id;
    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    char abacking[ACKED_BUF_SIZE];
    encode_acked(ri, reg_id, seq_id, abacking);
    leveldb::Slice akey(abacking, ACKED_BUF_SIZE);
    std::string val;
    leveldb::Status st = m_db->Get(opts, akey, &val);

    if (st.ok())
    {
        return true;
    }
    else if (st.IsNotFound())
    {
        return false;
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption at the disk layer: region=" << reg_id
                   << " seq_id=" << seq_id << " desc=" << st.ToString();
        return false;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error at the disk layer: region=" << reg_id
                   << " seq_id=" << seq_id << " desc=" << st.ToString();
        return false;
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
        return false;
    }
}

void
datalayer :: mark_acked(const region_id& ri,
                        const region_id& reg_id,
                        uint64_t seq_id)
{
    // make it so that increasing seq_ids are ordered in reverse in the KVS
    seq_id = UINT64_MAX - seq_id;
    leveldb::WriteOptions opts;
    opts.sync = false;
    char abacking[ACKED_BUF_SIZE];
    encode_acked(ri, reg_id, seq_id, abacking);
    leveldb::Slice akey(abacking, ACKED_BUF_SIZE);
    leveldb::Slice val("", 0);
    leveldb::Status st = m_db->Put(opts, akey, val);

    if (st.ok())
    {
        // Yay!
    }
    else if (st.IsNotFound())
    {
        LOG(ERROR) << "mark_acked returned NOT_FOUND at the disk layer: region=" << reg_id
                   << " seq_id=" << seq_id << " desc=" << st.ToString();
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption at the disk layer: region=" << reg_id
                   << " seq_id=" << seq_id << " desc=" << st.ToString();
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error at the disk layer: region=" << reg_id
                   << " seq_id=" << seq_id << " desc=" << st.ToString();
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
    }
}

void
datalayer :: max_seq_id(const region_id& reg_id,
                        uint64_t* seq_id)
{
    leveldb::ReadOptions opts;
    opts.fill_cache = false;
    opts.verify_checksums = true;
    opts.snapshot = NULL;
    std::auto_ptr<leveldb::Iterator> it(m_db->NewIterator(opts));
    char abacking[ACKED_BUF_SIZE];
    encode_acked(reg_id, reg_id, 0, abacking);
    leveldb::Slice key(abacking, ACKED_BUF_SIZE);
    it->Seek(key);

    if (!it->Valid())
    {
        *seq_id = 0;
        return;
    }

    key = it->key();
    region_id tmp_ri;
    region_id tmp_reg_id;
    uint64_t tmp_seq_id;
    datalayer::returncode rc = decode_acked(e::slice(key.data(), key.size()),
                                            &tmp_ri, &tmp_reg_id, &tmp_seq_id);

    if (rc != SUCCESS || tmp_ri != reg_id || tmp_reg_id != reg_id)
    {
        *seq_id = 0;
        return;
    }

    *seq_id = UINT64_MAX - tmp_seq_id;
}

void
datalayer :: clear_acked(const region_id& reg_id,
                         uint64_t seq_id)
{
    leveldb::ReadOptions opts;
    opts.fill_cache = false;
    opts.verify_checksums = true;
    opts.snapshot = NULL;
    std::auto_ptr<leveldb::Iterator> it(m_db->NewIterator(opts));
    char abacking[ACKED_BUF_SIZE];
    encode_acked(region_id(0), reg_id, 0, abacking);
    it->Seek(leveldb::Slice(abacking, ACKED_BUF_SIZE));
    encode_acked(region_id(0), region_id(reg_id.get() + 1), 0, abacking);
    leveldb::Slice upper_bound(abacking, ACKED_BUF_SIZE);

    while (it->Valid() &&
           it->key().compare(upper_bound) < 0)
    {
        region_id tmp_ri;
        region_id tmp_reg_id;
        uint64_t tmp_seq_id;
        datalayer::returncode rc = decode_acked(e::slice(it->key().data(), it->key().size()),
                                                &tmp_ri, &tmp_reg_id, &tmp_seq_id);
        tmp_seq_id = UINT64_MAX - tmp_seq_id;

        if (rc == SUCCESS &&
            tmp_reg_id == reg_id &&
            tmp_seq_id < seq_id)
        {
            leveldb::WriteOptions wopts;
            wopts.sync = false;
            leveldb::Status st = m_db->Delete(wopts, it->key());

            if (st.ok() || st.IsNotFound())
            {
                // WOOT!
            }
            else if (st.IsCorruption())
            {
                LOG(ERROR) << "corruption at the disk layer: could not delete "
                           << reg_id << " " << seq_id << ": desc=" << st.ToString();
            }
            else if (st.IsIOError())
            {
                LOG(ERROR) << "IO error at the disk layer: could not delete "
                           << reg_id << " " << seq_id << ": desc=" << st.ToString();
            }
            else
            {
                LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
            }
        }

        it->Next();
    }
}

void
datalayer :: request_wipe(const capture_id& cid)
{
    po6::threads::mutex::hold hold(&m_block_cleaner);
    m_state_transfer_captures.insert(cid);
    m_wakeup_cleaner.broadcast();
}

datalayer::snapshot
datalayer :: make_snapshot()
{
    return leveldb_snapshot_ptr(m_db, m_db->GetSnapshot());
}

datalayer::iterator*
datalayer :: make_region_iterator(snapshot snap,
                                  const region_id& ri,
                                  returncode* error)
{
    *error = datalayer::SUCCESS;
    const size_t backing_sz = sizeof(uint8_t) + sizeof(uint64_t);
    char backing[backing_sz];
    char* ptr = backing;
    ptr = e::pack8be('o', ptr);
    ptr = e::pack64be(ri.get(), ptr);

    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    opts.snapshot = snap.get();
    leveldb_iterator_ptr iter;
    iter.reset(snap, m_db->NewIterator(opts));
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    return new region_iterator(iter, ri, index_info::lookup(sc.attrs[0].type));
}

datalayer::iterator*
datalayer :: make_search_iterator(snapshot snap,
                                  const region_id& ri,
                                  const std::vector<attribute_check>& checks,
                                  std::ostringstream* ostr)
{
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<e::intrusive_ptr<index_iterator> > iterators;

    // pull a set of range queries from checks
    std::vector<range> ranges;
    range_searches(checks, &ranges);
    index_info* ki = index_info::lookup(sc.attrs[0].type);
    const subspace& sub(*m_daemon->m_config.get_subspace(ri));

    // for each range query, construct an iterator
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        if (ranges[i].invalid)
        {
            if (ostr) *ostr << "encountered invalid range; returning no results\n";
            return new dummy_iterator();
        }

        assert(ranges[i].attr < sc.attrs_sz);
        assert(ranges[i].type == sc.attrs[ranges[i].attr].type);

        if (ostr) *ostr << "considering attr " << ranges[i].attr << " Range("
                        << ranges[i].start.hex() << ", " << ranges[i].end.hex() << " " << ranges[i].type << " "
                        << (ranges[i].has_start ? "[" : "<") << "-" << (ranges[i].has_end ? "]" : ">")
                        << " " << (ranges[i].invalid ? "invalid" : "valid") << "\n";

        if (!sub.indexed(ranges[i].attr))
        {
            continue;
        }

        index_info* ii = index_info::lookup(ranges[i].type);

        if (ii)
        {
            e::intrusive_ptr<index_iterator> it = ii->iterator_from_range(snap, ri, ranges[i], ki);

            if (it)
            {
                iterators.push_back(it);
            }
        }
    }

    // for everything that is not a range query, construct an iterator
    for (size_t i = 0; i < checks.size(); ++i)
    {
        if (checks[i].predicate == HYPERPREDICATE_EQUALS ||
            checks[i].predicate == HYPERPREDICATE_LESS_EQUAL ||
            checks[i].predicate == HYPERPREDICATE_GREATER_EQUAL)
        {
            continue;
        }

        if (!sub.indexed(checks[i].attr))
        {
            continue;
        }

        index_info* ii = index_info::lookup(sc.attrs[checks[i].attr].type);

        if (ii)
        {
            e::intrusive_ptr<index_iterator> it = ii->iterator_from_check(snap, ri, checks[i], ki);

            if (it)
            {
                iterators.push_back(it);
            }
        }
    }

    // figure out the cost of accessing all objects
    e::intrusive_ptr<index_iterator> full_scan;
    range scan;
    scan.attr = 0;
    scan.type = sc.attrs[0].type;
    scan.has_start = false;
    scan.has_end = false;
    scan.invalid = false;
    full_scan = ki->iterator_from_range(snap, ri, scan, ki);
    if (ostr) *ostr << "accessing all objects has cost " << full_scan->cost(m_db.get()) << "\n";

    // figure out the cost of each iterator
    // we do this here and not below so that iterators can cache the size and we
    // don't ping-pong between HyperDex and LevelDB.
    for (size_t i = 0; i < iterators.size(); ++i)
    {
        uint64_t iterator_cost = iterators[i]->cost(m_db.get());
        if (ostr) *ostr << "iterator " << *iterators[i] << " has cost " << iterator_cost << "\n";
    }

    std::vector<e::intrusive_ptr<index_iterator> > sorted;
    std::vector<e::intrusive_ptr<index_iterator> > unsorted;

    for (size_t i = 0; i < iterators.size(); ++i)
    {
        if (iterators[i]->sorted())
        {
            sorted.push_back(iterators[i]);
        }
        else
        {
            unsorted.push_back(iterators[i]);
        }
    }

    e::intrusive_ptr<index_iterator> best;

    if (!sorted.empty())
    {
        best = new intersect_iterator(snap, sorted);
    }

    if (!best || best->cost(m_db.get()) * 4 > full_scan->cost(m_db.get()))
    {
        best = full_scan;
    }

    // just pick one; do something smart later
    if (!unsorted.empty() && !best)
    {
        best = unsorted[0];
    }

    assert(best);
    if (ostr) *ostr << "choosing to use " << *best << "\n";
    return new search_iterator(this, ri, best, ostr, &checks);
}

datalayer::returncode
datalayer :: get_from_iterator(const region_id& ri,
                               iterator* iter,
                               e::slice* key,
                               std::vector<e::slice>* value,
                               uint64_t* version,
                               reference* ref)
{
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    std::vector<char> scratch;

    // create the encoded key
    leveldb::Slice lkey;
    encode_key(ri, sc.attrs[0].type, iter->key(), &scratch, &lkey);

    // perform the read
    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    opts.snapshot = iter->snap().get();
    leveldb::Status st = m_db->Get(opts, lkey, &ref->m_backing);

    if (st.ok())
    {
        ref->m_backing += std::string(reinterpret_cast<const char*>(iter->key().data()), iter->key().size());
        *key = e::slice(ref->m_backing.data()
                        + ref->m_backing.size()
                        - iter->key().size(),
                        iter->key().size());
        e::slice v(ref->m_backing.data(), ref->m_backing.size() - iter->key().size());
        return decode_value(v, value, version);
    }
    else if (st.IsNotFound())
    {
        return NOT_FOUND;
    }
    else
    {
        return handle_error(st);
    }
}

void
datalayer :: cleaner()
{
    LOG(INFO) << "cleanup thread started";
    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        return;
    }

    if (pthread_sigmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        PLOG(ERROR) << "could not block signals";
        return;
    }

    while (true)
    {
        std::set<capture_id> state_transfer_captures;

        {
            po6::threads::mutex::hold hold(&m_block_cleaner);

            while ((!m_need_cleaning &&
                    m_state_transfer_captures.empty() &&
                    !m_shutdown) || m_need_pause)
            {
                m_paused = true;

                if (m_need_pause)
                {
                    m_wakeup_reconfigurer.signal();
                }

                m_wakeup_cleaner.wait();
                m_paused = false;
            }

            if (m_shutdown)
            {
                break;
            }

            m_state_transfer_captures.swap(state_transfer_captures);
            m_need_cleaning = false;
        }

        leveldb::ReadOptions opts;
        opts.fill_cache = true;
        opts.verify_checksums = true;
        std::auto_ptr<leveldb::Iterator> it;
        it.reset(m_db->NewIterator(opts));
        it->Seek(leveldb::Slice("t", 1));
        capture_id cached_cid;

        while (it->Valid())
        {
            uint8_t prefix;
            uint64_t cid;
            uint64_t seq_no;
            e::unpacker up(it->key().data(), it->key().size());
            up = up >> prefix >> cid >> seq_no;

            if (up.error() || prefix != 't')
            {
                break;
            }

            if (cid == cached_cid.get())
            {
                leveldb::WriteOptions wopts;
                wopts.sync = false;
                leveldb::Status st = m_db->Delete(wopts, it->key());

                if (st.ok() || st.IsNotFound())
                {
                    // pass
                }
                else if (st.IsCorruption())
                {
                    LOG(ERROR) << "corruption at the disk layer: could not cleanup old transfers:"
                               << " desc=" << st.ToString();
                }
                else if (st.IsIOError())
                {
                    LOG(ERROR) << "IO error at the disk layer: could not cleanup old transfers:"
                               << " desc=" << st.ToString();
                }
                else
                {
                    LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
                }

                it->Next();
                continue;
            }

            m_daemon->m_stm.report_wiped(cached_cid);

            if (!m_daemon->m_config.is_captured_region(capture_id(cid)))
            {
                cached_cid = capture_id(cid);
                continue;
            }

            if (state_transfer_captures.find(capture_id(cid)) != state_transfer_captures.end())
            {
                cached_cid = capture_id(cid);
                state_transfer_captures.erase(cached_cid);
                continue;
            }

            char tbacking[TRANSFER_BUF_SIZE];
            leveldb::Slice slice(tbacking, TRANSFER_BUF_SIZE);
            encode_transfer(capture_id(cid + 1), 0, tbacking);
            it->Seek(slice);
        }

        while (!state_transfer_captures.empty())
        {
            m_daemon->m_stm.report_wiped(*state_transfer_captures.begin());
            state_transfer_captures.erase(state_transfer_captures.begin());
        }
    }

    LOG(INFO) << "cleanup thread shutting down";
}

void
datalayer :: shutdown()
{
    bool is_shutdown;

    {
        po6::threads::mutex::hold hold(&m_block_cleaner);
        m_wakeup_cleaner.broadcast();
        is_shutdown = m_shutdown;
        m_shutdown = true;
    }

    if (!is_shutdown)
    {
        m_cleaner.join();
    }
}

datalayer::returncode
datalayer :: handle_error(leveldb::Status st)
{
    if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption at the disk layer: " << st.ToString();
        return CORRUPTION;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error at the disk layer: " << st.ToString();
        return IO_ERROR;
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
        return LEVELDB_ERROR;
    }
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
