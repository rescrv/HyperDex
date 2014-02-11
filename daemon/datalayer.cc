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

#define STRLENOF(x)	(sizeof(x)-1)

// ASSUME:  all keys put into leveldb have a first byte without the high bit set

using po6::threads::make_thread_wrapper;
using hyperdex::datalayer;
using hyperdex::reconfigure_returncode;

datalayer :: datalayer(daemon* d)
    : m_daemon(d)
    , m_db()
    , m_checkpointer(make_thread_wrapper(&datalayer::checkpointer, this))
    , m_wiper(make_thread_wrapper(&datalayer::wiper, this))
    , m_protect()
    , m_wakeup_checkpointer(&m_protect)
    , m_wakeup_wiper(&m_protect)
    , m_wakeup_reconfigurer(&m_protect)
    , m_shutdown(true)
    , m_need_pause(false)
    , m_checkpointer_paused(false)
    , m_wiper_paused(false)
    , m_checkpoint_gc(0)
    , m_wiping()
{
    po6::threads::mutex::hold hold(&m_protect);
}

datalayer :: ~datalayer() throw ()
{
    shutdown();
}

bool
datalayer :: initialize(const po6::pathname& path,
                        bool* saved,
                        server_id* saved_us,
                        po6::net::location* saved_bind_to,
                        po6::net::hostname* saved_coordinator)
{
    leveldb::Options opts;
    opts.write_buffer_size = 16ULL * 1024ULL * 1024ULL;
    opts.create_if_missing = true;
    opts.filter_policy = leveldb::NewBloomFilterPolicy(10);
    opts.manual_garbage_collection = true;
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
    leveldb::WriteOptions wopts;
    wopts.sync = true;

    // read the "hyperdex" key and check the version
    std::string rbacking;
    st = m_db->Get(ropts, leveldb::Slice("hyperdex", 8), &rbacking);
    bool first_time = false;

    if (st.ok())
    {
        first_time = false;

        if (rbacking != PACKAGE_VERSION &&
            rbacking != "1.0.5" &&
            rbacking != "1.0.4" &&
            rbacking != "1.0.3" &&
            rbacking != "1.0.2" &&
            rbacking != "1.0.1" &&
            rbacking != "1.0.0")
        {
            LOG(ERROR) << "could not restore from disk because "
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
        leveldb::Slice v(PACKAGE_VERSION, STRLENOF(PACKAGE_VERSION));
        st = m_db->Put(wopts, k, v);

        if (!st.ok())
        {
            LOG(ERROR) << "could not save \"hyperdex\" key to disk: " << st.ToString();
            return false;
        }
    }
    else
    {
        LOG(ERROR) << "could not read \"hyperdex\" key from LevelDB: " << st.ToString();
        return false;
    }

    // read the "state" key and parse it
    std::string sbacking;
    st = m_db->Get(ropts, leveldb::Slice("state", 5), &sbacking);

    if (st.ok())
    {
        if (first_time)
        {
            LOG(ERROR) << "could not restore from LevelDB because a previous "
                       << "execution crashed and the database was tampered with; "
                       << "you'll need to manually erase this DB and create a new one";
            return false;
        }

        e::unpacker up(sbacking.data(), sbacking.size());
        uint64_t us;
        up = up >> us >> *saved_bind_to >> *saved_coordinator;
        *saved_us = server_id(us);

        if (up.error())
        {
            LOG(ERROR) << "could not restore from LevelDB because a previous "
                       << "execution wrote an invalid state; "
                       << "you'll need to manually erase this DB and create a new one";
            return false;
        }
    }
    else if (st.IsNotFound())
    {
        if (!only_key_is_hyperdex_key())
        {
            LOG(ERROR) << "could not restore from LevelDB because a previous "
                       << "execution didn't save state and wrote other data; "
                       << "you'll need to manually erase this DB and create a new one";
            return false;
        }
    }
    else
    {
        LOG(ERROR) << "could not read \"hyperdex\" key from LevelDB: " << st.ToString();
        return false;
    }

    {
        po6::threads::mutex::hold hold(&m_protect);
        m_checkpointer.start();
        m_wiper.start();
        m_shutdown = false;
    }

    *saved = !first_time;
    return true;
}

void
datalayer :: teardown()
{
    shutdown();
}

bool
datalayer :: save_state(const server_id& us,
                        const po6::net::location& bind_to,
                        const po6::net::hostname& coordinator)
{
    leveldb::WriteOptions wopts;
    wopts.sync = true;
    size_t sz = sizeof(uint64_t)
              + pack_size(bind_to)
              + pack_size(coordinator);
    std::auto_ptr<e::buffer> state(e::buffer::create(sz));
    *state << us << bind_to << coordinator;
    leveldb::Status st = m_db->Put(wopts, leveldb::Slice("state", 5),
                                   leveldb::Slice(reinterpret_cast<const char*>(state->data()), state->size()));

    if (st.ok())
    {
        return true;
    }
    else
    {
        LOG(ERROR) << "could not save state: " << st.ToString();
        return false;
    }
}

void
datalayer :: pause()
{
    po6::threads::mutex::hold hold(&m_protect);
    assert(!m_need_pause);
    m_need_pause = true;
}

void
datalayer :: unpause()
{
    po6::threads::mutex::hold hold(&m_protect);
    assert(m_need_pause);
    m_wakeup_checkpointer.broadcast();
    m_wakeup_wiper.broadcast();
    m_need_pause = false;
}

void
datalayer :: reconfigure(const configuration&,
                         const configuration&,
                         const server_id&)
{
    {
        po6::threads::mutex::hold hold(&m_protect);
        assert(m_need_pause);

        while (!m_checkpointer_paused || !m_wiper_paused)
        {
            m_wakeup_reconfigurer.wait();
        }
    }
}

bool
datalayer :: get_property(const e::slice& property,
                          std::string* value)
{
    leveldb::Slice prop(reinterpret_cast<const char*>(property.data()), property.size());
    return m_db->GetProperty(prop, value);
}

std::string
datalayer :: get_timestamp()
{
    std::string timestamp;
    m_db->GetReplayTimestamp(&timestamp);
    return timestamp;
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

        if (ostr) *ostr << " considering attr " << ranges[i].attr << " Range("
                        << ranges[i].start.hex() << ", " << ranges[i].end.hex() << ") " << ranges[i].type << " "
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
    if (ostr) *ostr << " accessing all objects has cost " << full_scan->cost(m_db.get()) << "\n";

    // figure out the cost of each iterator
    // we do this here and not below so that iterators can cache the size and we
    // don't ping-pong between HyperDex and LevelDB.
    for (size_t i = 0; i < iterators.size(); ++i)
    {
        uint64_t iterator_cost = iterators[i]->cost(m_db.get());
        if (ostr) *ostr << " iterator " << *iterators[i] << " has cost " << iterator_cost << "\n";
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

    if (!best && !sorted.empty())
    {
        best = new intersect_iterator(snap, sorted);
    }
    else if (!best && !unsorted.empty())
    {
        best = unsorted[0];
    }
    else
    {
        best = full_scan;
    }

    assert(best);
    uint64_t cost = best->cost(m_db.get());

    if (cost > 0 && cost * 4 > full_scan->cost(m_db.get()))
    {
        best = full_scan;
    }

    if (ostr) *ostr << " choosing to use " << *best << "\n";
    return new search_iterator(this, ri, best, ostr, &checks);
}

bool
datalayer :: backup(const e::slice& _name)
{
    leveldb::Slice name(reinterpret_cast<const char*>(_name.data()), _name.size());
    leveldb::Status st = m_db->LiveBackup(name);

    if (st.ok())
    {
        return true;
    }
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption while taking a backup: " << st.ToString();
        return false;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error while taking a backup: " << st.ToString();
        return false;
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle: " << st.ToString();
        return false;
    }
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

datalayer::returncode
datalayer :: create_checkpoint(const region_timestamp& rt)
{
    po6::threads::mutex::hold hold(&m_protect);

    for (wipe_list_t::iterator it = m_wiping.begin(); it != m_wiping.end(); ++it)
    {
        if (it->second == rt.rid)
        {
            return SUCCESS;
        }
    }

    char cbacking[CHECKPOINT_BUF_SIZE + 1024];
    encode_checkpoint(rt.rid, rt.checkpoint, cbacking);
    leveldb::WriteOptions opts;
    opts.sync = false;
    leveldb::Slice ckey(cbacking, CHECKPOINT_BUF_SIZE);
    leveldb::Slice val(rt.local_timestamp);
    leveldb::Status st = m_db->Put(opts, ckey, val);

    if (!st.ok())
    {
        return handle_error(st);
    }

    return SUCCESS;
}

void
datalayer :: set_checkpoint_lower_gc(uint64_t checkpoint_gc)
{
    po6::threads::mutex::hold hold(&m_protect);
    m_checkpoint_gc = std::max(checkpoint_gc, m_checkpoint_gc);
    m_wakeup_checkpointer.signal();
}

void
datalayer :: largest_checkpoint_for(const region_id& ri, uint64_t* checkpoint)
{
    po6::threads::mutex::hold hold(&m_protect);

    for (wipe_list_t::iterator it = m_wiping.begin(); it != m_wiping.end(); ++it)
    {
        if (it->second == ri)
        {
            *checkpoint = 0;
            return;
        }
    }

    leveldb::ReadOptions opts;
    opts.verify_checksums = true;
    std::auto_ptr<leveldb::Iterator> it;
    it.reset(m_db->NewIterator(opts));
    char cbacking[CHECKPOINT_BUF_SIZE];
    encode_checkpoint(ri, 0, cbacking);
    it->Seek(leveldb::Slice(cbacking, CHECKPOINT_BUF_SIZE));
    *checkpoint = 0;

    while (it->Valid())
    {
        region_timestamp rt;
        e::slice key(it->key().data(), it->key().size());
        returncode rc = decode_checkpoint(key, &rt.rid, &rt.checkpoint);

        if (rc != datalayer::SUCCESS)
        {
            break;
        }

        if (rt.rid != ri)
        {
            break;
        }

        *checkpoint = std::max(*checkpoint, rt.checkpoint);
        it->Next();
    }
}

void
datalayer :: request_wipe(const transfer_id& xid,
                          const region_id& ri)
{
    po6::threads::mutex::hold hold(&m_protect);
    m_wiping.push_back(std::make_pair(xid, ri));
    m_wakeup_wiper.broadcast();
}

datalayer::replay_iterator*
datalayer :: replay_region_from_checkpoint(const region_id& ri,
                                           uint64_t checkpoint,
                                           bool* wipe)
{
    po6::threads::mutex::hold hold(&m_protect);
    leveldb::ReadOptions opts;
    opts.verify_checksums = true;
    std::auto_ptr<leveldb::Iterator> it;
    it.reset(m_db->NewIterator(opts));
    char cbacking[CHECKPOINT_BUF_SIZE];
    encode_checkpoint(ri, 0, cbacking);
    it->Seek(leveldb::Slice(cbacking, CHECKPOINT_BUF_SIZE));
    std::string local_timestamp("all");

    while (it->Valid())
    {
        region_timestamp rt;
        e::slice key(it->key().data(), it->key().size());
        returncode rc = decode_checkpoint(key, &rt.rid, &rt.checkpoint);

        if (rc != datalayer::SUCCESS)
        {
            break;
        }

        if (rt.rid != ri || rt.checkpoint > checkpoint)
        {
            break;
        }

        local_timestamp.assign(it->value().data(), it->value().size());
        it->Next();
    }

    for (wipe_list_t::iterator w = m_wiping.begin(); w != m_wiping.end(); ++w)
    {
        if (w->second == ri)
        {
            local_timestamp = "all";
            break;
        }
    }

    *wipe = local_timestamp == "all";
    leveldb::ReplayIterator* iter;
    leveldb::Status st = m_db->GetReplayIterator(local_timestamp, &iter);

    if (!st.ok())
    {
        LOG(ERROR) << "LevelDB corruption: invalid timestamp";
        abort();
    }

    leveldb_replay_iterator_ptr ptr(m_db, iter);
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    return new replay_iterator(ri, ptr, index_info::lookup(sc.attrs[0].type));
}

void
datalayer :: collect_lower_checkpoints(uint64_t checkpoint_gc)
{
    po6::threads::mutex::hold hold(&m_protect);
    leveldb::ReadOptions opts;
    opts.verify_checksums = true;
    std::auto_ptr<leveldb::Iterator> it;
    it.reset(m_db->NewIterator(opts));
    it->Seek(leveldb::Slice("c", 1));
    std::string lower_bound_timestamp("now");

    while (it->Valid())
    {
        region_timestamp rt;
        e::slice key(it->key().data(), it->key().size());
        returncode rc = decode_checkpoint(key, &rt.rid, &rt.checkpoint);

        if (rc != datalayer::SUCCESS)
        {
            break;
        }

        rt.local_timestamp = std::string(it->value().data(), it->value().size());

        if (rt.checkpoint >= checkpoint_gc &&
            m_db->ValidateTimestamp(rt.local_timestamp))
        {
            if (m_db->CompareTimestamps(rt.local_timestamp, lower_bound_timestamp) < 0)
            {
                lower_bound_timestamp = rt.local_timestamp;
            }

            it->Next();
            continue;
        }

        leveldb::WriteOptions wopts;
        wopts.sync = false;
        leveldb::Status st = m_db->Delete(wopts, it->key());

        if (!st.ok())
        {
            break;
        }

        it->Next();
    }

    m_db->AllowGarbageCollectBeforeTimestamp(lower_bound_timestamp);
}

bool
datalayer :: only_key_is_hyperdex_key()
{
    leveldb::ReadOptions opts;
    opts.fill_cache = false;
    opts.verify_checksums = true;
    opts.snapshot = NULL;
    std::auto_ptr<leveldb::Iterator> it(m_db->NewIterator(opts));
    it->SeekToFirst();
    bool seen = false;

    while (it->Valid())
    {
        if (it->key().compare(leveldb::Slice("hyperdex", 8)) != 0)
        {
            return false;
        }

        it->Next();
        seen = true;
    }

    return seen;
}

void
datalayer :: checkpointer()
{
    LOG(INFO) << "checkpoint thread started";
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

    bool need_checkpoint_gc = false;
    uint64_t checkpoint_gc = 0;

    while (true)
    {
        {
            po6::threads::mutex::hold hold(&m_protect);

            while ((checkpoint_gc >= m_checkpoint_gc && !m_shutdown) ||
                   m_need_pause)
            {
                m_checkpointer_paused = true;

                if (m_need_pause)
                {
                    m_wakeup_reconfigurer.signal();
                }

                m_wakeup_checkpointer.wait();
                m_checkpointer_paused = false;
            }

            if (m_shutdown)
            {
                break;
            }

            need_checkpoint_gc = checkpoint_gc < m_checkpoint_gc;
            checkpoint_gc = m_checkpoint_gc;
        }

        if (need_checkpoint_gc)
        {
            collect_lower_checkpoints(checkpoint_gc);
        }
    }

    LOG(INFO) << "checkpoint thread shutting down";
}

void
datalayer :: wiper()
{
    LOG(INFO) << "wiping thread started";
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
        transfer_id xid;
        region_id rid;

        {
            po6::threads::mutex::hold hold(&m_protect);

            while ((m_wiping.empty() && !m_shutdown) || m_need_pause)
            {
                m_wiper_paused = true;

                if (m_need_pause)
                {
                    m_wakeup_reconfigurer.signal();
                }

                m_wakeup_wiper.wait();
                m_wiper_paused = false;
            }

            if (m_shutdown)
            {
                break;
            }

            if (!m_wiping.empty())
            {
                xid = m_wiping.front().first;
                rid = m_wiping.front().second;
            }
        }

        assert(rid != region_id());
        wipe_checkpoints(rid);

        if (wipe_some_indices(rid) &&
            wipe_some_objects(rid))
        {
            m_daemon->m_stm.report_wiped(xid);
            po6::threads::mutex::hold hold(&m_protect);
            m_wiping.pop_front();
        }
    }

    LOG(INFO) << "wiping thread shutting down";
}

void
datalayer :: wipe_checkpoints(const region_id& ri)
{
    po6::threads::mutex::hold hold(&m_protect);
    std::auto_ptr<leveldb::Iterator> it;
    it.reset(m_db->NewIterator(leveldb::ReadOptions()));
    char cbacking[CHECKPOINT_BUF_SIZE];
    encode_checkpoint(ri, 0, cbacking);
    it->Seek(leveldb::Slice(cbacking, CHECKPOINT_BUF_SIZE));

    while (it->Valid())
    {
        region_timestamp rt;
        e::slice key(it->key().data(), it->key().size());
        returncode rc = decode_checkpoint(key, &rt.rid, &rt.checkpoint);

        if (rc != datalayer::SUCCESS || rt.rid != ri)
        {
            break;
        }

        m_db->Delete(leveldb::WriteOptions(), it->key());
        it->Next();
    }
}

bool
datalayer :: wipe_some_indices(const region_id& ri)
{
    return wipe_some_common('i', ri);
}

bool
datalayer :: wipe_some_objects(const region_id& ri)
{
    return wipe_some_common('o', ri);
}

bool
datalayer :: wipe_some_common(uint8_t c, const region_id& ri)
{
    std::auto_ptr<leveldb::Iterator> it;
    it.reset(m_db->NewIterator(leveldb::ReadOptions()));
    char backing[sizeof(uint8_t) + sizeof(uint64_t)];
    e::pack8be(c, backing);
    e::pack64be(ri.get(), backing + sizeof(uint8_t));
    leveldb::Slice prefix(backing, sizeof(uint8_t) + sizeof(uint64_t));
    it->Seek(prefix);

    for (uint64_t i = 0; i < 65536 && it->Valid(); ++i)
    {
        if (!it->key().starts_with(prefix))
        {
            return true;
        }

        m_db->Delete(leveldb::WriteOptions(), it->key());
        it->Next();
    }

    return false;
}

void
datalayer :: shutdown()
{
    bool is_shutdown;

    {
        po6::threads::mutex::hold hold(&m_protect);
        m_wakeup_checkpointer.broadcast();
        m_wakeup_wiper.broadcast();
        is_shutdown = m_shutdown;
        m_shutdown = true;
    }

    if (!is_shutdown)
    {
        m_checkpointer.join();
        m_wiper.join();
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
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle:";
        LOG(ERROR) << st.ToString();
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
