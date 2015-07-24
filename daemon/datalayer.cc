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
#include <map>
#include <sstream>
#include <string>

// Google Log
#include <glog/logging.h>

// LevelDB
#include <hyperleveldb/write_batch.h>
#include <hyperleveldb/filter_policy.h>

// e
#include <e/atomic.h>
#include <e/endian.h>
#include <e/tuple_compare.h>
#include <e/varint.h>

// HyperDex
#include "common/datatype_info.h"
#include "common/macros.h"
#include "common/range_searches.h"
#include "common/serialization.h"
#include "daemon/background_thread.h"
#include "daemon/daemon.h"
#include "daemon/datalayer.h"
#include "daemon/datalayer_checkpointer_thread.h"
#include "daemon/datalayer_encodings.h"
#include "daemon/datalayer_index_state.h"
#include "daemon/datalayer_indexer_thread.h"
#include "daemon/datalayer_iterator.h"
#include "daemon/datalayer_wiper_thread.h"

#define STRLENOF(x)	(sizeof(x)-1)

// ASSUME:  all keys put into leveldb have a first byte without the high bit set

using po6::threads::make_thread_wrapper;
using hyperdex::datalayer;
using hyperdex::reconfigure_returncode;

const hyperdex::region_id datalayer::defaultri;

datalayer :: datalayer(daemon* d)
    : m_daemon(d)
    , m_db()
    , m_indices()
    , m_versions()
    , m_checkpointer(new checkpointer_thread(d))
    , m_mediator(new wiper_indexer_mediator())
    , m_indexer(new indexer_thread(d, m_mediator.get()))
    , m_wiper(new wiper_thread(d, m_mediator.get()))
{
}

datalayer :: ~datalayer() throw ()
{
    m_checkpointer->shutdown();
    m_indexer->shutdown();
    m_wiper->shutdown();
}

#define FORMAT_1_6 "v1.6.0 format"

bool
datalayer :: initialize(const std::string& path,
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
    opts.max_open_files = std::max(sysconf(_SC_OPEN_MAX) >> 1, 1024L);
    std::string name(path);
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

        if (rbacking != FORMAT_1_6)
        {
            LOG(ERROR) << "could not restore daemon "
                       << "the existing data was created with"
                       << "HyperDex " << rbacking << " but "
                       << "this is requires the v1.6.0-compatible format";
            return false;
        }
    }
    else if (st.IsNotFound())
    {
        first_time = true;
        leveldb::Slice k("hyperdex", 8);
        leveldb::Slice v(FORMAT_1_6, STRLENOF(FORMAT_1_6));
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

    m_checkpointer->start();
    m_indexer->start();
    m_wiper->start();
    *saved = !first_time;
    return true;
}

void
datalayer :: teardown()
{
    m_checkpointer->shutdown();
    m_indexer->shutdown();
    m_wiper->shutdown();
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
    state->pack() << us << bind_to << coordinator;
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
    m_checkpointer->initiate_pause();
    m_indexer->initiate_pause();
    m_wiper->initiate_pause();
}

void
datalayer :: unpause()
{
    m_checkpointer->unpause();
    m_indexer->unpause();
    m_wiper->unpause();
}

void
datalayer :: reconfigure(const configuration&,
                         const configuration& config,
                         const server_id&)
{
    m_checkpointer->wait_until_paused();
    m_indexer->wait_until_paused();
    m_wiper->wait_until_paused();

    // indices that must exist
    std::vector<std::pair<region_id, index_id> > indices;
    config.all_indices(m_daemon->m_us, &indices);
    std::sort(indices.begin(), indices.end());
    std::vector<std::pair<region_id, index_id> >::iterator it;
    it = std::unique(indices.begin(), indices.end());
    indices.resize(it - indices.begin());

    // iterate and create indices where necessary
    std::vector<index_state> next_indices;
    size_t old_idx = 0;
    size_t new_idx = 0;

    while (new_idx < indices.size())
    {
        int cmp = 1;

        if (old_idx < m_indices.size())
        {
            cmp = e::tuple_compare(m_indices[old_idx].ri, m_indices[old_idx].ii,
                                   indices[new_idx].first, indices[old_idx].second);
        }

        if (cmp == 0)
        {
            // Case B
            next_indices.push_back(m_indices[old_idx]);
            ++old_idx;
            ++new_idx;
        }
        else if (cmp < 0)
        {
            // Case A
            ++old_idx;
        }
        else if (cmp > 0)
        {
            next_indices.push_back(index_state(indices[new_idx].first,
                                               indices[new_idx].second));
            ++new_idx;
            region_id ri(indices[new_idx].first);
            index_id ii(indices[new_idx].second);
            char buf[sizeof(uint8_t) + 2 * VARINT_64_MAX_SIZE];
            char* ptr = buf;
            ptr = e::pack8be('I', ptr);
            ptr = e::packvarint64(ri.get(), ptr);
            ptr = e::packvarint64(ii.get(), ptr);
            leveldb::ReadOptions ro;
            leveldb::Slice key(buf, ptr - buf);
            std::string val;
            leveldb::Status st = m_db->Get(ro, key, &val);

            if (st.ok())
            {
                next_indices.back().set_usable();
            }
            else if (!st.IsNotFound())
            {
                handle_error(st);
            }
        }
    }

    m_indices.swap(next_indices);

    // update the versions
    std::vector<region_id> key_regions;
    config.key_regions(m_daemon->m_us, &key_regions);
    std::vector<region_id> xfer_regions;
    config.transfers_in_regions(m_daemon->m_us, &xfer_regions);

    for (size_t i = 0; i < xfer_regions.size(); ++i)
    {
        if (config.is_point_leader(config.head_of_region(xfer_regions[i])))
        {
            key_regions.push_back(xfer_regions[i]);
        }
    }

    e::ao_hash_map<region_id, uint64_t, id, defaultri> new_versions;

    for (size_t i = 0; i < key_regions.size(); ++i)
    {
        uint64_t val = 0;

        if (!m_versions.get(key_regions[i], &val))
        {
            val = disk_version(key_regions[i]);
        }

        new_versions.put(key_regions[i], val);
    }

    m_versions.swap(&new_versions);
    m_indexer->kick();
    m_wiper->kick();
}

void
datalayer :: debug_dump()
{
    LOG(INFO) << "index state ===================================================================";

    for (size_t i = 0; i < m_indices.size(); ++i)
    {
        LOG(INFO) << "index id=" << m_indices[i].ii.get()
                  << " region=" << m_indices[i].ri.get()
                  << " usable=" << (m_indices[i].is_usable() ? "yes" : "no");
    }

    m_checkpointer->debug_dump();
    m_mediator->debug_dump();
    m_indexer->debug_dump();
    m_wiper->debug_dump();
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
    std::vector<const index*> indices;
    find_indices(ri, &indices);
    create_index_changes(sc, ri, indices, key, &old_value, NULL, &updates);

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
    std::vector<const index*> indices;
    find_indices(ri, &indices);
    create_index_changes(sc, ri, indices, key, NULL, &new_value, &updates);

    // ensure we've recorded a version at least as high as this key
    write_version(ri, version, &updates);

    // Perform the write
    leveldb::WriteOptions opts;
    opts.sync = false;
    leveldb::Status st = m_db->Write(opts, &updates);

    if (st.ok())
    {
        update_memory_version(ri, version);
        return SUCCESS;
    }
    else
    {
        return handle_error(st);
    }
}

datalayer::returncode
datalayer :: overput(const region_id& ri,
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
    std::vector<const index*> indices;
    find_indices(ri, &indices);
    create_index_changes(sc, ri, indices, key, &old_value, &new_value, &updates);

    // ensure we've recorded a version at least as high as this key
    write_version(ri, version, &updates);

    // Perform the write
    leveldb::WriteOptions opts;
    opts.sync = false;
    leveldb::Status st = m_db->Write(opts, &updates);

    if (st.ok())
    {
        update_memory_version(ri, version);
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

        return del(ri, key, old_value);
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

        return overput(ri, key, old_value, new_value, version);
    }
    else if (st.IsNotFound())
    {
        return put(ri, key, new_value, version);
    }
    else
    {
        return handle_error(st);
    }
}

datalayer::snapshot
datalayer :: make_snapshot()
{
    return leveldb_snapshot_ptr(m_db, m_db->GetSnapshot());
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
    range_searches(sc, checks, &ranges);
    const index_encoding* key_ie = index_encoding::lookup(sc.attrs[0].type);
    const index_info* key_ii = index_info::lookup(sc.attrs[0].type);

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
        std::vector<const index*> indices;
        find_indices(ri, ranges[i].attr, &indices);

        for (size_t j = 0; j < indices.size(); ++j)
        {
            assert(indices[j]->attr == ranges[i].attr);
            const index* idx = indices[j];
            const index_info* ii = index_info::lookup(ranges[i].type);

            if (!ii)
            {
                continue;
            }

            e::intrusive_ptr<index_iterator> it = ii->iterator_from_range(snap, ri, idx->id, ranges[i], key_ie);

            if (it)
            {
                iterators.push_back(it);

                if (ostr) *ostr << " considering attr " << ranges[i].attr << " Range("
                                << ranges[i].start.hex() << ", " << ranges[i].end.hex() << ") " << ranges[i].type << " "
                                << (ranges[i].has_start ? "[" : "<") << "-" << (ranges[i].has_end ? "]" : ">")
                                << " " << (ranges[i].invalid ? "invalid" : "valid") << "\n";
            }
        }
    }

    // For each index
    for (size_t i = 0; i < checks.size(); ++i)
    {
        std::vector<const index*> indices;
        find_indices(ri, checks[i].attr, &indices);

        for (size_t j = 0; j < indices.size(); ++j)
        {
            const index* idx = indices[j];
            const index_info* ii = index_info::lookup(sc.attrs[checks[i].attr].type);

            if (!ii)
            {
                continue;
            }

            e::intrusive_ptr<index_iterator> it = ii->iterator_from_check(snap, ri, idx->id, checks[i], key_ie);

            if (it)
            {
                iterators.push_back(it);
            }
        }
    }

    // figure out the cost of accessing all objects
    e::intrusive_ptr<index_iterator> full_scan;
    full_scan = key_ii->iterator_for_keys(snap, ri);
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
                               const schema& sc,
                               iterator* iter,
                               e::slice* key,
                               std::vector<e::slice>* value,
                               uint64_t* version,
                               reference* ref)
{
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

namespace
{

uint64_t
roundup_version(uint64_t v)
{
    const static uint64_t REGION_PERIODIC = hyperdex::datalayer::REGION_PERIODIC;
    v += REGION_PERIODIC;
    v  = v & ~(REGION_PERIODIC - 1);
    return v;
}

}

bool
datalayer :: write_version(const region_id& ri,
                           uint64_t version,
                           leveldb::WriteBatch* updates)
{
    version = roundup_version(version);
    uint64_t* current = NULL;

    if (!m_versions.mod(ri, &current) ||
        e::atomic::load_64_nobarrier(current) >= version)
    {
        return false;
    }

    char vbacking[VERSION_BUF_SIZE];
    encode_version(ri, version, vbacking);
    updates->Put(leveldb::Slice(vbacking, VERSION_BUF_SIZE), leveldb::Slice());
    return true;
}

void
datalayer :: update_memory_version(const region_id& ri, uint64_t version)
{
    version = roundup_version(version);
    uint64_t* current = NULL;
    m_versions.mod(ri, &current);

    if (current == NULL)
    {
        return;
    }

    uint64_t expected = e::atomic::load_64_nobarrier(current);
    uint64_t witness  = 0;

    while ((witness = e::atomic::compare_and_swap_64_nobarrier(current, expected, version)) < version)
    {
        expected = witness;
    }
}

void
datalayer :: bump_version(const region_id& ri, uint64_t version)
{
    leveldb::WriteBatch updates;

    if (!write_version(ri, version, &updates))
    {
        return;
    }

    // Perform the write
    leveldb::WriteOptions opts;
    opts.sync = false;
    leveldb::Status st = m_db->Write(opts, &updates);

    if (!st.ok())
    {
        handle_error(st);
    }

    update_memory_version(ri, version);
}

uint64_t
datalayer :: max_version(const region_id& ri)
{
    uint64_t val;

    if (!m_versions.get(ri, &val))
    {
        val = disk_version(ri);
    }

    return val;
}

uint64_t
datalayer :: disk_version(const region_id& ri)
{
    leveldb::ReadOptions opts;
    opts.fill_cache = false;
    opts.verify_checksums = true;
    opts.snapshot = NULL;
    std::auto_ptr<leveldb::Iterator> it(m_db->NewIterator(opts));
    char vbacking[VERSION_BUF_SIZE];
    encode_version(ri, UINT64_MAX, vbacking);
    leveldb::Slice key(vbacking, VERSION_BUF_SIZE);
    it->Seek(key);

    if (!it->Valid())
    {
        // XXX err
        return 0;
    }

    key = it->key();
    region_id tmp_ri;
    uint64_t tmp_version;
    datalayer::returncode rc = decode_version(e::slice(key.data(), key.size()),
                                              &tmp_ri, &tmp_version);

    if (rc != SUCCESS)
    {
        // XXX err
        return 0;
    }

    if (tmp_ri != ri)
    {
        return 0;
    }

    return tmp_version;
}

datalayer::returncode
datalayer :: create_checkpoint(const region_timestamp& rt)
{
    m_wiper->inhibit_wiping();
    e::guard g = e::makeobjguard(*m_wiper, &wiper_thread::permit_wiping);
    g.use_variable();

    if (m_wiper->region_will_be_wiped(rt.rid))
    {
        return SUCCESS;
    }

    char cbacking[CHECKPOINT_BUF_SIZE];
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
datalayer :: set_checkpoint_gc(uint64_t checkpoint_gc)
{
    m_checkpointer->set_checkpoint_gc(checkpoint_gc);
}

void
datalayer :: largest_checkpoint_for(const region_id& ri, uint64_t* checkpoint)
{
    m_wiper->inhibit_wiping();
    e::guard g = e::makeobjguard(*m_wiper, &wiper_thread::permit_wiping);
    g.use_variable();

    if (m_wiper->region_will_be_wiped(ri))
    {
        *checkpoint = 0;
        return;
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

bool
datalayer :: region_will_be_wiped(region_id rid)
{
    return m_wiper->region_will_be_wiped(rid);
}

void
datalayer :: request_wipe(const transfer_id& xid,
                          const region_id& ri)
{
    m_wiper->request_wipe(xid, ri);
}

void
datalayer :: inhibit_wiping()
{
    m_wiper->inhibit_wiping();
}

void
datalayer :: permit_wiping()
{
    m_wiper->permit_wiping();
}

datalayer::replay_iterator*
datalayer :: replay_region_from_checkpoint(const region_id& ri,
                                           uint64_t checkpoint,
                                           bool* wipe)
{
    m_wiper->inhibit_wiping();
    e::guard g1 = e::makeobjguard(*m_wiper, &wiper_thread::permit_wiping);
    g1.use_variable();
    m_checkpointer->inhibit_gc();
    e::guard g2 = e::makeobjguard(*m_checkpointer, &checkpointer_thread::permit_gc);
    g2.use_variable();

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

    assert(!m_wiper->region_will_be_wiped(ri));
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
    return new replay_iterator(ri, ptr, index_encoding::lookup(sc.attrs[0].type));
}

void
datalayer :: create_index_marker(const region_id& ri, const index_id& ii)
{
    char buf[sizeof(uint8_t) + 2 * VARINT_64_MAX_SIZE];
    char* ptr = buf;
    ptr = e::pack8be('I', ptr);
    ptr = e::packvarint64(ri.get(), ptr);
    ptr = e::packvarint64(ii.get(), ptr);
    leveldb::Slice key(buf, ptr - buf);
    leveldb::Slice val;
    leveldb::Status st = m_db->Put(leveldb::WriteOptions(), key, val);

    if (!st.ok())
    {
        LOG(ERROR) << "LevelDB corruption: could not put";
        abort();
    }
}

bool
datalayer :: has_index_marker(const region_id& ri, const index_id& ii)
{
    char buf[sizeof(uint8_t) + 2 * VARINT_64_MAX_SIZE];
    char* ptr = buf;
    ptr = e::pack8be('I', ptr);
    ptr = e::packvarint64(ri.get(), ptr);
    ptr = e::packvarint64(ii.get(), ptr);
    leveldb::Slice key(buf, ptr - buf);
    std::string val;
    leveldb::Status st = m_db->Get(leveldb::ReadOptions(), key, &val);

    if (!st.ok() && !st.IsNotFound())
    {
        LOG(ERROR) << "LevelDB corruption: could not get";
        abort();
    }

    return st.ok();
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

bool
datalayer :: upgrade_13x_to_14()
{
    leveldb::WriteOptions wopts;
    wopts.sync = true;

    // first scan every key that starts with "a", making a map from region id to max seqid.
    std::map<region_id, uint64_t> seq_ids;
    leveldb::ReadOptions ropts;
    ropts.fill_cache = false;
    ropts.verify_checksums = true;
    ropts.snapshot = NULL;
    std::auto_ptr<leveldb::Iterator> it(m_db->NewIterator(ropts));
    it->Seek(leveldb::Slice("a", 1));

    while (it->Valid())
    {
        if (it->key().size() != sizeof(uint64_t) * 3 + 1 ||
            it->key().data()[0] != 'a')
        {
            break;
        }

        const char* ptr = it->key().data();
        uint64_t reg_id;
        uint64_t seq_id;
        uint64_t ri;
        e::unpack64be(ptr + 1, &reg_id);
        e::unpack64be(ptr + 9, &seq_id);
        e::unpack64be(ptr + 17, &ri);

        if (reg_id == ri)
        {
            seq_id = std::max(seq_ids[region_id(ri)], UINT64_MAX - seq_id);
            seq_ids[region_id(ri)] = seq_id;
        }

        it->Next();
    }

    if (!it->status().ok())
    {
        LOG(ERROR) << "could not upgrade to 1.4: " << it->status().ToString();
        return false;
    }

    // now write out versions for the regions that we saw
    for (std::map<region_id, uint64_t>::iterator s = seq_ids.begin();
            s != seq_ids.end(); ++s)
    {
        char vbacking[VERSION_BUF_SIZE];
        uint64_t v = std::max(s->second, disk_version(s->first));
        encode_version(s->first, roundup_version(v), vbacking);
        leveldb::Status st = m_db->Put(wopts, leveldb::Slice(vbacking, VERSION_BUF_SIZE), leveldb::Slice());

        if (!st.ok())
        {
            LOG(ERROR) << "could not upgrade to 1.4: " << st.ToString();
            return false;
        }
    }

    // only after those are on disk should we delete the original keys
    it->Seek(leveldb::Slice("a", 1));

    while (it->Valid())
    {
        if (it->key().size() == sizeof(uint64_t) * 3 + 1 ||
            it->key().data()[0] != 'a')
        {
            break;
        }

        leveldb::Status st = m_db->Delete(wopts, it->key());

        if (!st.ok())
        {
            LOG(ERROR) << "could not upgrade to 1.4: " << it->status().ToString();
            return false;
        }

        it->Next();
    }

    if (!it->status().ok())
    {
        LOG(ERROR) << "could not upgrade to 1.4: " << it->status().ToString();
        return false;
    }

    // finally mark the new version
    leveldb::Slice k("hyperdex", 8);
    leveldb::Slice v(PACKAGE_VERSION, STRLENOF(PACKAGE_VERSION));
    leveldb::Status st = m_db->Put(wopts, k, v);

    if (!st.ok())
    {
        LOG(ERROR) << "could not upgrade to 1.4: " << st.ToString();
        return false;
    }

    return true;
}

void
datalayer :: find_indices(const region_id& rid, std::vector<const index*>* indices)
{
    std::vector<index_state>::iterator it;
    it = std::lower_bound(m_indices.begin(), m_indices.end(), rid);

    for (; it < m_indices.end() && it->ri == rid; ++it)
    {
        if (!it->is_usable())
        {
            continue;
        }

        const index* idx = m_daemon->m_config.get_index(it->ii);
        assert(idx);
        indices->push_back(idx);
    }
}

void
datalayer :: find_indices(const region_id& rid, uint16_t attr,
                          std::vector<const index*>* indices)
{
    std::vector<index_state>::iterator it;
    it = std::lower_bound(m_indices.begin(), m_indices.end(), rid);

    for (; it < m_indices.end() && it->ri == rid; ++it)
    {
        if (!it->is_usable())
        {
            continue;
        }

        const index* idx = m_daemon->m_config.get_index(it->ii);
        assert(idx);

        if (idx->attr == attr)
        {
            indices->push_back(idx);
        }
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
