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
#include <sstream>
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
#include "common/range_searches.h"
#include "common/serialization.h"
#include "daemon/daemon.h"
#include "daemon/datalayer.h"
#include "daemon/index_encode.h"
#include "datatypes/apply.h"
#include "datatypes/microerror.h"

#define ACKED_BUF_SIZE (sizeof(uint8_t) + 3 * sizeof(uint64_t))

// ASSUME:  all keys put into leveldb have a first byte without the high bit set

using std::tr1::placeholders::_1;
using hyperdex::datalayer;
using hyperdex::leveldb_snapshot_ptr;
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
                 const region_id& reg_id,
                 uint64_t seq_id,
                 const e::slice& key,
                 const std::vector<e::slice>& value,
                 uint64_t version)
{
    const schema* sc = m_daemon->m_config.get_schema(ri);
    assert(sc);
    leveldb::WriteOptions opts;
    opts.sync = false;
    std::vector<char> kbacking;
    leveldb::Slice lkey;
    encode_key(ri, key, &kbacking, &lkey);
    std::vector<char> vbacking;
    leveldb::Slice lvalue;
    encode_value(value, version, &vbacking, &lvalue);
    std::list<std::vector<char> > backing;
    leveldb::WriteBatch updates;
    updates.Put(lkey, lvalue);
    returncode rc = create_index_changes(sc, ri, key, lkey, value, &backing, &updates);

    if (rc != SUCCESS)
    {
        return rc;
    }

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
        capture_id cid = m_daemon->m_config.capture_for(ri);
        assert(cid != capture_id());
        backing.push_back(std::vector<char>());
        leveldb::Slice tkey;
        encode_transfer(cid, count, &backing.back(), &tkey);
        backing.push_back(std::vector<char>());
        leveldb::Slice tvalue;
        encode_key_value(key, &value, version, &backing.back(), &tvalue);
        updates.Put(tkey, tvalue);
    }

    // Perform the write
    leveldb::Status st = m_db->Write(opts, &updates);

    if (st.ok())
    {
        return SUCCESS;
    }
    else if (st.IsNotFound())
    {
        LOG(ERROR) << "put returned NOT_FOUND at the disk layer: region=" << ri
                   << " key=0x" << key.hex() << " desc=" << st.ToString();
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
                 const region_id& reg_id,
                 uint64_t seq_id,
                 const e::slice& key)
{
    const schema* sc = m_daemon->m_config.get_schema(ri);
    assert(sc);
    leveldb::WriteOptions opts;
    opts.sync = false;
    std::vector<char> kbacking;
    leveldb::Slice lkey;
    encode_key(ri, key, &kbacking, &lkey);
    std::list<std::vector<char> > backing;
    leveldb::WriteBatch updates;
    updates.Delete(lkey);
    returncode rc = create_index_changes(sc, ri, key, lkey, &backing, &updates);

    if (rc != SUCCESS)
    {
        return rc;
    }

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
        capture_id cid = m_daemon->m_config.capture_for(ri);
        assert(cid != capture_id());
        backing.push_back(std::vector<char>());
        leveldb::Slice tkey;
        encode_transfer(cid, count, &backing.back(), &tkey);
        backing.push_back(std::vector<char>());
        leveldb::Slice tvalue;
        encode_key_value(key, NULL, 0, &backing.back(), &tvalue);
        updates.Put(tkey, tvalue);
    }

    // Perform the write
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
datalayer :: make_snapshot(const region_id& ri,
                           const schema& sc,
                           const std::vector<attribute_check>* checks,
                           snapshot* snap,
                           std::ostringstream* ostr)
{
    snap->m_dl = this;
    snap->m_snap.reset(m_db, m_db->GetSnapshot());
    snap->m_checks = checks;
    snap->m_ri = ri;
    snap->m_ostr = ostr;
    std::vector<range> ranges;

    if (!range_searches(*checks, &ranges))
    {
        return BAD_SEARCH;
    }

    if (ostr) *ostr << " converted " << checks->size() << " checks to " << ranges.size() << " ranges\n";

    char* ptr;
    std::vector<leveldb::Range> level_ranges;
    std::vector<bool (datalayer::*)(const leveldb::Slice& in, e::slice* out)> parsers;

    // For each range, setup a leveldb range using encoded values
    for (size_t i = 0; i < ranges.size(); ++i)
    {
        if (ostr) *ostr << " considering attr " << ranges[i].attr << " Range("
                        << ranges[i].start.hex() << ", " << ranges[i].end.hex() << " " << ranges[i].type << " "
                        << (ranges[i].has_start ? "[" : "<") << "-" << (ranges[i].has_end ? "]" : ">")
                        << " " << (ranges[i].invalid ? "invalid" : "valid") << "\n";

        if (ranges[i].attr >= sc.attrs_sz ||
            sc.attrs[ranges[i].attr].type != ranges[i].type)
        {
            return BAD_SEARCH;
        }

        bool (datalayer::*parse)(const leveldb::Slice& in, e::slice* out);

        // XXX sometime in the future we could support efficient range search on
        // keys.  Today is not that day.  Tomorrow doesn't look good either.
        if (ranges[i].attr == 0)
        {
            continue;
        }

        else if (ranges[i].type == HYPERDATATYPE_STRING)
        {
            parse = &datalayer::parse_index_string;
        }
        else if (ranges[i].type == HYPERDATATYPE_INT64)
        {
            parse = &datalayer::parse_index_sizeof8;
        }
        else if (ranges[i].type == HYPERDATATYPE_FLOAT)
        {
            parse = &datalayer::parse_index_sizeof8;
        }
        else
        {
            continue;
        }

        if (ranges[i].has_start)
        {
            snap->m_backing.push_back(std::vector<char>());
            encode_index(ri, ranges[i].attr, ranges[i].type, ranges[i].start, &snap->m_backing.back());
        }
        else
        {
            snap->m_backing.push_back(std::vector<char>());
            encode_index(ri, ranges[i].attr, &snap->m_backing.back());
        }

        leveldb::Slice start(&snap->m_backing.back()[0], snap->m_backing.back().size());

        if (ranges[i].has_end && ranges[i].type != HYPERDATATYPE_STRING)
        {
            snap->m_backing.push_back(std::vector<char>());
            encode_index(ri, ranges[i].attr, ranges[i].type, ranges[i].end, &snap->m_backing.back());
            bump(&snap->m_backing.back());
        }
        else
        {
            snap->m_backing.push_back(std::vector<char>());
            encode_index(ri, ranges[i].attr + 1, &snap->m_backing.back());
        }

        leveldb::Slice end(&snap->m_backing.back()[0], snap->m_backing.back().size());
        level_ranges.push_back(leveldb::Range(start, end));
        parsers.push_back(parse);
    }

    // Add to level_ranges the size of the object range for the region itself
    // excluding indices
    level_ranges.push_back(leveldb::Range());
    snap->m_backing.push_back(std::vector<char>());
    snap->m_backing.back().resize(sizeof(uint8_t) + sizeof(uint64_t));
    ptr = &snap->m_backing.back()[0];
    ptr = e::pack8be('o', ptr);
    ptr = e::pack64be(ri.get(), ptr);
    level_ranges.back().start = leveldb::Slice(&snap->m_backing.back()[0],
                                               snap->m_backing.back().size());
    snap->m_backing.push_back(snap->m_backing.back());
    bump(&snap->m_backing.back());
    level_ranges.back().limit = leveldb::Slice(&snap->m_backing.back()[0],
                                               snap->m_backing.back().size());

    // Fetch from leveldb the approximate space usage of each computed range
    std::vector<uint64_t> sizes(level_ranges.size());
    m_db->GetApproximateSizes(&level_ranges.front(), level_ranges.size(), &sizes.front());

    if (ostr)
    {
        for (size_t i = 0; i < level_ranges.size(); ++i)
        {
            *ostr << " index Range(" << e::slice(level_ranges[i].start.data(), level_ranges[i].start.size()).hex()
                  << ", " << e::slice(level_ranges[i].start.data(), level_ranges[i].start.size()).hex()
                  << " occupies " << sizes[i] << " bytes\n";
        }
    }

    // the size of all objects in the region of the search
    uint64_t object_disk_space = sizes.back();
    if (ostr) *ostr << " objects for " << ri << " occupies " << object_disk_space << " bytes\n";
    leveldb::Range object_range = level_ranges.back();
    level_ranges.pop_back();
    sizes.pop_back();

    // Find the least costly option and store it in idx
    assert(parsers.size() == sizes.size());
    assert(parsers.size() == level_ranges.size());
    size_t idx = 0;

    for (size_t i = 0; i < sizes.size(); ++i)
    {
        if (sizes[i] < sizes[idx])
        {
            idx = i;
        }
    }

    if (sizes.empty() || sizes[idx] > object_disk_space / 4.)
    {
        if (ostr) *ostr << " choosing to just enumerate all objects\n";
        snap->m_range = object_range;
        snap->m_parse = &datalayer::parse_object_key;
    }
    else
    {
        if (ostr) *ostr << " choosing to use index " << idx << "\n";
        snap->m_range = level_ranges[idx];
        snap->m_parse = parsers[idx];
    }

    // Create iterator
    leveldb::ReadOptions opts;
    opts.fill_cache = false;
    opts.verify_checksums = true;
    opts.snapshot = snap->m_snap.get();
    snap->m_iter.reset(snap->m_snap, m_db->NewIterator(opts));
    snap->m_iter->Seek(snap->m_range.start);
    return SUCCESS;
}

leveldb_snapshot_ptr
datalayer :: make_raw_snapshot()
{
    return leveldb_snapshot_ptr(m_db, m_db->GetSnapshot());
}

void
datalayer :: make_region_iterator(region_iterator* riter,
                                  leveldb_snapshot_ptr snap,
                                  const region_id& ri)
{
    riter->m_dl = this;
    riter->m_snap = snap;
    riter->m_region = ri;
    leveldb::ReadOptions opts;
    opts.fill_cache = true;
    opts.verify_checksums = true;
    opts.snapshot = riter->m_snap.get();
    riter->m_iter.reset(riter->m_snap, m_db->NewIterator(opts));
    char backing[sizeof(uint8_t) + sizeof(uint64_t)];
    char* ptr = backing;
    ptr = e::pack8be('o', ptr);
    ptr = e::pack64be(riter->m_region.get(), ptr);
    riter->m_iter->Seek(leveldb::Slice(backing, sizeof(uint8_t) + sizeof(uint64_t)));
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
    std::vector<char> kbacking;
    leveldb::Slice lkey;
    capture_id cid = m_daemon->m_config.capture_for(ri);
    assert(cid != capture_id());
    encode_transfer(cid, seq_no, &kbacking, &lkey);
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
    else if (st.IsCorruption())
    {
        LOG(ERROR) << "corruption at the disk layer: region=" << ri
                   << " seq_no=" << seq_no << " desc=" << st.ToString();
        return CORRUPTION;
    }
    else if (st.IsIOError())
    {
        LOG(ERROR) << "IO error at the disk layer: region=" << ri
                   << " seq_no=" << seq_no << " desc=" << st.ToString();
        return IO_ERROR;
    }
    else
    {
        LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
        return LEVELDB_ERROR;
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

    if (ptr + sizeof(uint16_t) <= end)
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
datalayer :: encode_acked(const region_id& ri, /*region we saw an ack for*/
                          const region_id& reg_id, /*region of the point leader*/
                          uint64_t seq_id,
                          char* buf)
{
    char* ptr = buf;
    ptr = e::pack8be('a', ptr);
    ptr = e::pack64be(reg_id.get(), ptr);
    ptr = e::pack64be(seq_id, ptr);
    ptr = e::pack64be(ri.get(), ptr);
}

datalayer::returncode
datalayer :: decode_acked(const e::slice& key,
                          region_id* ri, /*region we saw an ack for*/
                          region_id* reg_id, /*region of the point leader*/
                          uint64_t* seq_id)
{
    if (key.size() != ACKED_BUF_SIZE)
    {
        return BAD_ENCODING;
    }

    uint8_t _p;
    uint64_t _ri;
    uint64_t _reg_id;
    const uint8_t* ptr = key.data();
    ptr = e::unpack8be(ptr, &_p);
    ptr = e::unpack64be(ptr, &_reg_id);
    ptr = e::unpack64be(ptr, seq_id);
    ptr = e::unpack64be(ptr, &_ri);
    *ri = region_id(_ri);
    *reg_id = region_id(_reg_id);
    return _p == 'a' ? SUCCESS : BAD_ENCODING;
}

void
datalayer :: encode_transfer(const capture_id& ci,
                             uint64_t count,
                             std::vector<char>* backing,
                             leveldb::Slice* tkey)
{
    backing->resize(sizeof(uint8_t) + 2 * sizeof(uint64_t));
    char* tmp = &backing->front();
    tmp = e::pack8be('t', tmp);
    tmp = e::pack64be(ci.get(), tmp);
    tmp = e::pack64be(count, tmp);
    *tkey = leveldb::Slice(&backing->front(), sizeof(uint8_t) + 2 * sizeof(uint64_t));
}

void
datalayer :: encode_key_value(const e::slice& key,
                              const std::vector<e::slice>* value,
                              uint64_t version,
                              std::vector<char>* backing,
                              leveldb::Slice* slice)
{
    size_t sz = sizeof(uint32_t) + key.size() + sizeof(uint64_t) + sizeof(uint16_t);

    for (size_t i = 0; value && i < value->size(); ++i)
    {
        sz += sizeof(uint32_t) + (*value)[i].size();
    }

    backing->resize(sz);
    char* ptr = &backing->front();
    *slice = leveldb::Slice(ptr, sz);
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
datalayer :: decode_key_value(const e::slice& slice,
                              bool* has_value,
                              e::slice* key,
                              std::vector<e::slice>* value,
                              uint64_t* version)
{
    const uint8_t* ptr = slice.data();
    const uint8_t* end = ptr + slice.size();

    if (ptr >= end)
    {
        return BAD_ENCODING;
    }

    uint32_t key_sz;

    if (ptr + sizeof(uint32_t) <= end)
    {
        ptr = e::unpack32be(ptr, &key_sz);
    }
    else
    {
        return BAD_ENCODING;
    }

    if (ptr + key_sz <= end)
    {
        *key = e::slice(ptr, key_sz);
        ptr += key_sz;
    }
    else
    {
        return BAD_ENCODING;
    }

    if (ptr + sizeof(uint64_t) <= end)
    {
        ptr = e::unpack64be(ptr, version);
    }
    else
    {
        return BAD_ENCODING;
    }

    if (ptr == end)
    {
        *has_value = false;
        return SUCCESS;
    }

    uint16_t num_attrs;

    if (ptr + sizeof(uint16_t) <= end)
    {
        ptr = e::unpack16be(ptr, &num_attrs);
    }
    else
    {
        return BAD_ENCODING;
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
            return BAD_ENCODING;
        }

        e::slice s(ptr, sz);
        ptr += sz;
        value->push_back(s);
    }

    *has_value = true;
    return SUCCESS;
}

void
datalayer :: encode_index(const region_id& ri,
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
datalayer :: encode_index(const region_id& ri,
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
datalayer :: encode_index(const region_id& ri,
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
datalayer :: bump(std::vector<char>* backing)
{
    assert(!backing->empty());
    assert((*backing)[0] ^ 0x80);
    index_encode_bump(&backing->front(),
                      &backing->front() + backing->size());
}

bool
datalayer :: parse_index_string(const leveldb::Slice& s, e::slice* k)
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
datalayer :: parse_index_sizeof8(const leveldb::Slice& s, e::slice* k)
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
datalayer :: parse_object_key(const leveldb::Slice& s, e::slice* k)
{
    region_id tmp;
    return decode_key(e::slice(s.data(), s.size()), &tmp, k) == SUCCESS;
}

void
datalayer :: generate_index(const region_id& ri,
                            uint16_t attr,
                            hyperdatatype type,
                            const e::slice& value,
                            const e::slice& key,
                            std::list<std::vector<char> >* backing,
                            std::vector<leveldb::Slice>* idxs)
{
    if (type == HYPERDATATYPE_STRING ||
        type == HYPERDATATYPE_INT64 ||
        type == HYPERDATATYPE_FLOAT)
    {
        backing->push_back(std::vector<char>());
        encode_index(ri, attr, type, value, key, &backing->back());
        idxs->push_back(leveldb::Slice(&backing->back().front(), backing->back().size()));
    }
}

datalayer::returncode
datalayer :: create_index_changes(const schema* sc,
                                  const region_id& ri,
                                  const e::slice& key,
                                  const leveldb::Slice& lkey,
                                  std::list<std::vector<char> >* backing,
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
                                  std::list<std::vector<char> >* backing,
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

            std::vector<char> backing;
            leveldb::Slice slice;
            encode_transfer(capture_id(cid + 1), 0, &backing, &slice);
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
        STRINGIFY(datalayer::BAD_SEARCH);
        STRINGIFY(datalayer::BAD_ENCODING);
        STRINGIFY(datalayer::CORRUPTION);
        STRINGIFY(datalayer::IO_ERROR);
        STRINGIFY(datalayer::LEVELDB_ERROR);
        default:
            lhs << "unknown returncode";
    }

    return lhs;
}

datalayer :: region_iterator :: region_iterator()
    : m_dl()
    , m_snap()
    , m_iter()
    , m_region()
{
}

datalayer :: region_iterator :: ~region_iterator() throw ()
{
}

bool
datalayer :: region_iterator :: valid()
{
    if (!m_iter->Valid())
    {
        return false;
    }

    leveldb::Slice k = m_iter->key();
    uint8_t b;
    uint64_t ri;
    e::unpacker up(k.data(), k.size());
    up = up >> b >> ri;
    return !up.error() && b == 'o' && ri == m_region.get();
}

void
datalayer :: region_iterator :: next()
{
    m_iter->Next();
}

void
datalayer :: region_iterator :: unpack(e::slice* k,
                                       std::vector<e::slice>* val,
                                       uint64_t* ver,
                                       reference* ref)
{
    region_id ri;
    // XXX returncode
    m_dl->decode_key(e::slice(m_iter->key().data(), m_iter->key().size()), &ri, k);
    m_dl->decode_value(e::slice(m_iter->value().data(), m_iter->value().size()), val, ver);
    size_t sz = k->size();

    for (size_t i = 0; i < val->size(); ++i)
    {
        sz += (*val)[i].size();
    }

    std::vector<char> tmp(sz + 1);
    char* ptr = &tmp.front();
    memmove(ptr, k->data(), k->size());
    ptr += k->size();

    for (size_t i = 0; i < val->size(); ++i)
    {
        memmove(ptr, (*val)[i].data(), (*val)[i].size());
        ptr += (*val)[i].size();
    }

    ref->m_backing = std::string(tmp.begin(), tmp.end());
    const char* cptr = ref->m_backing.data();
    *k = e::slice(cptr, k->size());
    cptr += k->size();

    for (size_t i = 0; i < val->size(); ++i)
    {
        (*val)[i] = e::slice(cptr, (*val)[i].size());
        cptr += (*val)[i].size();
    }
}

e::slice
datalayer :: region_iterator :: key()
{
    return e::slice(m_iter->key().data(), m_iter->key().size());
}

datalayer :: snapshot :: snapshot()
    : m_dl()
    , m_snap()
    , m_checks()
    , m_ri()
    , m_backing()
    , m_range()
    , m_parse()
    , m_iter()
    , m_error(SUCCESS)
    , m_version()
    , m_key()
    , m_value()
    , m_ostr()
    , m_num_gets(0)
    , m_ref()
{
}

datalayer :: snapshot :: ~snapshot() throw ()
{
}

bool
datalayer :: snapshot :: valid()
{
    if (m_error != SUCCESS || !m_iter.get() || !m_parse)
    {
        return false;
    }

    // Don't try to optimize by replacing m_ri with a const schema* because it
    // won't persist across reconfigurations
    const schema* sc = m_dl->m_daemon->m_config.get_schema(m_ri);
    assert(sc);

    // while the most selective iterator is valid and not past the end
    while (m_iter->Valid())
    {
        if (m_iter->key().compare(m_range.limit) >= 0)
        {
            if (m_ostr) *m_ostr << " iterator retrieved " << m_num_gets << " objects from disk\n";
            return false;
        }

        (m_dl->*m_parse)(m_iter->key(), &m_key);
        leveldb::ReadOptions opts;
        opts.fill_cache = true;
        opts.verify_checksums = true;
        std::vector<char> kbacking;
        leveldb::Slice lkey;
        m_dl->encode_key(m_ri, m_key, &kbacking, &lkey);

        leveldb::Status st = m_dl->m_db->Get(opts, lkey, &m_ref.m_backing);

        if (st.ok())
        {
            e::slice v(m_ref.m_backing.data(), m_ref.m_backing.size());
            datalayer::returncode rc = m_dl->decode_value(v, &m_value, &m_version);

            if (rc != SUCCESS)
            {
                m_error = rc;
                return false;
            }

            ++m_num_gets;
        }
        else if (st.IsNotFound())
        {
            LOG(ERROR) << "snapshot points to items (" << m_key.hex() << ") not found in the snapshot";
            m_error = CORRUPTION;
            return false;
        }
        else if (st.IsCorruption())
        {
            LOG(ERROR) << "corruption at the disk layer: region=" << m_ri
                       << " key=0x" << m_key.hex() << " desc=" << st.ToString();
            m_error = CORRUPTION;
            return false;
        }
        else if (st.IsIOError())
        {
            LOG(ERROR) << "IO error at the disk layer: region=" << m_ri
                       << " key=0x" << m_key.hex() << " desc=" << st.ToString();
            m_error = IO_ERROR;
            return false;
        }
        else
        {
            LOG(ERROR) << "LevelDB returned an unknown error that we don't know how to handle";
            m_error = LEVELDB_ERROR;
            return false;
        }

        bool passes_checks = true;

        for (size_t i = 0; passes_checks && i < m_checks->size(); ++i)
        {
            if ((*m_checks)[i].attr >= sc->attrs_sz)
            {
                passes_checks = false;
            }
            else if ((*m_checks)[i].attr == 0)
            {
                microerror e;
                passes_checks = passes_attribute_check(sc->attrs[0].type, (*m_checks)[i], m_key, &e);
            }
            else
            {
                hyperdatatype type = sc->attrs[(*m_checks)[i].attr].type;
                microerror e;
                passes_checks = passes_attribute_check(type, (*m_checks)[i], m_value[(*m_checks)[i].attr - 1], &e);
            }
        }

        if (passes_checks)
        {
            return true;
        }
        else
        {
            m_iter->Next();
        }
    }

    return false;
}

void
datalayer :: snapshot :: next()
{
    assert(m_error == SUCCESS);
    m_iter->Next();
}

void
datalayer :: snapshot :: unpack(e::slice* key, std::vector<e::slice>* val, uint64_t* ver)
{
    *key = m_key;
    *val = m_value;
    *ver = m_version;
}

void
datalayer :: snapshot :: unpack(e::slice* key, std::vector<e::slice>* val, uint64_t* ver, reference* ref)
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
