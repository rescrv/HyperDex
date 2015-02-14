// Copyright (c) 2014, Cornell University
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

// Google Log
#include <glog/logging.h>

// e
#include <e/endian.h>
#include <e/varint.h>

// HyperDex
#include "daemon/daemon.h"
#include "daemon/datalayer_checkpointer_thread.h"
#include "daemon/datalayer_encodings.h"
#include "daemon/datalayer_index_state.h"
#include "daemon/datalayer_indexer_thread.h"
#include "daemon/datalayer_wiper_thread.h"

using hyperdex::datalayer;

datalayer :: indexer_thread :: indexer_thread(daemon* d, wiper_indexer_mediator* m)
    : background_thread(d)
    , m_daemon(d)
    , m_mediator(m)
    , m_config()
    , m_have_current(false)
    , m_current_region()
    , m_current_index()
    , m_interrupted_count(0)
    , m_interrupted(false)
{
}

datalayer :: indexer_thread :: ~indexer_thread() throw ()
{
}

const char*
datalayer :: indexer_thread :: thread_name()
{
    return "indexing";
}

bool
datalayer :: indexer_thread :: have_work()
{
    m_interrupted = false;
    m_mediator->clear_indexer_region();
    m_have_current = false;
    m_current_region = region_id();
    m_current_index = index_id();

    for (size_t i = 0; i < m_daemon->m_data.m_indices.size(); ++i)
    {
        index_state* is = &m_daemon->m_data.m_indices[i];

        // if it's not usable (we have to index it first), and it's not
        // currently being wiped, and it's something that we've been mapped to,
        // then we have work to do
        if (!is->is_usable() &&
            !m_mediator->region_conflicts_with_wiper(is->ri) &&
            m_daemon->m_config.get_virtual(is->ri, m_daemon->m_us) != virtual_server_id())
        {
            return true;
        }
    }

    return false;
}

void
datalayer :: indexer_thread :: copy_work()
{
    for (size_t i = 0; i < m_daemon->m_data.m_indices.size(); ++i)
    {
        index_state* is = &m_daemon->m_data.m_indices[i];

        // if it's not usable (we have to index it first), and it's not
        // currently being wiped, and it's something that we've been mapped to,
        // then we have work to do
        if (!is->is_usable() &&
            m_daemon->m_config.get_virtual(is->ri, m_daemon->m_us) != virtual_server_id() &&
            m_mediator->set_indexer_region(is->ri))
        {
            m_config = m_daemon->m_config;
            m_have_current   = true;
            m_current_region = is->ri;
            m_current_index  = is->ii;
            return;
        }
    }
}

void
datalayer :: indexer_thread :: do_work()
{
    if (!m_have_current)
    {
        return;
    }

    if (!wipe(m_current_region, m_current_index))
    {
        return;
    }

    configuration config = m_config;
    leveldb_db_ptr db = m_daemon->m_data.m_db;
    const schema* sc = config.get_schema(m_current_region);
    const index* idx = config.get_index(m_current_index);
    assert(idx);
    std::vector<const index*> idxs(1, idx);

    // prohibit garbage collection during this section.
    // this ensures that the timestamp we take will remain valid until the end
    m_daemon->m_data.m_checkpointer->inhibit_gc();
    e::guard g1 = e::makeobjguard(*m_daemon->m_data.m_checkpointer,
                                  &checkpointer_thread::permit_gc);
    g1.use_variable();

    // Take a timestamp from the data layer.  We could just use a replay
    // iterator for "all", but we wouldn't know which objects we indexed twice.
    // By taking an iterator and then a replay iterator from the timestamp, we
    // ensure that the majority of data gets indexed using "put" ops instead of
    // the "get/put" pattern necessary to remove existing indices before adding.
    std::string timestamp;
    db->GetReplayTimestamp(&timestamp);

    // Take this offline
    this->offline();
    e::guard g2 = e::makeobjguard(*this, &indexer_thread::online);
    g2.use_variable();

    // Now iterate over the data once.
    std::auto_ptr<region_iterator> it(play(m_current_region, sc));

    if (!it.get())
    {
        return;
    }

    while (it->valid())
    {
        if (!index_from_iterator(it.get(), sc, m_current_region, idxs))
        {
            return;
        }

        it->next();
    }

    // Now do it again from the checkpoint we took.
    std::auto_ptr<replay_iterator> rit(replay(m_current_region, timestamp));

    if (!rit.get())
    {
        return;
    }

    while (rit->valid())
    {
        if (!index_from_replay_iterator(rit.get(), sc, m_current_region, idxs))
        {
            return;
        }

        rit->next();
    }

    // Pause writes so we can hit the end of the iterator.
    m_daemon->pause();
    e::guard g3 = e::makeobjguard(*m_daemon, &daemon::unpause);
    g3.use_variable();

    // Keep iterating on the replay iterator
    while (rit->valid())
    {
        if (!index_from_replay_iterator(rit.get(), sc, m_current_region, idxs))
        {
            return;
        }

        rit->next();
    }

    // make the index usable to all
    if (!mark_usable(m_current_region, m_current_index))
    {
        return;
    }

    // Unpause writes
    g3.dismiss();
    m_daemon->unpause();

    // Let the index possibly do its thing
    m_daemon->m_data.m_wiper->kick();
}

void
datalayer :: indexer_thread :: debug_dump()
{
    this->lock();
    LOG(INFO) << "indexer thread ================================================================";
    LOG(INFO) << "have_current=" << (m_have_current ? "yes" : "no");
    LOG(INFO) << "current_region=" << m_current_region;
    LOG(INFO) << "current_index=" << m_current_index;
    LOG(INFO) << "interrupted_count=" << m_interrupted_count;
    this->unlock();
}

void
datalayer :: indexer_thread :: kick()
{
    this->lock();
    this->wakeup();
    this->unlock();
}

bool
datalayer :: indexer_thread :: mark_usable(const region_id& ri, const index_id& ii)
{
    // Publish the index
    char buf[sizeof(uint8_t) + 2 * VARINT_64_MAX_SIZE];
    char* ptr = buf;
    ptr = e::pack8be('I', ptr);
    ptr = e::packvarint64(ri.get(), ptr);
    ptr = e::packvarint64(ii.get(), ptr);
    leveldb::WriteOptions wo;
    leveldb::Slice key(buf, ptr - buf);
    leveldb::Slice val;
    leveldb::Status st = m_daemon->m_data.m_db->Put(wo, key, val);

    if (!st.ok())
    {
        LOG(ERROR) << "error indexing: write failed: " << st.ToString();
        return false;
    }

    // Set the index to be usable
    for (size_t i = 0; i < m_daemon->m_data.m_indices.size(); ++i)
    {
        index_state* is = &m_daemon->m_data.m_indices[i];

        if (is->ri == ri &&
            is->ii == ii)
        {
            is->set_usable();
        }
    }

    return true;
}

bool
datalayer :: indexer_thread :: interrupted()
{
    ++m_interrupted_count;
    bool ret = m_interrupted;

    if (m_interrupted_count % 1000 == 0)
    {
        this->lock();
        ret = this->is_shutdown();
        m_interrupted = ret;
        this->unlock();
    }

    return ret;
}

datalayer::region_iterator*
datalayer :: indexer_thread :: play(const region_id& ri, const schema* sc)
{
    leveldb_db_ptr db = m_daemon->m_data.m_db;
    leveldb_iterator_ptr iip;
    leveldb::ReadOptions ro;
    ro.fill_cache = false;
    iip.reset(leveldb_snapshot_ptr(db, NULL), db->NewIterator(ro));
    const index_encoding* ie = index_encoding::lookup(sc->attrs[0].type);
    return new region_iterator(iip, ri, ie);
}

datalayer::replay_iterator*
datalayer :: indexer_thread :: replay(const region_id& ri, const std::string& timestamp)
{
    leveldb::ReplayIterator* riip;
    leveldb::Status st = m_daemon->m_data.m_db->GetReplayIterator(timestamp, &riip);

    if (!st.ok())
    {
        LOG(ERROR) << "error indexing: LevelDB corruption: invalid timestamp";
        return NULL;
    }

    leveldb_replay_iterator_ptr ptr(m_daemon->m_data.m_db, riip);
    const schema& sc(*m_daemon->m_config.get_schema(ri));
    return new replay_iterator(ri, ptr, index_encoding::lookup(sc.attrs[0].type));
}

bool
datalayer :: indexer_thread :: wipe(const region_id& ri, const index_id& ii)
{
    return wipe_common('I', ri, ii) && wipe_common('i', ri, ii);
}

bool
datalayer :: indexer_thread :: wipe_common(uint8_t c, const region_id& ri, const index_id& ii)
{
    std::auto_ptr<leveldb::Iterator> it;
    it.reset(m_daemon->m_data.m_db->NewIterator(leveldb::ReadOptions()));
    char backing[sizeof(uint8_t) + 2 * VARINT_64_MAX_SIZE];
    char* ptr = backing;
    ptr = e::pack8be(c, ptr);
    ptr = e::packvarint64(ri.get(), ptr);
    ptr = e::packvarint64(ii.get(), ptr);
    leveldb::Slice prefix(backing, ptr - backing);
    it->Seek(prefix);

    while (it->key().starts_with(prefix))
    {
        if (interrupted())
        {
            return false;
        }

        m_daemon->m_data.m_db->Delete(leveldb::WriteOptions(), it->key());
        it->Next();
    }

    return true;
}

bool
datalayer :: indexer_thread :: index_from_iterator(region_iterator* it,
                                                   const schema* sc,
                                                   const region_id& ri,
                                                   const std::vector<const index*>& idxs)
{
    if (interrupted())
    {
        return false;
    }

    e::slice key;
    std::vector<e::slice> value;
    uint64_t version;
    datalayer::reference ref;
    datalayer::returncode rc;
    rc = m_daemon->m_data.get_from_iterator(ri, *sc, it,
            &key, &value, &version, &ref);

    if (rc != SUCCESS)
    {
        LOG(ERROR) << "error indexing: " << rc;
        return false;
    }

    leveldb::WriteBatch batch;
    create_index_changes(*sc, ri, idxs, key, NULL, &value, &batch);

    leveldb::WriteOptions opts;
    opts.sync = false;
    leveldb::Status st = m_daemon->m_data.m_db->Write(opts, &batch);

    if (!st.ok())
    {
        rc = m_daemon->m_data.handle_error(st);
        LOG(ERROR) << "error indexing: " << rc;
        return false;
    }

    return true;
}

bool
datalayer :: indexer_thread :: index_from_replay_iterator(replay_iterator* rit,
                                                          const schema* sc,
                                                          const region_id& ri,
                                                          const std::vector<const index*>& idxs)
{
    if (interrupted())
    {
        return false;
    }

    e::slice key = rit->key();
    std::vector<e::slice> _value;
    uint64_t version;
    datalayer::reference ref1;
    datalayer::returncode rc;
    std::vector<e::slice>* old_value = NULL;
    std::vector<e::slice>* new_value = NULL;

    if (rit->has_value())
    {
        rc = rit->unpack_value(&_value, &version, &ref1);

        if (rc != SUCCESS)
        {
            LOG(ERROR) << "error indexing: " << rc;
            return false;
        }

        new_value = &_value;
    }
    else
    {
        new_value = NULL;
    }

    // perform the read
    std::vector<char> scratch;
    leveldb::Slice lkey;
    encode_key(ri, sc->attrs[0].type, key, &scratch, &lkey);
    std::string ref2;
    leveldb::ReadOptions opts;
    opts.verify_checksums = true;
    leveldb::Status st = m_daemon->m_data.m_db->Get(opts, lkey, &ref2);
    std::vector<e::slice> _old_value;

    if (st.ok())
    {
        uint64_t old_version;
        rc = decode_value(e::slice(ref2.data(), ref2.size()),
                          &_old_value, &old_version);

        if (rc != SUCCESS)
        {
            rc = m_daemon->m_data.handle_error(st);
            LOG(ERROR) << "error indexing: " << rc;
            return false;
        }

        if (_old_value.size() + 1 != sc->attrs_sz)
        {
            LOG(ERROR) << "error indexing: " << BAD_ENCODING;
            return false;
        }

        old_value = &_old_value;
    }
    else if (st.IsNotFound())
    {
        old_value = NULL;
    }
    else
    {
        rc = m_daemon->m_data.handle_error(st);
        LOG(ERROR) << "error indexing: " << rc;
        return false;
    }

    leveldb::WriteBatch batch;
    create_index_changes(*sc, ri, idxs, key, old_value, new_value, &batch);
    leveldb::WriteOptions wopts;
    wopts.sync = false;
    st = m_daemon->m_data.m_db->Write(wopts, &batch);

    if (!st.ok())
    {
        rc = m_daemon->m_data.handle_error(st);
        LOG(ERROR) << "error indexing: " << rc;
        return false;
    }

    return true;
}
