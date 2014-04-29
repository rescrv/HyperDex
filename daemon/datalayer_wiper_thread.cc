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
#include "daemon/datalayer_index_state.h"
#include "daemon/datalayer_indexer_thread.h"
#include "daemon/datalayer_wiper_thread.h"

using hyperdex::datalayer;

datalayer :: wiper_thread :: wiper_thread(daemon* d, wiper_indexer_mediator* m)
    : background_thread(d)
    , m_daemon(d)
    , m_mediator(m)
    , m_wiping()
    , m_have_current(false)
    , m_wipe_current_xid()
    , m_wipe_current_rid()
    , m_wiping_inhibit_permit_diff(0)
    , m_interrupted_count(0)
    , m_interrupted(false)
{
}

datalayer :: wiper_thread :: ~wiper_thread() throw ()
{
}

const char*
datalayer :: wiper_thread :: thread_name()
{
    return "wiping";
}

bool
datalayer :: wiper_thread :: have_work()
{
    m_interrupted = false;
    m_mediator->clear_wiper_region();
    m_have_current = false;
    m_wipe_current_xid = transfer_id();
    m_wipe_current_rid = region_id();
    return !m_wiping.empty() && m_wiping_inhibit_permit_diff == 0 &&
           !m_mediator->region_conflicts_with_indexer(m_wiping.front().second);
}

void
datalayer :: wiper_thread :: copy_work()
{
    m_wipe_current_xid = m_wiping.front().first;
    m_wipe_current_rid = m_wiping.front().second;
    m_have_current = m_mediator->set_wiper_region(m_wipe_current_rid);
}

void
datalayer :: wiper_thread :: do_work()
{
    if (!m_have_current)
    {
        return;
    }

    wipe(m_wipe_current_xid, m_wipe_current_rid);
}

void
datalayer :: wiper_thread :: debug_dump()
{
    this->lock();
    LOG(INFO) << "wiper thread ==================================================================";
    LOG(INFO) << "wiping:";

    for (wipe_list_t::iterator it = m_wiping.begin(); it != m_wiping.end(); ++it)
    {
        LOG(INFO) << "  " << it->first << " " << it->second;
    }

    LOG(INFO) << "have_current=" << (m_have_current ? "yes" : "no");
    LOG(INFO) << "wipe_current_xid=" << m_wipe_current_xid;
    LOG(INFO) << "wipe_current_rid=" << m_wipe_current_rid;
    LOG(INFO) << "wiping_inhibit_permit_diff=" << m_wiping_inhibit_permit_diff;
    LOG(INFO) << "interrupted_count=" << m_interrupted_count;
    this->unlock();
}

void
datalayer :: wiper_thread :: inhibit_wiping()
{
    this->lock();
    ++m_wiping_inhibit_permit_diff;
    this->unlock();
}

void
datalayer :: wiper_thread :: permit_wiping()
{
    this->lock();
    assert(m_wiping_inhibit_permit_diff > 0);
    --m_wiping_inhibit_permit_diff;

    if (m_wiping_inhibit_permit_diff == 0)
    {
        this->wakeup();
    }

    this->unlock();
}

bool
datalayer :: wiper_thread :: region_will_be_wiped(region_id rid)
{
    bool found = false;
    this->lock();

    for (wipe_list_t::iterator it = m_wiping.begin();
            !found && it != m_wiping.end(); ++it)
    {
        found = it->second == rid;
    }

    this->unlock();
    return found;
}

void
datalayer :: wiper_thread :: request_wipe(transfer_id xid,
                                                  region_id ri)
{
    this->lock();
    m_wiping.push_back(std::make_pair(xid, ri));
    this->wakeup();
    this->unlock();
}

void
datalayer :: wiper_thread :: kick()
{
    this->lock();
    this->wakeup();
    this->unlock();
}

bool
datalayer :: wiper_thread :: interrupted()
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

void
datalayer :: wiper_thread :: wipe(transfer_id xid, region_id rid)
{
    this->offline();
    wipe_checkpoints(rid);
    wipe_indices(rid);
    wipe_objects(rid);
    this->online();

    if (interrupted())
    {
        return;
    }

    // now mark it as usable
    for (size_t i = 0; i < m_daemon->m_data.m_indices.size(); ++i)
    {
        index_state* is = &m_daemon->m_data.m_indices[i];

        if (is->ri == rid)
        {
            if (!m_daemon->m_data.m_indexer->mark_usable(is->ri, is->ii))
            {
                return;
            }
        }
    }

    this->lock();
    assert(!m_wiping.empty());
    assert(m_wiping.front().first == xid);
    assert(m_wiping.front().second == rid);

    if (m_wiping_inhibit_permit_diff == 0)
    {
        m_wiping.pop_front();
    }

    this->unlock();
    m_daemon->m_data.m_indexer->kick();

    // now report that it was wiped
    m_daemon->m_stm.report_wiped(xid);
}

void
datalayer :: wiper_thread :: wipe_checkpoints(region_id rid)
{
    std::auto_ptr<leveldb::Iterator> it;
    it.reset(m_daemon->m_data.m_db->NewIterator(leveldb::ReadOptions()));
    char cbacking[CHECKPOINT_BUF_SIZE];
    encode_checkpoint(rid, 0, cbacking);
    it->Seek(leveldb::Slice(cbacking, CHECKPOINT_BUF_SIZE));

    while (it->Valid())
    {
        if (interrupted())
        {
            return;
        }

        region_timestamp rt;
        e::slice key(it->key().data(), it->key().size());
        returncode rc = decode_checkpoint(key, &rt.rid, &rt.checkpoint);

        if (rc != datalayer::SUCCESS || rt.rid != rid)
        {
            break;
        }

        m_daemon->m_data.m_db->Delete(leveldb::WriteOptions(), it->key());
        it->Next();
    }
}

void
datalayer :: wiper_thread :: wipe_indices(region_id rid)
{
    wipe_common('I', rid);
    wipe_common('i', rid);
}

void
datalayer :: wiper_thread :: wipe_objects(region_id rid)
{
    wipe_common('o', rid);
}

void
datalayer :: wiper_thread :: wipe_common(uint8_t c, region_id rid)
{
    std::auto_ptr<leveldb::Iterator> it;
    it.reset(m_daemon->m_data.m_db->NewIterator(leveldb::ReadOptions()));
    char backing[sizeof(uint8_t) + VARINT_64_MAX_SIZE];
    char* ptr = backing;
    ptr = e::pack8be(c, ptr);
    ptr = e::packvarint64(rid.get(), ptr);
    leveldb::Slice prefix(backing, ptr - backing);
    it->Seek(prefix);

    while (it->key().starts_with(prefix))
    {
        if (interrupted())
        {
            return;
        }

        m_daemon->m_data.m_db->Delete(leveldb::WriteOptions(), it->key());
        it->Next();
    }
}
