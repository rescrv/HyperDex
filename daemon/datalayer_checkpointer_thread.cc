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

// HyperDex
#include "daemon/daemon.h"
#include "daemon/datalayer.h"
#include "daemon/datalayer_checkpointer_thread.h"
#include "daemon/datalayer_encodings.h"

using hyperdex::datalayer;

datalayer :: checkpointer_thread :: checkpointer_thread(daemon* d)
    : background_thread(d)
    , m_daemon(d)
    , m_checkpoint_gc(0)
    , m_checkpoint_gced(0)
    , m_checkpoint_target(0)
    , m_gc_inhibit_permit_diff(0)
{
}

datalayer :: checkpointer_thread :: ~checkpointer_thread() throw ()
{
}

const char*
datalayer :: checkpointer_thread :: thread_name()
{
    return "checkpointer";
}

bool
datalayer :: checkpointer_thread :: have_work()
{
    return m_checkpoint_gced < m_checkpoint_gc &&
           m_gc_inhibit_permit_diff == 0;
}

void
datalayer :: checkpointer_thread :: copy_work()
{
    m_checkpoint_target = m_checkpoint_gc;
}

void
datalayer :: checkpointer_thread :: do_work()
{
    collect_lower_checkpoints(m_checkpoint_target);
}

void
datalayer :: checkpointer_thread :: debug_dump()
{
    this->lock();
    LOG(INFO) << "checkpointer thread ===========================================================";
    LOG(INFO) << "checkpoint_gc=" << m_checkpoint_gc;
    LOG(INFO) << "checkpoint_gced=" << m_checkpoint_gced;
    LOG(INFO) << "checkpoint_target=" << m_checkpoint_target;
    LOG(INFO) << "gc_inhibit_permit_diff=" << m_gc_inhibit_permit_diff;
    this->unlock();
}

void
datalayer :: checkpointer_thread :: inhibit_gc()
{
    this->lock();
    ++m_gc_inhibit_permit_diff;
    this->unlock();
}

void
datalayer :: checkpointer_thread :: permit_gc()
{
    this->lock();
    assert(m_gc_inhibit_permit_diff > 0);
    --m_gc_inhibit_permit_diff;

    if (m_gc_inhibit_permit_diff == 0)
    {
        this->wakeup();
    }

    this->unlock();
}

void
datalayer :: checkpointer_thread :: set_checkpoint_gc(uint64_t checkpoint_gc)
{
    this->lock();
    m_checkpoint_gc = std::max(checkpoint_gc, m_checkpoint_gc);
    this->wakeup();
    this->unlock();
}

void
datalayer :: checkpointer_thread :: collect_lower_checkpoints(uint64_t checkpoint_gc)
{
    leveldb::ReadOptions opts;
    opts.verify_checksums = true;
    std::auto_ptr<leveldb::Iterator> it;
    it.reset(m_daemon->m_data.m_db->NewIterator(opts));
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
            m_daemon->m_data.m_db->ValidateTimestamp(rt.local_timestamp))
        {
            if (m_daemon->m_data.m_db->CompareTimestamps(rt.local_timestamp, lower_bound_timestamp) < 0)
            {
                lower_bound_timestamp = rt.local_timestamp;
            }

            it->Next();
            continue;
        }

        leveldb::WriteOptions wopts;
        wopts.sync = false;
        leveldb::Status st = m_daemon->m_data.m_db->Delete(wopts, it->key());

        if (!st.ok())
        {
            break;
        }

        it->Next();
    }

    this->lock();

    if (m_gc_inhibit_permit_diff == 0)
    {
        m_daemon->m_data.m_db->AllowGarbageCollectBeforeTimestamp(lower_bound_timestamp);
        m_checkpoint_gced = m_checkpoint_target;
    }

    this->unlock();
}
