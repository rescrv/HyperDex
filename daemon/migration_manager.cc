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

// POSIX
#include <signal.h>

// STL
#include <algorithm>

// Google Log
#include <glog/logging.h>

// HyperDex
#include "common/serialization.h"
#include "daemon/daemon.h"
#include "daemon/datalayer_iterator.h"
#include "daemon/migration_manager.h"
#include "daemon/migration_manager_pending.h"
#include "daemon/migration_out_state.h"
#include "daemon/leveldb.h"

using hyperdex::reconfigure_returncode;
using hyperdex::migration_manager;
using hyperdex::migration_id;
using hyperdex::region_id;

migration_manager :: migration_manager(daemon* d)
    : m_daemon(d)
    , m_migrations_out()
    , m_kickstarter(std::tr1::bind(&migration_manager::kickstarter, this))
    , m_block_kickstarter()
    , m_wakeup_kickstarter(&m_block_kickstarter)
    , m_wakeup_reconfigurer(&m_block_kickstarter)
    , m_need_kickstart(false)
    , m_shutdown(true)
    , m_need_pause(false)
    , m_paused(false)
{
}

migration_manager :: ~migration_manager() throw ()
{
    shutdown();
}

bool
migration_manager :: setup()
{
    po6::threads::mutex::hold hold(&m_block_kickstarter);
    m_kickstarter.start();
    m_shutdown = false;
    return true;
}

void
migration_manager :: teardown()
{
    shutdown();
    m_migrations_out.clear();
}

void
migration_manager :: pause()
{
    po6::threads::mutex::hold hold(&m_block_kickstarter);
    assert(!m_need_pause);
    m_need_pause = true;
}

void
migration_manager :: unpause()
{
    po6::threads::mutex::hold hold(&m_block_kickstarter);
    assert(m_need_pause);
    m_wakeup_kickstarter.broadcast();
    m_need_pause = false;
    m_need_kickstart = true;
}

void
migration_manager :: reconfigure(const configuration&,
                                 const configuration& new_config,
                                 const server_id& sid)
{
    LOG(INFO) << "reconfiguring migration_manager";
    {
        po6::threads::mutex::hold hold(&m_block_kickstarter);
        assert(m_need_pause);

        while (!m_paused)
        {
            m_wakeup_reconfigurer.wait();
        }
    }

    std::vector<migration> migrations;
    new_config.migrations_out(sid, &migrations);
    std::sort(migrations.begin(), migrations.end());
    LOG(INFO) << "We are getting " << migrations.size() << " migration objects.";
    setup_migration_state(migrations, &m_migrations_out);
    LOG(INFO) << "and " << m_migrations_out.size() << " migration_out objects.";

    // std::vector<region_id> regions;
    // new_config.mapped_regions(sid, &regions);

    // std::vector<hyperdex::migration>::iterator m_iter;
    // for (m_iter = migrations_out.begin(); m_iter != migrations_out.end(); m_iter++) {
    //     std::vector<hyperdex::region_id>::iterator r_iter;
    //     for (r_iter = regions.begin(); r_iter != regions.end(); r_iter++) {
    //         region_id rid = (*r_iter);
    //         if (new_config.space_of(rid) == (*m_iter).space_from) {
    //             e::intrusive_ptr<migration_out_state> ptr(new migration_out_state((*m_iter).space_to, rid));
    //             m_migrations_out.push_back(ptr);
    //         }
    //     }
    // }
}

void
migration_manager :: setup_migration_state(const std::vector<hyperdex::migration> migrations,
                      std::vector<e::intrusive_ptr<migration_out_state> >* migration_states)
{
    std::vector<e::intrusive_ptr<migration_out_state> > tmp;
    // In reality, tmp probably will store way more elements than
    // migrations, since one migration will likely correspond to
    // many migration out states.
    tmp.reserve(migrations.size());
    size_t m_idx = 0;
    size_t ms_idx = 0;

    std::vector<region_id> regions;
    m_daemon->m_config.mapped_regions(m_daemon->m_us, &regions);

    leveldb_snapshot_ptr snapshot_ptr = m_daemon->m_data.make_snapshot();

    while (m_idx < migrations.size() && ms_idx < migration_states->size())
    {
        if (migrations[m_idx].id == (*migration_states)[ms_idx]->mid)
        {
            tmp.push_back((*migration_states)[ms_idx]);
            // TODO: commenting out the following line seems to fix the
            // bug, but check the correctness of this method again.
            // ++m_idx;
            ++ms_idx;
        }
        else if (migrations[m_idx].id < (*migration_states)[ms_idx]->mid)
        {
            LOG(INFO) << "initiating migration out state " << migrations[m_idx];

            std::vector<hyperdex::region_id>::iterator r_iter;
            for (r_iter = regions.begin(); r_iter != regions.end(); r_iter++) {
                region_id rid = (*r_iter);
                if (m_daemon->m_config.space_of(rid) == migrations[m_idx].space_from) {
                    datalayer::returncode err;
                    std::auto_ptr<datalayer::iterator> iter;
                    iter.reset(m_daemon->m_data.make_region_iterator(snapshot_ptr, rid, &err));
                    if (err != datalayer::SUCCESS) {
                        LOG(ERROR) << "failed to create region iterator";
                        continue;  // TODO: should we continue?
                    }
                    e::intrusive_ptr<migration_out_state> ptr(
                        new migration_out_state(migrations[m_idx].id,
                                                migrations[m_idx].space_to,
                                                rid,
                                                iter));
                    tmp.push_back(ptr);
                }
            }
            ++m_idx;
        }
        else if (migrations[m_idx].id > (*migration_states)[ms_idx]->mid)
        {
            LOG(INFO) << "ending migration out state " << (*migration_states)[ms_idx]->mid;
            ++ms_idx;
        }
    }

    while (m_idx < migrations.size())
    {
        LOG(INFO) << "initiating migration out state " << migrations[m_idx];

        std::vector<hyperdex::region_id>::iterator r_iter;
        for (r_iter = regions.begin(); r_iter != regions.end(); r_iter++) {
            region_id rid = (*r_iter);
            if (m_daemon->m_config.space_of(rid) == migrations[m_idx].space_from) {
                datalayer::returncode err;
                std::auto_ptr<datalayer::iterator> iter;
                iter.reset(m_daemon->m_data.make_region_iterator(snapshot_ptr, rid, &err));
                if (err != datalayer::SUCCESS) {
                    LOG(ERROR) << "failed to create region iterator";
                    continue;  // TODO: should we continue?
                }
                e::intrusive_ptr<migration_out_state> ptr(
                    new migration_out_state(migrations[m_idx].id,
                                            migrations[m_idx].space_to,
                                            rid,
                                            iter));
                tmp.push_back(ptr);
            }
        }
        ++m_idx;
    }
    
    while (ms_idx < migration_states->size())
    {
        ++ms_idx;
    }

    tmp.swap(*migration_states);
}

void
migration_manager :: migrate_more_state(migration_out_state* mos)
{
    assert(mos->iter.get());

    while (mos->window.size() < mos->window_sz && mos->iter->valid())
    {
        e::intrusive_ptr<pending> op(new pending());
        op->rid = mos->rid;
        op->seq_no = mos->next_seq_no;
        ++mos->next_seq_no;

        // TODO: can an object has no value?
        if (m_daemon->m_data.get_from_iterator(mos->rid, mos->iter.get(), &op->key, &op->value, &op->version, &op->vref) != datalayer::SUCCESS)
        {
            LOG(ERROR) << "error unpacking value during migration";
            break;
        }

        mos->window.push_back(op);
        send_object(mos, op.get());
        mos->iter->next();
    }

    if (mos->window.empty()) {
        m_daemon->m_coord.migration_complete(mos->mid, mos->rid);
    }
}

void
migration_manager :: retransmit(migration_out_state* mos)
{
    for (std::list<e::intrusive_ptr<pending> >::iterator it = mos->window.begin();
            it != mos->window.end(); ++it)
    {
        send_object(mos, it->get());
    }
}

void
migration_manager :: send_object(migration_out_state* mos, pending* op)
{
    virtual_server_id to = m_daemon->m_config.point_leader(mos->sid, op->key);

    const schema* sc = m_daemon->m_config.get_schema(op->rid);
    if (sc == NULL) {
        // TODO: this happens occationally.  Not sure why.  Possibly because the
        // related space has been destroyed?
        LOG(INFO) << "trying to send an object whose region no longer exists.";
        return;
    }
    std::vector<funcall> funcs;
    std::vector<attribute_check> checks;
    funcs.reserve(op->value.size());

    for (size_t j = 1; j <= op->value.size(); ++j)
    {
        hyperdatatype datatype = sc->attrs[j].type;

        funcall o;
        o.attr = j;
        o.name = FUNC_SET;
        o.arg1 = op->value[j - 1];
        o.arg1_datatype = datatype;
        funcs.push_back(o);
    }

    size_t sz = HYPERDEX_HEADER_SIZE_SV
              + pack_size(op->key)
              + sizeof(uint8_t)
              + pack_size(checks)
              + pack_size(funcs)
              + sizeof(region_id)
              + sizeof(uint64_t); // seq_no
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    uint8_t flags = (0 | 0 | 128);
    msg->pack_at(HYPERDEX_HEADER_SIZE_SV)
        << op->key << flags << checks << funcs << mos->rid << op->seq_no;
    m_daemon->m_comm.send(to, REQ_MIGRATION, msg);
    // TODO: do we need this here? m_daemon->m_comm.wake_one();
}

void
migration_manager :: migration_ack(const server_id& from,
                                   const virtual_server_id& to,
                                   region_id rid,
                                   uint64_t seq_no,
                                   uint16_t result)
{
    migration_out_state* mos = get_mos(rid);

    if (!mos)
    {
        // TODO: it seems that sometimes we receive ACK for regions
        // that have already been completely migrated.  Why is that?
        // Does that indicate a bug?
        // LOG(INFO) << "dropping RESP_MIGRATION for " << rid << " which we don't know about.";
        return;
    }

    po6::threads::mutex::hold hold(&mos->mtx);

    // TODO: do we need to check if the ACK comes from the right server?
    // The state transfer manager does that.

    std::list<e::intrusive_ptr<pending> >::iterator it;

    for (it = mos->window.begin(); it != mos->window.end(); ++it)
    {
        if ((*it)->seq_no == seq_no)
        {
            break;
        }
    }

    if (it != mos->window.end())
    {
        (*it)->acked = true;

        if (mos->window_sz < 1024)
        {
            ++mos->window_sz;
        }
    }

    while (!mos->window.empty() && (*mos->window.begin())->acked)
    {
        mos->window.pop_front();
    }

    migrate_more_state(mos);
}

migration_manager::migration_out_state*
migration_manager :: get_mos(region_id rid)
{
    for (size_t i = 0; i < m_migrations_out.size(); ++i)
    {
        if (m_migrations_out[i]->rid == rid)
        {
            return m_migrations_out[i].get();
        }
    }

    return NULL;
}

void
migration_manager :: kickstarter()
{
    LOG(INFO) << "migration thread started";
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
        {
            po6::threads::mutex::hold hold(&m_block_kickstarter);

            while ((!m_need_kickstart && !m_shutdown) || m_need_pause)
            {
                m_paused = true;

                if (m_need_pause)
                {
                    m_wakeup_reconfigurer.signal();
                }

                m_wakeup_kickstarter.wait();
                m_paused = false;
            }

            if (m_shutdown)
            {
                break;
            }

            m_need_kickstart = false;
        }

        size_t idx = 0;

        while (true)
        {
            po6::threads::mutex::hold hold(&m_block_kickstarter);

            if (idx >= m_migrations_out.size())
            {
                break;
            }

            po6::threads::mutex::hold hold2(&m_migrations_out[idx]->mtx);
            retransmit(m_migrations_out[idx].get());
            migrate_more_state(m_migrations_out[idx].get());
            ++idx;
        }
    }

    LOG(INFO) << "migration thread shutting down";
}

void
migration_manager :: shutdown()
{
    bool is_shutdown;

    {
        po6::threads::mutex::hold hold(&m_block_kickstarter);
        m_wakeup_kickstarter.broadcast();
        is_shutdown = m_shutdown;
        m_shutdown = true;
    }

    if (!is_shutdown)
    {
        m_kickstarter.join();
    }
}
