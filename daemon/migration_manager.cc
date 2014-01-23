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
#include "daemon/migration_out_state.h"
#include "daemon/leveldb.h"

using hyperdex::reconfigure_returncode;
using hyperdex::migration_manager;
using hyperdex::migration_id;

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
    {
        po6::threads::mutex::hold hold(&m_block_kickstarter);
        assert(m_need_pause);

        while (!m_paused)
        {
            m_wakeup_reconfigurer.wait();
        }
    }

    std::vector<migration> migrations_out;
    new_config.migrations_out(sid, &migrations_out);

    std::vector<region_id> regions;
    new_config.mapped_regions(sid, &regions);

    std::vector<hyperdex::migration>::iterator m_iter;
    for (m_iter = migrations_out.begin(); m_iter != migrations_out.end(); m_iter++) {
        migration_out_state* new_state = NULL;
        std::vector<hyperdex::region_id>::iterator r_iter;
        for (r_iter = regions.begin(); r_iter != regions.end(); r_iter++) {
            region_id rid = (*r_iter);
            if (new_config.space_of(rid) == (*m_iter).space_from) {
                if (new_state == NULL) {
                    new_state = new migration_out_state();
                }
                new_state->region_iters.push_back(new std::pair<space_id, region_id>((*m_iter).space_to, rid));
            }
        }
        if (new_state != NULL) {
            e::intrusive_ptr<migration_out_state> ptr(new_state);
            m_migrations_out.push_back(ptr);
        }
    }
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

        leveldb_snapshot_ptr snapshot_ptr = m_daemon->m_data.make_snapshot();

        while (true)
        {
            po6::threads::mutex::hold hold(&m_block_kickstarter);

            if (idx >= m_migrations_out.size())
            {
                break;
            }

            po6::threads::mutex::hold hold2(&m_migrations_out[idx]->mtx);

            // send data
            migration_out_state* mos = m_migrations_out[idx].get();
            for (size_t i; i < mos->region_iters.size(); i++) {
                space_id sid = mos->region_iters[i]->first;
                region_id rid = mos->region_iters[i]->second;
                datalayer::returncode err;
                datalayer::iterator* iter = m_daemon->m_data.make_region_iterator(snapshot_ptr, rid, &err);
                if (err != datalayer::SUCCESS) {
                    LOG(ERROR) << "failed to create region iterator";
                    break;
                }

                if (iter->valid())
                {
                    e::slice key;
                    std::vector<e::slice> val;
                    uint64_t ver;
                    datalayer::reference tmp;
                    m_daemon->m_data.get_from_iterator(rid, iter, &key, &val, &ver, &tmp);
                    virtual_server_id to = m_daemon->m_config.point_leader(sid, key);

                    const schema* sc = m_daemon->m_config.get_schema(rid);
                    std::vector<funcall> funcs;
                    std::vector<attribute_check> checks;
                    funcs.reserve(val.size());

                    for (size_t i = 0; i < val.size(); ++i)
                    {
                        uint16_t attrnum = i;

                        hyperdatatype datatype = sc->attrs[attrnum + 1].type;
                        std::cout << "datatype " << i << ": " << datatype << std::endl;

                        funcall o;
                        o.attr = attrnum + 1;
                        o.name = FUNC_SET;
                        o.arg1 = val[i];
                        o.arg1_datatype = datatype;
                        funcs.push_back(o);
                    }

                    std::cout << "funcs size: " << funcs.size() << std::endl;

                    size_t HEADER_SIZE = BUSYBEE_HEADER_SIZE
                              + sizeof(uint8_t) /*mt*/ \
                              + sizeof(uint8_t) /*flags*/ \
                              + sizeof(uint64_t) /*version*/ \
                              + sizeof(uint64_t); /*vidt*/

                    size_t sz = HEADER_SIZE
                              + sizeof(uint64_t) /*nonce*/
                              + sizeof(uint8_t)
                              + pack_size(key)
                              + pack_size(checks)
                              + pack_size(funcs);
                    std::cout << "sent size: " << sz << std::endl;
                    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
                    uint8_t flags = (0 | 0 | 128);
                    uint64_t nonce = 0; // TODO: what should it be?
                    msg->pack_at(HEADER_SIZE)
                        << nonce << key << flags << checks << funcs;
                    m_daemon->m_comm.send(to, REQ_ATOMIC, msg);
                    iter->next();
                }
            }

            ++idx;
        }
    }

    LOG(INFO) << "state transfer thread shutting down";
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
