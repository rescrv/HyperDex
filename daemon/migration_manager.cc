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
// #include "daemon/migration_manager_pending.h"
// #include "daemon/migration_manager_migration_in_state.h"
#include "daemon/migration_out_state.h"
#include "daemon/leveldb.h"

using hyperdex::reconfigure_returncode;
using hyperdex::migration_manager;
using hyperdex::migration_id;

migration_manager :: migration_manager(daemon* d)
    : m_daemon(d)
    // , m_migrations_in()
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
    // m_migrations_in.clear();
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

    // TODO
    // // Setup migrations in
    // std::vector<migration> migrations_in;
    // new_config.migrations_in(m_daemon->m_us, &migrations_in);
    // std::sort(migrations_in.begin(), migrations_in.end());
    // setup_migration_state("incoming", migrations_in, &m_migrations_in);

    // Setup migrations out
    std::vector<migration> migrations_out;
    new_config.migrations_out(sid, &migrations_out);

    std::vector<region_id> regions;
    new_config.mapped_regions(sid, &regions);

    leveldb_snapshot_ptr s = m_daemon->m_data.make_snapshot();

    std::vector<hyperdex::migration>::iterator m_iter;
    for (m_iter = migrations_out.begin(); m_iter != migrations_out.end(); m_iter++) {
        std::vector<hyperdex::region_id>::iterator r_iter;
        for (r_iter = regions.begin(); r_iter != regions.end(); r_iter++) {
            region_id rid = (*r_iter);
            if (new_config.space_of(rid) == (*m_iter).space_from) {
                datalayer::returncode err;
                datalayer::iterator* data_iter = m_daemon->m_data.make_region_iterator(s, rid, &err);
                if (err != datalayer::SUCCESS) {
                    return;
                }
            }
        }
    }

    // setup_migration_state("outgoing", migrations_out, &m_migrations_out);
}

void
migration_manager :: kickstarter()
{
    LOG(INFO) << "state transfer thread started";
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

        // while (true)
        // {
        //     po6::threads::mutex::hold hold(&m_block_kickstarter);

        //     if (idx >= m_transfers_out.size())
        //     {
        //         break;
        //     }

        //     po6::threads::mutex::hold hold2(&m_transfers_out[idx]->mtx);
        //     retransmit(m_transfers_out[idx].get());
        //     transfer_more_state(m_transfers_out[idx].get());
        //     ++idx;
        // }
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
