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

#ifndef hyperdex_daemon_migration_manager_h_
#define hyperdex_daemon_migration_manager_h_

// STL
#include <memory>

// po6
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/intrusive_ptr.h>

// HyperDex
#include "namespace.h"
#include "common/configuration.h"
#include "daemon/reconfigure_returncode.h"
#include "daemon/datalayer.h"

BEGIN_HYPERDEX_NAMESPACE
class daemon;

class migration_manager
{
    public:
        migration_manager(daemon*);
        ~migration_manager() throw ();

    public:
        bool setup();
        void teardown();
        void pause();
        void unpause();
        void reconfigure(const configuration& old_config,
                         const configuration& new_config,
                         const server_id&);

    // public:
    //     void handshake_syn(const virtual_server_id& from,
    //                        const migration_id& xid);
    //     void handshake_synack(const server_id& from,
    //                           const virtual_server_id& to,
    //                           const migration_id& xid,
    //                           uint64_t timestamp);
    //     void handshake_ack(const virtual_server_id& from,
    //                        const migration_id& xid,
    //                        bool wipe);
    //     void handshake_wiped(const server_id& from,
    //                          const virtual_server_id& to,
    //                          const migration_id& xid);
    //     void report_wiped(const migration_id& xid);
    //     void xfer_op(const virtual_server_id& from,
    //                  const migration_id& xid,
    //                  uint64_t seq_no,
    //                  bool has_value,
    //                  uint64_t version,
    //                  std::auto_ptr<e::buffer> msg,
    //                  const e::slice& key,
    //                  const std::vector<e::slice>& value);
    //     void xfer_ack(const server_id& from,
    //                   const virtual_server_id& to,
    //                   const migration_id& xid,
    //                   uint64_t seq_no);

    private:
        class migration_out_state;

    private:
        void kickstarter();
        void shutdown();

    private:
        migration_manager(const migration_manager&);
        migration_manager& operator = (const migration_manager&);

    private:
        daemon* m_daemon;
        std::vector<e::intrusive_ptr<migration_out_state> > m_migrations_out;
        po6::threads::thread m_kickstarter;
        po6::threads::mutex m_block_kickstarter;
        po6::threads::cond m_wakeup_kickstarter;
        po6::threads::cond m_wakeup_reconfigurer;
        bool m_need_kickstart;
        bool m_shutdown;
        bool m_need_pause;
        bool m_paused;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_migration_manager_h_