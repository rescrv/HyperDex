// Copyright (c) 2013, Cornell University
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

#ifndef hyperdex_daemon_coordinator_link_wrapper_h_
#define hyperdex_daemon_coordinator_link_wrapper_h_

// C
#include <stdint.h>

// po6
#include <po6/net/location.h>
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>

// HyperDex
#include "namespace.h"
#include "common/configuration.h"
#include "common/coordinator_link.h"
#include "common/ids.h"

BEGIN_HYPERDEX_NAMESPACE
class daemon;

// The thread whose calls the constructor can call everything.  All other
// threads are left with the threadsafe block below.

class coordinator_link_wrapper
{
    public:
        coordinator_link_wrapper(daemon* d);
        ~coordinator_link_wrapper() throw ();

    public:
        void set_coordinator_address(const char* host, uint16_t port);
        bool register_id(server_id us, const po6::net::location& bind_to);
        bool should_exit();
        bool maintain_link();
        const configuration& config();
        void request_shutdown();
        uint64_t checkpoint();
        uint64_t checkpoint_stable();
        uint64_t checkpoint_gc();

    // threadsafe
    public:
        void transfer_go_live(const transfer_id& id);
        void transfer_complete(const transfer_id& id);
        void report_tcp_disconnect(const server_id& id);
        void config_ack(uint64_t version);
        void config_stable(uint64_t version);
        void checkpoint_report_stable(uint64_t checkpoint);

    private:
        class coord_rpc;
        class coord_rpc_available;
        class coord_rpc_config_ack;
        class coord_rpc_config_stable;
        class coord_rpc_checkpoint;
        class coord_rpc_checkpoint_report_stable;
        class coord_rpc_checkpoint_stable;
        class coord_rpc_checkpoint_gc;
        typedef std::map<int64_t, e::intrusive_ptr<coord_rpc> > rpc_map_t;

    private:
        void do_sleep();
        void reset_sleep();
        void enter_critical_section();
        void exit_critical_section();
        void enter_critical_section_killable();
        void exit_critical_section_killable();
        void ensure_available();
        void ensure_config_ack();
        void ensure_config_stable();
        void ensure_checkpoint();
        void ensure_checkpoint_report_stable();
        void ensure_checkpoint_stable();
        void ensure_checkpoint_gc();
        void make_rpc(const char* func,
                      const char* data, size_t data_sz,
                      e::intrusive_ptr<coord_rpc> rpc);
        int64_t make_rpc_nosync(const char* func,
                                const char* data, size_t data_sz,
                                e::intrusive_ptr<coord_rpc> rpc);
        int64_t wait_nosync(const char* cond, uint64_t state,
                            e::intrusive_ptr<coord_rpc> rpc);

    private:
        daemon* m_daemon;
        std::auto_ptr<coordinator_link> m_coord;
        rpc_map_t m_rpcs;
        po6::threads::mutex m_mtx;
        po6::threads::cond m_cond;
        bool m_locked;
        bool m_kill;
        pthread_t m_to_kill;
        uint64_t m_waiting;
        uint64_t m_sleep;
        int64_t m_online_id;
        bool m_shutdown_requested;
        // make sure we reliably ack
        bool m_need_config_ack;
        uint64_t m_config_ack;
        int64_t m_config_ack_id;
        // make sure we reliably stabilize
        bool m_need_config_stable;
        uint64_t m_config_stable;
        int64_t m_config_stable_id;
        // reliably track the checkpoint
        uint64_t m_checkpoint;
        int64_t m_checkpoint_id;
        // reliably checkpoint
        bool m_need_checkpoint_report_stable;
        uint64_t m_checkpoint_report_stable;
        int64_t m_checkpoint_report_stable_id;
        // repliably track checkpoint_stable
        uint64_t m_checkpoint_stable;
        int64_t m_checkpoint_stable_id;
        // repliably track checkpoint_gc
        uint64_t m_checkpoint_gc;
        int64_t m_checkpoint_gc_id;

    private:
        coordinator_link_wrapper(const coordinator_link_wrapper&);
        coordinator_link_wrapper& operator = (const coordinator_link_wrapper&);
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_coordinator_link_wrapper_h_
