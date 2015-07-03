// Copyright (c) 2015, Cornell University
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

#ifndef hyperdex_daemon_coordinator_link_h_
#define hyperdex_daemon_coordinator_link_h_

// STL
#include <map>

// po6
#include <po6/threads/mutex.h>

// e
#include <e/compat.h>

// Replicant
#include <replicant.h>

// HyperDex
#include "namespace.h"
#include "common/configuration.h"
#include "common/ids.h"

BEGIN_HYPERDEX_NAMESPACE
class daemon;

class coordinator_link
{
    public:
        coordinator_link(daemon* d, const char* host, uint16_t port);
        coordinator_link(daemon* d, const char* conn_str);
        ~coordinator_link() throw ();

    // external synchro required
    public:
        bool register_server(server_id us, const po6::net::location& bind_to);
        bool initialize();
        bool maintain();
        void shutdown();
        const configuration& config() { return m_config; }
        uint64_t checkpoint_config_version() { return m_value_checkpoint_config_version; }
        uint64_t checkpoint() { return m_value_checkpoint; }
        uint64_t checkpoint_stable() { return m_value_checkpoint_stable; }
        uint64_t checkpoint_gc() { return m_value_checkpoint_gc; }

    // internally synchronized
    public:
        void config_ack(uint64_t config_version);
        void config_stable(uint64_t config_version);
        void checkpoint_report_stable(uint64_t config_version);
        void transfer_go_live(const transfer_id& id);
        void transfer_complete(const transfer_id& id);
        void report_tcp_disconnect(uint64_t config_version, const server_id& id);

    private:
        struct rpc;
        void make_rpc(e::compat::shared_ptr<rpc> r);
        void make_rpc_no_synchro(e::compat::shared_ptr<rpc> r);
        bool synchronous_call(const char* log_action, const char* func,
                              const char* input, size_t input_sz,
                              char** output, size_t* output_sz);
        bool process_configuration(const char* config, size_t config_sz);
        bool bring_online();

    private:
        daemon* m_daemon;
        unsigned m_sleep_ms;
        // configuration
        configuration m_config;
        int64_t m_config_id;
        replicant_returncode m_config_status;
        uint64_t m_config_state;
        char* m_config_data;
        size_t m_config_data_sz;
        // checkpoints
        int64_t m_checkpoint_id;
        replicant_returncode m_checkpoint_status;
        uint64_t m_checkpoint_state;
        char* m_checkpoint_data;
        size_t m_checkpoint_data_sz;
        uint64_t m_value_checkpoint_config_version;
        uint64_t m_value_checkpoint;
        uint64_t m_value_checkpoint_stable;
        uint64_t m_value_checkpoint_gc;
        // other RPCs
        replicant_returncode m_throwaway_status;
        po6::threads::mutex m_mtx;
        replicant_client* m_repl;
        std::map<int64_t, e::compat::shared_ptr<rpc> > m_rpcs;

    private:
        coordinator_link(const coordinator_link&);
        coordinator_link& operator = (const coordinator_link&);
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_coordinator_link_h_
