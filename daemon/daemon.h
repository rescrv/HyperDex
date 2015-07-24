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

#ifndef hyperdex_daemon_daemon_h_
#define hyperdex_daemon_daemon_h_

// po6
#include <po6/net/hostname.h>
#include <po6/net/ipaddr.h>
#include <po6/net/location.h>
#include <po6/path.h>
#include <po6/threads/thread.h>

// e
#include <e/compat.h>

// Replicant
#include <replicant.h>

// HyperDex
#include "namespace.h"
#include "common/ids.h"
#include "daemon/communication.h"
#include "daemon/coordinator_link.h"
#include "daemon/datalayer.h"
#include "daemon/performance_counter.h"
#include "daemon/replication_manager.h"
#include "daemon/search_manager.h"
#include "daemon/state_transfer_manager.h"

BEGIN_HYPERDEX_NAMESPACE

class daemon
{
    public:
        daemon();
        ~daemon() throw ();

    public:
        int run(bool daemonize,
                std::string data,
                std::string log,
                std::string pidfile,
                bool has_pidfile,
                bool set_bind_to,
                po6::net::location bind_to,
                bool set_coordinator,
                po6::net::hostname coordinator,
                unsigned threads);

    private:
        // Pause and unpause all activity, e.g. for reconfiguration or
        // installing new indices.  If called from a background thread, the
        // thread must remain offline for entire time between pause/unpause.
        void pause();
        void unpause();
        // process messages from the network threads
        void loop(size_t thread);
        void process_req_get(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_req_get_partial(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_req_atomic(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_req_search_start(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_req_search_next(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_req_search_stop(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_req_sorted_search(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_req_count(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_req_search_describe(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_req_group_atomic(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_chain_op(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_chain_subspace(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_chain_ack(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_xfer_handshake_syn(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_xfer_handshake_synack(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_xfer_handshake_ack(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_xfer_handshake_wiped(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_xfer_op(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_xfer_ack(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_backup(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);
        void process_perf_counters(server_id from, virtual_server_id vfrom, virtual_server_id vto, std::auto_ptr<e::buffer> msg, e::unpacker up);

    private:
        void collect_stats();
        void collect_stats_msgs(std::ostringstream* ret);
        void collect_stats_leveldb(std::ostringstream* ret);
        void determine_block_stat_path(const std::string& data);
        void collect_stats_io(std::ostringstream* ret);

    private:
        friend class background_thread;
        friend class communication;
        friend class coordinator_link;
        friend class datalayer;
        friend class key_state;
        friend class replication_manager;
        friend class search_manager;
        friend class state_transfer_manager;

    private:
        server_id m_us;
        po6::net::location m_bind_to;
        std::vector<e::compat::shared_ptr<po6::threads::thread> > m_threads;
        e::garbage_collector m_gc;
        e::garbage_collector::thread_state m_gc_ts;
        std::auto_ptr<coordinator_link> m_coord;
        std::string m_data_dir;
        datalayer m_data;
        communication m_comm;
        replication_manager m_repl;
        state_transfer_manager m_stm;
        search_manager m_sm;
        configuration m_config;
        // pause management
        po6::threads::mutex m_protect_pause;
        po6::threads::cond m_can_pause;
        bool m_paused;
        // counters
        performance_counter m_perf_req_get;
        performance_counter m_perf_req_get_partial;
        performance_counter m_perf_req_atomic;
        performance_counter m_perf_req_search_start;
        performance_counter m_perf_req_search_next;
        performance_counter m_perf_req_search_stop;
        performance_counter m_perf_req_sorted_search;
        performance_counter m_perf_req_count;
        performance_counter m_perf_req_search_describe;
        performance_counter m_perf_req_group_atomic;
        performance_counter m_perf_chain_op;
        performance_counter m_perf_chain_subspace;
        performance_counter m_perf_chain_ack;
        performance_counter m_perf_xfer_handshake_syn;
        performance_counter m_perf_xfer_handshake_synack;
        performance_counter m_perf_xfer_handshake_ack;
        performance_counter m_perf_xfer_handshake_wiped;
        performance_counter m_perf_xfer_op;
        performance_counter m_perf_xfer_ack;
        performance_counter m_perf_backup;
        performance_counter m_perf_perf_counters;
        // iostat-like stats
        std::string m_block_stat_path;
        // historical data
        po6::threads::thread m_stat_collector;
        po6::threads::mutex m_protect_stats;
        uint64_t m_stats_start;
        std::list<std::pair<uint64_t, std::string> > m_stats;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_daemon_h_
