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
#include <po6/pathname.h>
#include <po6/threads/thread.h>

// HyperDex
#include "daemon/datalayer.h"
#include "daemon/search_manager.h"
#include "hyperdex/hyperdex/coordinatorlink.h"
#include "hyperdaemon/logical.h"
#include "hyperdaemon/replication_manager.h"

namespace hyperdex
{

typedef std::tr1::shared_ptr<po6::threads::thread> thread_ptr;

class daemon
{
    public:
        daemon(po6::net::location bind_to,
               po6::net::hostname coordinator,
               po6::pathname datadir,
               uint16_t num_threads,
               bool daemonize);
        ~daemon() throw ();

    public:
        int run();

    private:
        bool install_signal_handlers();
        bool install_signal_handler(int signum, void (*func)(int));
        bool generate_identifier_token();
        bool daemonize();
        void loop();

    private:
        void process_req_get(entityid from, entityid to, std::auto_ptr<e::buffer> msg, e::buffer::unpacker up);
        void process_req_atomic(entityid from, entityid to, std::auto_ptr<e::buffer> msg, e::buffer::unpacker up);
        void process_req_search_start(entityid from, entityid to, std::auto_ptr<e::buffer> msg, e::buffer::unpacker up);
        void process_req_search_next(entityid from, entityid to, std::auto_ptr<e::buffer> msg, e::buffer::unpacker up);
        void process_req_search_stop(entityid from, entityid to, std::auto_ptr<e::buffer> msg, e::buffer::unpacker up);
        void process_req_sorted_search(entityid from, entityid to, std::auto_ptr<e::buffer> msg, e::buffer::unpacker up);
        void process_req_group_del(entityid from, entityid to, std::auto_ptr<e::buffer> msg, e::buffer::unpacker up);
        void process_req_count(entityid from, entityid to, std::auto_ptr<e::buffer> msg, e::buffer::unpacker up);
        void process_chain_put(entityid from, entityid to, std::auto_ptr<e::buffer> msg, e::buffer::unpacker up);
        void process_chain_del(entityid from, entityid to, std::auto_ptr<e::buffer> msg, e::buffer::unpacker up);
        void process_chain_subspace(entityid from, entityid to, std::auto_ptr<e::buffer> msg, e::buffer::unpacker up);
        void process_chain_ack(entityid from, entityid to, std::auto_ptr<e::buffer> msg, e::buffer::unpacker up);

    private:
        friend class hyperdaemon::logical;
        friend class hyperdaemon::replication_manager;
        friend class datalayer;
        friend class search_manager;

    private:
        uint64_t m_token;
        instance m_us;
        std::vector<thread_ptr> m_threads;
        po6::net::hostname m_coordinator;
        po6::pathname m_datadir;
        bool m_daemonize;
        coordinatorlink m_cl;
        datalayer m_data;
        hyperdaemon::logical m_comm;
        hyperdaemon::replication_manager m_repl;
        search_manager m_sm;
        configuration m_config;
};

} // namespace hyperdex

#endif // hyperdex_daemon_daemon_h_
