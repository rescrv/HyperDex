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

#ifndef hyperdex_admin_admin_h_
#define hyperdex_admin_admin_h_

// STL
#include <map>
#include <list>

// BusyBee
#include <busybee_st.h>

// HyperDex
#include "include/hyperdex/admin.h"
#include "namespace.h"
#include "common/coordinator_link.h"
#include "common/mapper.h"
#include "admin/pending.h"
#include "admin/pending_perf_counters.h"

BEGIN_HYPERDEX_NAMESPACE

class admin
{
    public:
        admin(const char* coordinator, uint16_t port);
        ~admin() throw ();

    public:
        // introspect the config
        int64_t dump_config(enum hyperdex_admin_returncode* status,
                            const char** config);
        // manage spaces
        int validate_space(const char* description,
                           enum hyperdex_admin_returncode* status,
                           const char** error_msg);
        int64_t add_space(const char* description,
                          enum hyperdex_admin_returncode* status);
        int64_t rm_space(const char* name,
                         enum hyperdex_admin_returncode* status);
        // read performance counters
        int64_t enable_perf_counters(hyperdex_admin_returncode* status,
                                     hyperdex_admin_perf_counter* pc);
        void disable_perf_counters();
        // looping/polling
        int64_t loop(int timeout, hyperdex_admin_returncode* status);

    private:
        struct pending_server_pair
        {
            pending_server_pair()
                : si(), op() {}
            pending_server_pair(const server_id& s,
                                const e::intrusive_ptr<pending>& o)
                : si(s), op(o) {}
            ~pending_server_pair() throw () {}
            server_id si;
            e::intrusive_ptr<pending> op;
        };
        typedef std::map<uint64_t, pending_server_pair> pending_map_t;
        typedef std::list<pending_server_pair> pending_queue_t;
        friend class pending_perf_counters;

    private:
        bool maintain_coord_connection(hyperdex_admin_returncode* status);
        bool send(network_msgtype mt,
                  server_id id,
                  uint64_t nonce,
                  std::auto_ptr<e::buffer> msg,
                  e::intrusive_ptr<pending> op,
                  hyperdex_admin_returncode* status);
        void handle_disruption(const server_id& si);

    private:
        coordinator_link m_coord;
        mapper m_busybee_mapper;
        busybee_st m_busybee;
        int64_t m_next_admin_id;
        uint64_t m_next_server_nonce;
        pending_map_t m_pending_ops;
        pending_queue_t m_failed;
        std::list<e::intrusive_ptr<pending> > m_yieldable;
        e::intrusive_ptr<pending> m_yielding;
        e::intrusive_ptr<pending_perf_counters> m_pcs;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_admin_admin_h_
