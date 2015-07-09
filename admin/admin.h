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

#define __STDC_LIMIT_MACROS

#ifndef hyperdex_admin_admin_h_
#define hyperdex_admin_admin_h_

// STL
#include <map>
#include <list>

// BusyBee
#include <busybee_st.h>

// Replicant
#include <replicant.h>

// HyperDex
#include "include/hyperdex/admin.h"
#include "namespace.h"
#include "common/mapper.h"
#include "admin/coord_rpc.h"
#include "admin/multi_yieldable.h"
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
        // cluster
        int64_t read_only(int ro,
                          enum hyperdex_admin_returncode* status);
        int64_t wait_until_stable(enum hyperdex_admin_returncode* status);
        int64_t fault_tolerance(const char* space, uint64_t ft,
                                enum hyperdex_admin_returncode* status);
        // manage spaces
        int validate_space(const char* description,
                           enum hyperdex_admin_returncode* status);
        int64_t add_space(const char* description,
                          enum hyperdex_admin_returncode* status);
        int64_t rm_space(const char* name,
                         enum hyperdex_admin_returncode* status);
        int64_t mv_space(const char* source, const char* target,
                         enum hyperdex_admin_returncode* status);
        int64_t add_index(const char* space, const char* attr,
                          enum hyperdex_admin_returncode* status);
        int64_t list_indices(const char* space, enum hyperdex_admin_returncode* status,
                            const char** spaces);
        int64_t rm_index(uint64_t idxid,
                         enum hyperdex_admin_returncode* status);
        int64_t list_spaces(enum hyperdex_admin_returncode* status,
                            const char** spaces);
        int64_t list_subspaces(const char* space,
                            enum hyperdex_admin_returncode* status,
                            const char** subspaces);
        // manage servers
        int64_t server_register(uint64_t token, const char* address,
                                enum hyperdex_admin_returncode* status);
        int64_t server_online(uint64_t token, enum hyperdex_admin_returncode* status);
        int64_t server_offline(uint64_t token, enum hyperdex_admin_returncode* status);
        int64_t server_forget(uint64_t token, enum hyperdex_admin_returncode* status);
        int64_t server_kill(uint64_t token, enum hyperdex_admin_returncode* status);
        // backups
        int64_t backup(const char* name, enum hyperdex_admin_returncode* status, const char** backups);
        int64_t coord_backup(const char* path,
                             enum hyperdex_admin_returncode* status);
        int64_t raw_backup(const server_id& sid, const char* name,
                           enum hyperdex_admin_returncode* status,
                           const char** path);
        // read performance counters
        int64_t enable_perf_counters(hyperdex_admin_returncode* status,
                                     hyperdex_admin_perf_counter* pc);
        void disable_perf_counters();
        // looping/polling
        int64_t loop(int timeout, hyperdex_admin_returncode* status);
        // error handling
        const char* error_message();
        const char* error_location();
        void set_error_message(const char* msg);
        void interpret_replicant_returncode(replicant_returncode rstatus,
                                            hyperdex_admin_returncode* status,
                                            e::error* err);

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
        typedef std::map<int64_t, e::intrusive_ptr<coord_rpc> > coord_rpc_map_t;
        typedef std::map<int64_t, e::intrusive_ptr<multi_yieldable> > multi_yieldable_map_t;
        typedef std::list<pending_server_pair> pending_queue_t;
        friend class backup_state_machine;
        friend class pending_perf_counters;

    private:
        bool maintain_coord_connection(hyperdex_admin_returncode* status);
        int64_t rpc(const char* func,
                    const char* data, size_t data_sz,
                    replicant_returncode* status,
                    char** output, size_t* output_sz);
        bool send(network_msgtype mt,
                  server_id id,
                  uint64_t nonce,
                  std::auto_ptr<e::buffer> msg,
                  e::intrusive_ptr<pending> op,
                  hyperdex_admin_returncode* status);
        void handle_disruption(const server_id& si);

    private:
        replicant_client* m_coord;
        mapper m_busybee_mapper;
        busybee_st m_busybee;
        // configuration
        configuration m_config;
        int64_t m_config_id;
        replicant_returncode m_config_status;
        uint64_t m_config_state;
        char* m_config_data;
        size_t m_config_data_sz;
        // nonces
        int64_t m_next_admin_id;
        uint64_t m_next_server_nonce;
        // operations
        bool m_handle_coord_ops;
        coord_rpc_map_t m_coord_ops;
        pending_map_t m_server_ops;
        multi_yieldable_map_t m_multi_ops;
        pending_queue_t m_failed;
        std::list<e::intrusive_ptr<yieldable> > m_yieldable;
        e::intrusive_ptr<yieldable> m_yielding;
        e::intrusive_ptr<yieldable> m_yielded;
        e::intrusive_ptr<pending_perf_counters> m_pcs;
        // misc
        e::error m_last_error;

    private:
        admin(const admin&);
        admin& operator = (const admin&);
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_admin_admin_h_
