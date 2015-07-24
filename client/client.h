// Copyright (c) 2011-2013, Cornell University
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

#ifndef hyperdex_client_client_h_
#define hyperdex_client_client_h_

// STL
#include <map>
#include <list>

// BusyBee
#include <busybee_st.h>

// Replicant
#include <replicant.h>

// HyperDex
#include <hyperdex/client.h>
#include "namespace.h"
#include "common/configuration.h"
#include "common/mapper.h"
#include "client/keyop_info.h"
#include "client/pending.h"
#include "client/pending_aggregation.h"

BEGIN_HYPERDEX_NAMESPACE

struct microtransaction
{
    microtransaction(const char* space_, const schema& sc_,
                              hyperdex_client_returncode *status_)
        : space(space_), sc(sc_), status(status_), funcalls(), memory()
    {}

    int64_t generate_message(size_t header_sz, size_t footer_sz,
                             const std::vector<attribute_check>& checks,
                             std::auto_ptr<e::buffer>* msg);

    const char* space;
    const schema& sc;
    hyperdex_client_returncode* status;
    std::vector<funcall> funcalls;
    e::arena memory;

    private:
        microtransaction(const microtransaction&);
        microtransaction& operator = (const microtransaction&);
};

class client
{
    public:
        client(const char* coordinator, uint16_t port);
        client(const char* conn_str);
        ~client() throw ();

    public:
        void clear_auth_context() { m_macaroons = NULL; m_macaroons_sz = 0; }
        void set_auth_context(const char** macaroons, size_t macaroons_sz)
        { m_macaroons = macaroons; m_macaroons_sz = macaroons_sz; }

    public:
        // specific calls
        int64_t get(const char* space, const char* key, size_t key_sz,
                    hyperdex_client_returncode* status,
                    const hyperdex_client_attribute** attrs, size_t* attrs_sz);
        int64_t get_partial(const char* space, const char* key, size_t key_sz,
                            const char** attrnames, size_t attrnames_sz,
                            hyperdex_client_returncode* status,
                            const hyperdex_client_attribute** attrs, size_t* attrs_sz);
        int64_t search(const char* space,
                       const hyperdex_client_attribute_check* checks, size_t checks_sz,
                       hyperdex_client_returncode* status,
                       const hyperdex_client_attribute** attrs, size_t* attrs_sz);
        int64_t search_describe(const char* space,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                hyperdex_client_returncode* status, const char** description);
        int64_t sorted_search(const char* space,
                              const hyperdex_client_attribute_check* checks, size_t checks_sz,
                              const char* sort_by,
                              uint64_t limit,
                              bool maximize,
                              hyperdex_client_returncode* status,
                              const hyperdex_client_attribute** attrs, size_t* attrs_sz);
        int64_t group_del(const char* space,
                          const hyperdex_client_attribute_check* checks, size_t checks_sz,
                          hyperdex_client_returncode* status);
        int64_t count(const char* space,
                      const hyperdex_client_attribute_check* checks, size_t checks_sz,
                      hyperdex_client_returncode* status, uint64_t* result);

        // General keyop call
        // This will be called by the bindings from c.cc
        int64_t perform_funcall(const hyperdex_client_keyop_info* opinfo,
                                const char* space, const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                hyperdex_client_returncode* status);

        // General keyop call for group operations
        // This will be called by the bindings from c.cc
        int64_t perform_group_funcall(const hyperdex_client_keyop_info* opinfo,
                                      const char* space,
                                      const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                      const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                      const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                      hyperdex_client_returncode* status,
                                      uint64_t* update_count);

        // Initiate a microtransaction
        // Status and transaction object must remain valid until the operation has completed
        microtransaction *uxact_init(const char* space,
                                   hyperdex_client_returncode *status);

        // Add a new funcall to the microstransaction
        int64_t uxact_add_funcall(microtransaction *transaction,
                                          const hyperdex_client_keyop_info* opinfo,
                                          const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                          const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz);

        // Issue a microtransaction to a specific key
        int64_t uxact_commit(microtransaction *transaction,
                             const char* key, size_t key_sz);

        // Issue a microtransaction to a specific key only if checks passs
        int64_t uxact_cond_commit(microtransaction *transaction,
                                  const char* key, size_t key_sz,
                                  const hyperdex_client_attribute_check* checks, size_t checks_sz);

        // Issue a mircotransaction to a all entries matching the checks
        int64_t uxact_group_commit(microtransaction *transaction,
                                   const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                   uint64_t *update_count);

        // looping/polling
        int64_t loop(int timeout, hyperdex_client_returncode* status);
        // Return the fildescriptor that hyperdex uses for networking
        int poll_fd();
        // Ensure the flagfd is set correctly
        void adjust_flagfd();
        // Block unitl there is incoming data or the timeout is reached
        int block(int timeout);
        // error handling
        const char* error_message();
        const char* error_location();
        void set_error_message(const char* msg);
        // helpers for bindings
        hyperdatatype attribute_type(const char* space, const char* name,
                                     hyperdex_client_returncode* status);
        // enable or disable type conversion on the client-side
        void set_type_conversion(bool enabled);

    private:
        struct pending_server_pair
        {
            pending_server_pair()
                : si(), vsi(), op() {}
            pending_server_pair(const server_id& s,
                                const virtual_server_id& v,
                                const e::intrusive_ptr<pending>& o)
                : si(s), vsi(v), op(o) {}
            ~pending_server_pair() throw () {}
            server_id si;
            virtual_server_id vsi;
            e::intrusive_ptr<pending> op;
        };
        typedef std::map<uint64_t, pending_server_pair> pending_map_t;
        typedef std::list<pending_server_pair> pending_queue_t;
        friend class pending_get;
        friend class pending_get_partial;
        friend class pending_search;
        friend class pending_sorted_search;

    private:
        size_t prepare_checks(const char* space, const schema& sc,
                              const hyperdex_client_attribute_check* chks, size_t chks_sz,
                              e::arena* memory,
                              hyperdex_client_returncode* status,
                              std::vector<attribute_check>* checks);
        size_t prepare_funcs(const char* space, const schema& sc,
                             const hyperdex_client_keyop_info* opinfo,
                             const hyperdex_client_attribute* attrs, size_t attrs_sz,
                             e::arena* memory,
                             hyperdex_client_returncode* status,
                             std::vector<funcall>* funcs);
        size_t prepare_funcs(const char* space, const schema& sc,
                             const hyperdex_client_keyop_info* opinfo,
                             const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                             e::arena* memory,
                             hyperdex_client_returncode* status,
                             std::vector<funcall>* funcs);
        size_t prepare_searchop(const schema& sc,
                                const char* space,
                                const hyperdex_client_attribute_check* chks, size_t chks_sz,
                                e::arena* memory,
                                hyperdex_client_returncode* status,
                                std::vector<attribute_check>* checks,
                                std::vector<virtual_server_id>* servers);
        int64_t perform_funcall(const char* space, const schema* sc,
                                const hyperdex_client_keyop_info* opinfo,
                                const hyperdex_client_attribute_check* chks, size_t chks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                size_t header_sz,
                                size_t footer_sz,
                                hyperdex_client_returncode* status,
                                std::auto_ptr<e::buffer>* msg);
        int64_t perform_aggregation(const std::vector<virtual_server_id>& servers,
                                    e::intrusive_ptr<pending_aggregation> op,
                                    network_msgtype mt,
                                    std::auto_ptr<e::buffer> msg,
                                    hyperdex_client_returncode* status);
        bool maintain_coord_connection(hyperdex_client_returncode* status);
        bool send(network_msgtype mt,
                  const virtual_server_id& to,
                  uint64_t nonce,
                  std::auto_ptr<e::buffer> msg,
                  e::intrusive_ptr<pending> op,
                  hyperdex_client_returncode* status);
        int64_t send_keyop(const char* space,
                           const e::slice& key,
                           network_msgtype mt,
                           std::auto_ptr<e::buffer> msg,
                           e::intrusive_ptr<pending> op,
                           hyperdex_client_returncode* status);
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
        int64_t m_next_client_id;
        uint64_t m_next_server_nonce;
        e::flagfd m_flagfd;
        // operations
        pending_map_t m_pending_ops;
        pending_queue_t m_failed;
        std::list<e::intrusive_ptr<pending> > m_yieldable;
        e::intrusive_ptr<pending> m_yielding;
        e::intrusive_ptr<pending> m_yielded;
        // misc
        e::error m_last_error;
        const char** m_macaroons;
        size_t m_macaroons_sz;
        bool m_convert_types;

    private:
        client(const client&);
        client& operator = (const client&);
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_client_client_h_
