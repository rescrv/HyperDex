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

// HyperDex
#include <hyperdex/client.h>
#include "namespace.h"
#include "common/configuration.h"
#include "common/coordinator_link.h"
#include "common/mapper.h"
#include "client/keyop_info.h"
#include "client/pending.h"
#include "client/pending_aggregation.h"

BEGIN_HYPERDEX_NAMESPACE

class client
{
    public:
        client(const char* coordinator, uint16_t port);
        ~client() throw ();

    public:
        hyperdex_client_returncode add_space(const char* description);
        hyperdex_client_returncode rm_space(const char* space);

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
        // general keyop call
        int64_t perform_funcall(const hyperdex_client_keyop_info* opinfo,
                                const char* space, const char* key, size_t key_sz,
                                const hyperdex_client_attribute_check* checks, size_t checks_sz,
                                const hyperdex_client_attribute* attrs, size_t attrs_sz,
                                const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                                hyperdex_client_returncode* status);
        // looping/polling
        int64_t loop(int timeout, hyperdex_client_returncode* status);
        int poll();
        // error handling
        const char* error_message();
        const char* error_location();
        void set_error_message(const char* msg);
        // helpers for bindings
        hyperdatatype attribute_type(const char* space, const char* name,
                                     hyperdex_client_returncode* status);

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
        typedef std::list<std::string> arena_t;
        friend class pending_get;
        friend class pending_get_partial;
        friend class pending_search;
        friend class pending_sorted_search;

    private:
        size_t prepare_checks(const char* space, const schema& sc,
                              const hyperdex_client_attribute_check* chks, size_t chks_sz,
                              arena_t* allocate,
                              hyperdex_client_returncode* status,
                              std::vector<attribute_check>* checks);
        size_t prepare_funcs(const char* space, const schema& sc,
                             const hyperdex_client_keyop_info* opinfo,
                             const hyperdex_client_attribute* attrs, size_t attrs_sz,
                             arena_t* allocate,
                             hyperdex_client_returncode* status,
                             std::vector<funcall>* funcs);
        size_t prepare_funcs(const char* space, const schema& sc,
                             const hyperdex_client_keyop_info* opinfo,
                             const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                             arena_t* allocate,
                             hyperdex_client_returncode* status,
                             std::vector<funcall>* funcs);
        size_t prepare_searchop(const schema& sc,
                                const char* space,
                                const hyperdex_client_attribute_check* chks, size_t chks_sz,
                                arena_t* allocate,
                                hyperdex_client_returncode* status,
                                std::vector<attribute_check>* checks,
                                std::vector<virtual_server_id>* servers);
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
        coordinator_link m_coord;
        e::garbage_collector m_gc;
        e::garbage_collector::thread_state m_gc_ts;
        mapper m_busybee_mapper;
        busybee_st m_busybee;
        int64_t m_next_client_id;
        uint64_t m_next_server_nonce;
        pending_map_t m_pending_ops;
        pending_queue_t m_failed;
        std::list<e::intrusive_ptr<pending> > m_yieldable;
        e::intrusive_ptr<pending> m_yielding;
        e::intrusive_ptr<pending> m_yielded;
        e::error m_last_error;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_client_client_h_
