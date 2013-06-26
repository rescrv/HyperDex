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

#ifndef hyperdex_client_client_h_
#define hyperdex_client_client_h_

// STL
#include <map>
#include <list>

// BusyBee
#include <busybee_st.h>

// HyperDex
#include "namespace.h"
#include "common/configuration.h"
#include "common/mapper.h"
#include "client/coordinator_link.h"
#include "client/hyperclient.h"
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
        hyperclient_returncode add_space(const char* description);
        hyperclient_returncode rm_space(const char* space);

    public:
        // specific calls
        int64_t get(const char* space, const char* key, size_t key_sz,
                    hyperclient_returncode* status,
                    hyperclient_attribute** attrs, size_t* attrs_sz);
        int64_t search(const char* space,
                       const struct hyperclient_attribute_check* checks, size_t checks_sz,
                       enum hyperclient_returncode* status,
                       struct hyperclient_attribute** attrs, size_t* attrs_sz);
        int64_t search_describe(const char* space,
                                const struct hyperclient_attribute_check* checks, size_t checks_sz,
                                enum hyperclient_returncode* status, const char** description);
        int64_t sorted_search(const char* space,
                              const struct hyperclient_attribute_check* checks, size_t checks_sz,
                              const char* sort_by,
                              uint64_t limit,
                              bool maximize,
                              enum hyperclient_returncode* status,
                              struct hyperclient_attribute** attrs, size_t* attrs_sz);
        int64_t group_del(const char* space,
                          const struct hyperclient_attribute_check* checks, size_t checks_sz,
                          enum hyperclient_returncode* status);
        int64_t count(const char* space,
                      const struct hyperclient_attribute_check* checks, size_t checks_sz,
                      enum hyperclient_returncode* status, uint64_t* result);
        // general keyop call
        int64_t perform_funcall(const hyperclient_keyop_info* opinfo,
                                const char* space, const char* key, size_t key_sz,
                                const hyperclient_attribute_check* checks, size_t checks_sz,
                                const hyperclient_attribute* attrs, size_t attrs_sz,
                                const hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                                hyperclient_returncode* status);
        // looping/polling
        int64_t loop(int timeout, hyperclient_returncode* status);

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
        friend class pending_search;
        friend class pending_sorted_search;

    private:
        size_t prepare_checks(const schema& sc,
                              const hyperclient_attribute_check* chks, size_t chks_sz,
                              hyperclient_returncode* status,
                              std::vector<attribute_check>* checks);
        size_t prepare_funcs(const schema& sc,
                             const hyperclient_keyop_info* opinfo,
                             const hyperclient_attribute* attrs, size_t attrs_sz,
                             hyperclient_returncode* status,
                             std::vector<funcall>* funcs);
        size_t prepare_funcs(const schema& sc,
                             const hyperclient_keyop_info* opinfo,
                             const hyperclient_map_attribute* mapattrs, size_t mapattrs_sz,
                             hyperclient_returncode* status,
                             std::vector<funcall>* funcs);
        size_t prepare_searchop(const schema& sc,
                                const char* space,
                                const struct hyperclient_attribute_check* chks, size_t chks_sz,
                                hyperclient_returncode* status,
                                std::vector<attribute_check>* checks,
                                std::vector<virtual_server_id>* servers);
        int64_t perform_aggregation(const std::vector<virtual_server_id>& servers,
                                    e::intrusive_ptr<pending_aggregation> op,
                                    network_msgtype mt,
                                    std::auto_ptr<e::buffer> msg,
                                    hyperclient_returncode* status);
        bool maintain_coord_connection(hyperclient_returncode* status);
        bool send(network_msgtype mt,
                  const virtual_server_id& to,
                  uint64_t nonce,
                  std::auto_ptr<e::buffer> msg,
                  e::intrusive_ptr<pending> op,
                  hyperclient_returncode* status);
        int64_t send_keyop(const char* space,
                           const e::slice& key,
                           network_msgtype mt,
                           std::auto_ptr<e::buffer> msg,
                           e::intrusive_ptr<pending> op,
                           hyperclient_returncode* status);
        void handle_disruption(const server_id& si);

    private:
        configuration m_config;
        bool m_have_seen_config;
        mapper m_busybee_mapper;
        busybee_st m_busybee;
        coordinator_link m_coord;
        int64_t m_next_client_id;
        uint64_t m_next_server_nonce;
        pending_map_t m_pending_ops;
        pending_queue_t m_failed;
        std::list<e::intrusive_ptr<pending> > m_yieldable;
        e::intrusive_ptr<pending> m_yielding;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_client_client_h_
