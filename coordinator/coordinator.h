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

#ifndef hyperdex_coordinator_coordinator_h_
#define hyperdex_coordinator_coordinator_h_

// C
#include <cstdlib>

// STL
#include <list>
#include <map>
#include <memory>
#include <utility>
#include <vector>

// po6
#include <po6/net/location.h>

// Replicant
#include <replicant_state_machine.h>

// HyperDex
#include "namespace.h"
#include "common/capture.h"
#include "common/hyperspace.h"
#include "common/ids.h"
#include "common/transfer.h"
#include "coordinator/missing_acks.h"
#include "coordinator/server_state.h"

BEGIN_HYPERDEX_NAMESPACE

class coordinator
{
    public:
        coordinator();
        ~coordinator() throw ();

    public:
        uint64_t cluster() const;

    public:
        // Setup the cluster
        void initialize(replicant_state_machine_context* ctx, uint64_t token);
        // Manage spaces
        void add_space(replicant_state_machine_context* ctx, const space& s);
        void rm_space(replicant_state_machine_context* ctx, const char* name);
        // Issue configs
        void get_config(replicant_state_machine_context* ctx);
        void ack_config(replicant_state_machine_context* ctx, const server_id&, uint64_t version);
        // Manage cluster membership
        void server_register(replicant_state_machine_context* ctx,
                             const server_id& sid,
                             const po6::net::location& bind_to);
        void server_reregister(replicant_state_machine_context* ctx,
                               const server_id& sid,
                               const po6::net::location& bind_to);
        void server_suspect(replicant_state_machine_context* ctx,
                            const server_id& sid, uint64_t version);
        void server_shutdown1(replicant_state_machine_context* ctx,
                              const server_id& sid);
        void server_shutdown2(replicant_state_machine_context* ctx,
                              const server_id& sid);
        // Transfers
        void xfer_begin(replicant_state_machine_context* ctx,
                        const region_id& rid,
                        const server_id& sid);
        void xfer_go_live(replicant_state_machine_context* ctx,
                          const transfer_id& xid);
        void xfer_complete(replicant_state_machine_context* ctx,
                           const transfer_id& xid);

    private:
        // servers
        server_state* get_state(const server_id& sid);
        bool is_registered(const server_id& sid);
        bool is_registered(const po6::net::location& bind_to);
        // regions
        region* get_region(const region_id& rid);
        // captures
        capture* new_capture(const region_id& rid);
        capture* get_capture(const region_id& rid);
        // transfers
        transfer* new_transfer(region* reg, const server_id& sid);
        transfer* get_transfer(const region_id& rid);
        transfer* get_transfer(const transfer_id& xid);
        void del_transfer(const transfer_id& xid);
        // capture references
        void add_capture_reference(const server_id& sid, const capture_id& cid);
        void add_capture_reference(const transfer_id& xid, const capture_id& cid);
        void release_capture_reference(const transfer_id& xid);
        void release_capture_references(const server_id& sid);
        void maybe_garbage_collect_references(const capture_id& cid);
        // region references
        void add_region_reference(const server_id& sid, const region_id& rid);
        server_id get_region_reference(const region_id& rid);
        void release_region_reference(const region_id& rid);
        void release_region_references(const server_id& sid);
        // other
        void remove_server(const server_id& sid, bool dry_run, bool shutdown,
                           std::vector<region_id>* rids,
                           std::vector<transfer_id>* xids);
        server_id select_new_server_for(const std::vector<replica>& replicas);
        void issue_new_config(struct replicant_state_machine_context* ctx);
        void initial_layout(struct replicant_state_machine_context* ctx, space* s);
        void maintain_layout(struct replicant_state_machine_context* ctx);
        void regenerate_cached(struct replicant_state_machine_context* ctx);

    private:
        uint64_t m_cluster;
        uint64_t m_version;
        uint64_t m_counter;
        uint64_t m_acked;
        std::vector<server_state> m_servers;
        std::map<std::string, std::tr1::shared_ptr<space> > m_spaces;
        std::vector<capture> m_captures;
        std::vector<transfer> m_transfers;
        std::list<missing_acks> m_missing_acks;
        std::vector<std::pair<capture_id, server_id> > m_capture_server_references;
        std::vector<std::pair<capture_id, transfer_id> > m_capture_transfer_references;
        std::vector<std::pair<region_id, server_id> > m_region_server_references;
        std::auto_ptr<e::buffer> m_latest_config; // cached config
        std::auto_ptr<e::buffer> m_resp; // response space
#ifdef __APPLE__
        unsigned int m_seed;
#else
        drand48_data m_seed;
#endif

    private:
        coordinator(const coordinator&);
        coordinator& operator = (const coordinator&);
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_coordinator_coordinator_h_
