// Copyright (c) 2012-2013, Cornell University
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

// STL
#include <map>
#include <tr1/memory>

// po6
#include <po6/net/location.h>

// Replicant
#include <replicant_state_machine.h>

// HyperDex
#include "namespace.h"
#include "common/hyperspace.h"
#include "common/ids.h"
#include "common/server.h"
#include "common/transfer.h"
#include "coordinator/region_intent.h"
#include "coordinator/replica_sets.h"
#include "coordinator/server_barrier.h"

BEGIN_HYPERDEX_NAMESPACE

class coordinator
{
    public:
        coordinator();
        ~coordinator() throw ();

    // identity
    public:
        void init(replicant_state_machine_context* ctx, uint64_t token);
        uint64_t cluster() const { return m_cluster; }

    // server management
    public:
        void server_register(replicant_state_machine_context* ctx,
                             const server_id& sid,
                             const po6::net::location& bind_to);
        void server_online(replicant_state_machine_context* ctx,
                           const server_id& sid,
                           const po6::net::location* bind_to);
        void server_offline(replicant_state_machine_context* ctx,
                            const server_id& sid);
        void server_shutdown(replicant_state_machine_context* ctx,
                             const server_id& sid);
        void server_kill(replicant_state_machine_context* ctx,
                         const server_id& sid);
        void server_forget(replicant_state_machine_context* ctx,
                           const server_id& sid);
        void server_suspect(replicant_state_machine_context* ctx,
                            const server_id& sid, uint64_t version);

    // space management
    public:
        void space_add(replicant_state_machine_context* ctx, const space& s);
        void space_rm(replicant_state_machine_context* ctx, const char* name);

    // transfers management
    public:
        void transfer_go_live(replicant_state_machine_context* ctx,
                              uint64_t version,
                              const transfer_id& xid);
        void transfer_complete(replicant_state_machine_context* ctx,
                               uint64_t version,
                               const transfer_id& xid);

    // config management
    public:
        void config_get(replicant_state_machine_context* ctx);
        void config_ack(replicant_state_machine_context* ctx,
                        const server_id& sid, uint64_t version);
        void config_stable(replicant_state_machine_context* ctx,
                           const server_id& sid, uint64_t version);

    // checkpoint management
    public:
        void checkpoint(replicant_state_machine_context* ctx);
        void checkpoint_stable(replicant_state_machine_context* ctx,
                               const server_id& sid,
                               uint64_t config,
                               uint64_t number);

    // alarm
    public:
        void alarm(replicant_state_machine_context* ctx);

    // debug
    public:
        void debug_dump(replicant_state_machine_context* ctx);

    private:
        typedef std::tr1::shared_ptr<space> space_ptr;
        typedef std::map<std::string, space_ptr > space_map_t;

    // utilities
    private:
        // servers
        server* new_server(const server_id& sid);
        server* get_server(const server_id& sid);
        // replica sets
        void rebalance_replica_sets(replicant_state_machine_context* ctx);
        // hyperspace
        void initial_space_layout(replicant_state_machine_context* ctx,
                                  space* s);
        region* get_region(const region_id& rid);
        // intents
        void setup_intents(replicant_state_machine_context* ctx,
                           const std::vector<replica_set>& replica_sets,
                           space* s, bool skip_transfers);
        // looks up region_intent* ri, removing any possibility of the user
        // using an invalid pointer
        void converge_intent(replicant_state_machine_context* ctx,
                             region* reg);
        // ri and m_intents may be changed after this call
        void converge_intent(replicant_state_machine_context* ctx,
                             region* reg, region_intent* ri);
        region_intent* new_region_intent(const region_id& rid);
        region_intent* get_region_intent(const region_id& rid);
        void del_region_intent(const region_id& rid);
        // transfers
        transfer* new_transfer(region* reg, const server_id& si);
        transfer* get_transfer(const region_id& rid);
        transfer* get_transfer(const transfer_id& xid);
        void del_transfer(const transfer_id& xid);
        // configuration
        void check_ack_condition(replicant_state_machine_context* ctx);
        void check_stable_condition(replicant_state_machine_context* ctx);
        void generate_next_configuration(replicant_state_machine_context* ctx);
        void servers_in_configuration(std::vector<server_id>* sids);
        void regions_in_space(space_ptr s, std::vector<region_id>* rids);
        // checkpoints
        void check_checkpoint_stable_condition(replicant_state_machine_context* ctx);

    private:
        // meta state
        uint64_t m_cluster;
        uint64_t m_counter;
        uint64_t m_version;
        // servers
        std::vector<server> m_servers;
        // replica sets
        std::vector<server_id> m_permutation;
        std::vector<server_id> m_spares;
        uint64_t m_desired_spares;
        // spaces
        space_map_t m_spaces;
        // intents
        std::vector<region_intent> m_intents;
        std::vector<space_id> m_deferred_init;
        // transfers
        std::vector<transfer> m_transfers;
        // barriers
        uint64_t m_config_ack_through;
        server_barrier m_config_ack_barrier;
        uint64_t m_config_stable_through;
        server_barrier m_config_stable_barrier;
        // checkpoints
        uint64_t m_checkpoint;
        uint64_t m_checkpoint_stable_through;
        uint64_t m_checkpoint_gc_through;
        server_barrier m_checkpoint_stable_barrier;
        // cached config
        std::auto_ptr<e::buffer> m_latest_config;

    private:
        coordinator(const coordinator&);
        coordinator& operator = (const coordinator&);
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_coordinator_coordinator_h_
