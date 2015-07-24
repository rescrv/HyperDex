// Copyright (c) 2011-2014, Cornell University
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

#ifndef hyperdex_daemon_replication_manager_h_
#define hyperdex_daemon_replication_manager_h_

// STL
#include <list>

// po6
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/buffer.h>
#include <e/compat.h>
#include <e/intrusive_ptr.h>
#include <e/nwf_hash_map.h>

// HyperDex
#include "namespace.h"
#include "common/attribute_check.h"
#include "common/configuration.h"
#include "common/funcall.h"
#include "common/ids.h"
#include "common/key_change.h"
#include "common/network_returncode.h"
#include "daemon/identifier_collector.h"
#include "daemon/identifier_generator.h"
#include "daemon/key_operation.h"
#include "daemon/key_region.h"
#include "daemon/key_state.h"
#include "daemon/reconfigure_returncode.h"
#include "daemon/region_timestamp.h"
#include "daemon/state_hash_table.h"

BEGIN_HYPERDEX_NAMESPACE
class daemon;

// Manage replication.
class replication_manager
{
    public:
        replication_manager(daemon*);
        ~replication_manager() throw ();

    // Reconfigure this layer.
    public:
        bool setup();
        void teardown();
        void pause();
        void unpause();
        void reconfigure(const configuration& old_config,
                         const configuration& new_config,
                         const server_id& us);
        void debug_dump();

    // Network workers call these methods.
    public:
        // These are called when the client initiates the action.  This implies
        // that only the point leader should call these methods.
        void client_atomic(const server_id& from,
                           const virtual_server_id& to,
                           uint64_t nonce,
                           std::auto_ptr<key_change> kc,
                           std::auto_ptr<e::buffer> backing);
        // These are called in response to messages from other hosts.
        void chain_op(const virtual_server_id& from,
                      const virtual_server_id& to,
                      uint64_t old_version,
                      uint64_t new_version,
                      bool fresh,
                      bool has_value,
                      const e::slice& key,
                      const std::vector<e::slice>& value,
                      std::auto_ptr<e::buffer> backing);
        void chain_subspace(const virtual_server_id& from,
                            const virtual_server_id& to,
                            uint64_t old_version,
                            uint64_t new_version,
                            const e::slice& key,
                            const std::vector<e::slice>& value,
                            std::auto_ptr<e::buffer> backing,
                            const region_id& prev_region,
                            const region_id& this_old_region,
                            const region_id& this_new_region,
                            const region_id& next_region);
        void chain_ack(const virtual_server_id& from,
                       const virtual_server_id& to,
                       uint64_t version,
                       const e::slice& key);
        void begin_checkpoint(uint64_t seq);
        void end_checkpoint(uint64_t seq);

    private:
        class retransmitter_thread;
        typedef state_hash_table<key_region, key_state> key_map_t;
        friend class key_state;

    private:
        // Get the state for the specified key.
        // Returns NULL if there is no key_state that's currently in use.
        key_state* get_key_state(const region_id& ri,
                                 const e::slice& key,
                                 key_map_t::state_reference* ksr);
        // Get the state for the specified key.
        // Will retrieve the state from disk and create the key_state when
        // necessary.
        key_state* get_or_create_key_state(const region_id& ri,
                                           const e::slice& key,
                                           key_map_t::state_reference* ksr);
        key_state* post_get_key_state_init(region_id ri, key_state* ks);
        void respond_to_client(const virtual_server_id& us,
                               const server_id& client,
                               uint64_t nonce,
                               network_returncode ret);
        bool send_message(const virtual_server_id& us,
                          const e::slice& key,
                          e::intrusive_ptr<key_operation> op);
        bool send_ack(const virtual_server_id& us,
                      const e::slice& key,
                      e::intrusive_ptr<key_operation> op);
        void retransmit(const std::vector<region_id>& point_leaders,
                        std::vector<std::pair<region_id, uint64_t> >* versions);
        void collect(const region_id& ri, e::intrusive_ptr<key_operation> op);
        void collect(const region_id& ri, uint64_t version);
        void close_gaps(const std::vector<region_id>& point_leaders,
                        const identifier_generator& peek_ids,
                        std::vector<std::pair<region_id, uint64_t> >* versions);
        // call reset_to_unstable holding m_protect_stable_stuff
        void reset_to_unstable();
        bool is_check_needed() { return e::atomic::compare_and_swap_32_nobarrier(&m_need_check, 0, 0) == 1; }
        void check_is_needed() { e::atomic::compare_and_swap_32_nobarrier(&m_need_check, 0, 1); }
        void check_is_not_needed() { e::atomic::compare_and_swap_32_nobarrier(&m_need_check, 1, 0); }
        void check_stable();
        void check_stable(const region_id& ri);

    private:
        daemon* m_daemon;
        key_map_t m_key_states;
        identifier_generator m_idgen;
        identifier_collector m_idcol;
        identifier_generator m_stable;
        const std::auto_ptr<retransmitter_thread> m_retransmitter;
        po6::threads::mutex m_protect_stable_stuff;
        uint64_t m_checkpoint;
        uint32_t m_need_check;
        std::vector<region_timestamp> m_timestamps;
        std::vector<region_id> m_unstable;

    private:
        replication_manager(const replication_manager&);
        replication_manager& operator = (const replication_manager&);
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_replication_manager_h_
