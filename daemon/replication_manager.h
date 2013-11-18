// Copyright (c) 2011-2012, Cornell University
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
#include <tr1/memory>
#include <tr1/unordered_map>

// po6
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/buffer.h>
#include <e/intrusive_ptr.h>
#include <e/lockfree_hash_map.h>
#include <e/striped_lock.h>

// HyperDex
#include "namespace.h"
#include "common/attribute_check.h"
#include "common/configuration.h"
#include "common/funcall.h"
#include "common/ids.h"
#include "common/network_returncode.h"
#include "daemon/identifier_collector.h"
#include "daemon/identifier_generator.h"
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
                           bool erase,
                           bool fail_if_not_found,
                           bool fail_if_found,
                           const e::slice& key,
                           const std::vector<attribute_check>& checks,
                           const std::vector<funcall>& funcs);
        // These are called in response to messages from other hosts.
        void chain_op(const virtual_server_id& from,
                      const virtual_server_id& to,
                      bool retransmission,
                      const region_id& reg_id,
                      uint64_t seq_id,
                      uint64_t new_version,
                      bool fresh,
                      bool has_value,
                      std::auto_ptr<e::buffer> backing,
                      const e::slice& key,
                      const std::vector<e::slice>& value);
        void chain_subspace(const virtual_server_id& from,
                            const virtual_server_id& to,
                            bool retransmission,
                            const region_id& reg_id,
                            uint64_t seq_id,
                            uint64_t version,
                            std::auto_ptr<e::buffer> backing,
                            const e::slice& key,
                            const std::vector<e::slice>& value,
                            const std::vector<uint64_t>& hashes);
        void chain_ack(const virtual_server_id& from,
                       const virtual_server_id& to,
                       bool retransmission,
                       const region_id& reg_id,
                       uint64_t seq_id,
                       uint64_t version,
                       const e::slice& key);
        void chain_gc(const region_id& reg_id, uint64_t seq_id);
        void trip_periodic();
        void begin_checkpoint(uint64_t seq);
        void end_checkpoint(uint64_t seq);

    private:
        class pending; // state for one pending operation
        class key_region; // a tuple of (key, region)
        class key_state; // state for a single key
        typedef state_hash_table<key_region, key_state> key_map_t;
        friend class std::tr1::hash<key_region>;

    private:
        replication_manager(const replication_manager&);
        replication_manager& operator = (const replication_manager&);

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
        // Send a response to the specified client.
        void send_message(const virtual_server_id& us,
                          bool retransmission,
                          uint64_t version,
                          const e::slice& key,
                          e::intrusive_ptr<pending> op);
        bool send_ack(const virtual_server_id& us,
                      const virtual_server_id& to,
                      bool retransmission,
                      const region_id& reg_id,
                      uint64_t seq_id,
                      uint64_t version,
                      const e::slice& key);
        void respond_to_client(const virtual_server_id& us,
                               const server_id& client,
                               uint64_t nonce,
                               network_returncode ret);
        // check stability
        bool is_check_needed();
        void check_is_needed();
        void check_is_not_needed();
        // stability
        void check_stable();
        void check_stable(const region_id& ri);
        // background thread
        void wait_until_paused();
        void background_thread();
        void retransmit(const std::vector<region_id>& point_leaders,
                        std::vector<std::pair<region_id, uint64_t> >* seq_ids);
        void close_gaps(const std::vector<region_id>& point_leaders,
                        const identifier_generator& peek_ids,
                        std::vector<std::pair<region_id, uint64_t> >* seq_ids);
        void send_chain_gc();
        void shutdown();

    private:
        daemon* m_daemon;
        key_map_t m_key_states;
        identifier_generator m_idgen;
        identifier_collector m_idcol;
        identifier_generator m_stable_counters;
        std::vector<region_id> m_unstable_regions;
        uint64_t m_checkpoint;
        uint32_t m_need_check;
        std::vector<region_timestamp> m_timestamps;
        po6::threads::thread m_background_thread;
        po6::threads::mutex m_block_background_thread;
        po6::threads::cond m_wakeup_background_thread;
        po6::threads::cond m_wakeup_reconfigurer;
        bool m_shutdown;
        bool m_need_pause;
        bool m_paused;
        bool m_need_post_reconfigure;
        bool m_need_periodic;
        std::list<std::pair<region_id, uint64_t> > m_lower_bounds;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_replication_manager_h_
