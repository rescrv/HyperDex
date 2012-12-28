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
#include <memory>

// po6
#include <po6/threads/mutex.h>

// e
#include <e/buffer.h>
#include <e/intrusive_ptr.h>
#include <e/lockfree_hash_map.h>
#include <e/striped_lock.h>

// HyperDex
#include "common/attribute_check.h"
#include "common/configuration.h"
#include "common/funcall.h"
#include "common/network_returncode.h"
#include "common/server_id.h"
#include "common/virtual_server_id.h"
#include "daemon/reconfigure_returncode.h"

namespace hyperdex
{
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
        reconfigure_returncode prepare(const configuration& old_config,
                                       const configuration& new_config,
                                       const server_id& us);
        reconfigure_returncode reconfigure(const configuration& old_config,
                                           const configuration& new_config,
                                           const server_id& us);
        reconfigure_returncode cleanup(const configuration& old_config,
                                       const configuration& new_config,
                                       const server_id& us);

    // Network workers call these methods.
    public:
        // These are called when the client initiates the action.  This implies
        // that only the point leader should call these methods.
        void client_atomic(const server_id& from,
                           const virtual_server_id& to,
                           uint64_t nonce,
                           bool fail_if_not_found,
                           bool fail_if_found,
                           bool erase,
                           const e::slice& key,
                           std::vector<attribute_check>* checks,
                           std::vector<funcall>* funcs);
        // These are called in response to messages from other hosts.
        void chain_put(const virtual_server_id& from,
                       const virtual_server_id& to,
                       bool retransmission,
                       uint64_t reg_id,
                       uint64_t seq_id,
                       uint64_t version,
                       bool fresh,
                       std::auto_ptr<e::buffer> backing,
                       const e::slice& key,
                       const std::vector<e::slice>& value);
        void chain_del(const virtual_server_id& from,
                       const virtual_server_id& to,
                       bool retransmission,
                       uint64_t reg_id,
                       uint64_t seq_id,
                       uint64_t version,
                       std::auto_ptr<e::buffer> backing,
                       const e::slice& key);
        void chain_subspace(const virtual_server_id& from,
                            const virtual_server_id& to,
                            bool retransmission,
                            uint64_t reg_id,
                            uint64_t seq_id,
                            uint64_t version,
                            std::auto_ptr<e::buffer> backing,
                            const e::slice& key,
                            const std::vector<e::slice>& value,
                            const std::vector<uint64_t>& hashes);
        void chain_ack(const virtual_server_id& from,
                       const virtual_server_id& to,
                       uint64_t reg_id,
                       uint64_t seq_id,
                       uint64_t version,
                       const e::slice& key);

    private:
        class pending;
        class keyholder;
        class keypair;
        static uint64_t hash(const keypair&);
        typedef e::lockfree_hash_map<keypair, e::intrusive_ptr<keyholder>, hash> keyholder_map_t;

    private:
        replication_manager(const replication_manager&);

    private:
        replication_manager& operator = (const replication_manager&);

    private:
        void chain_common(bool has_value,
                          const virtual_server_id& from,
                          const virtual_server_id& to,
                          bool retransmission,
                          uint64_t reg_id,
                          uint64_t seq_id,
                          uint64_t new_version,
                          bool fresh,
                          std::auto_ptr<e::buffer> backing,
                          const e::slice& key,
                          const std::vector<e::slice>& newvalue);
        uint64_t get_lock_num(const region_id& reg, const e::slice& key);
        e::intrusive_ptr<keyholder> get_keyholder(const region_id& reg, const e::slice& key);
        e::intrusive_ptr<keyholder> get_or_create_keyholder(const region_id& reg, const e::slice& key);
        void erase_keyholder(const region_id& reg, const e::slice& key);
        void hash_objects(const region_id& reg,
                          const schema& sc,
                          const e::slice& key,
                          bool has_new_value,
                          const std::vector<e::slice>& new_value,
                          bool has_old_value,
                          const std::vector<e::slice>& old_value,
                          e::intrusive_ptr<pending> pend);
        bool prev_and_next(const region_id& ri, const e::slice& key,
                           bool has_new_value, const std::vector<e::slice>& new_value,
                           bool has_old_value, const std::vector<e::slice>& old_value,
                           e::intrusive_ptr<pending> pend);
        // Move operations between the queues in the keyholder.  Blocked
        // operations will have their blocking criteria checked.  Deferred
        // operations will be checked for continuity with the blocked
        // operations.
        void move_operations_between_queues(const virtual_server_id& us,
                                            const region_id& ri,
                                            const schema& sc,
                                            const e::slice& key,
                                            e::intrusive_ptr<keyholder> kh);
        void send_message(const virtual_server_id& us,
                          uint64_t version,
                          const e::slice& key,
                          e::intrusive_ptr<pending> op);
        bool send_ack(const virtual_server_id& us,
                      const virtual_server_id& to,
                      uint64_t version,
                      const e::slice& key);
        void respond_to_client(const virtual_server_id& us,
                               const server_id& client,
                               uint64_t nonce,
                               network_returncode ret);
        bool check_acked(uint64_t reg_id, uint64_t seq_id);

    private:
        daemon* m_daemon;
        e::striped_lock<po6::threads::mutex> m_locks;
        keyholder_map_t m_keyholders;
        uint64_t m_counter;
        po6::threads::mutex m_acked_lock;
        std::set<std::pair<uint64_t, uint64_t> > m_acked;
};

} // namespace hyperdex

#endif // hyperdex_daemon_replication_manager_h_
