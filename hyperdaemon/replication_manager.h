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

#ifndef hyperdaemon_replication_manager_h_
#define hyperdaemon_replication_manager_h_

// STL
#include <limits>
#include <tr1/functional>
#include <tr1/unordered_map>

// po6
#include <po6/threads/rwlock.h>

// e
#include <e/bitfield.h>
#include <e/intrusive_ptr.h>
#include <e/lockfree_hash_set.h>
#include <e/striped_lock.h>

// HyperDex
#include "daemon/reconfigure_returncode.h"
#include "hyperdex/hyperdex/configuration.h"
#include "hyperdex/hyperdex/network_constants.h"
#include "hyperdaemon/replication/clientop.h"
#include "hyperdaemon/replication/keypair.h"

// Forward Declarations
namespace hyperdex
{
class attribute_check;
class coordinatorlink;
class daemon;
class datalayer;
class funcall;
class instance;
}
namespace hyperdaemon
{
class logical;
class ongoing_state_transfers;
}

namespace hyperdaemon
{

// Manage replication.
class replication_manager
{
    public:
        replication_manager(hyperdex::daemon*);
        ~replication_manager() throw ();

    // Reconfigure this layer.
    public:
        hyperdex::reconfigure_returncode prepare(const hyperdex::configuration& old_config,
                                                 const hyperdex::configuration& new_config,
                                                 const hyperdex::instance& us);
        hyperdex::reconfigure_returncode reconfigure(const hyperdex::configuration& old_config,
                                                     const hyperdex::configuration& new_config,
                                                     const hyperdex::instance& us);
        hyperdex::reconfigure_returncode cleanup(const hyperdex::configuration& old_config,
                                                 const hyperdex::configuration& new_config,
                                                 const hyperdex::instance& us);

    // Network workers call these methods.
    public:
        // These are called when the client initiates the action.  This implies
        // that only the point leader should call these methods.
        void client_atomic(const hyperdex::network_msgtype opcode,
                           const hyperdex::entityid& from,
                           const hyperdex::entityid& to,
                           uint64_t nonce,
                           std::auto_ptr<e::buffer> backing,
                           bool fail_if_not_found,
                           bool fail_if_found,
                           const e::slice& key,
                           std::vector<hyperdex::attribute_check>* checks,
                           std::vector<hyperdex::funcall>* funcs);
        void client_del(const hyperdex::network_msgtype opcode,
                        const hyperdex::entityid& from,
                        const hyperdex::entityid& to,
                        uint64_t nonce,
                        std::auto_ptr<e::buffer> backing,
                        const e::slice& key,
                        std::vector<hyperdex::attribute_check>* checks);
        // These are called in response to messages from other hosts.
        void chain_put(const hyperdex::entityid& from,
                       const hyperdex::entityid& to,
                       uint64_t rev,
                       bool fresh,
                       std::auto_ptr<e::buffer> backing,
                       const e::slice& key,
                       const std::vector<e::slice>& value);
        void chain_del(const hyperdex::entityid& from,
                       const hyperdex::entityid& to,
                       uint64_t rev,
                       std::auto_ptr<e::buffer> backing,
                       const e::slice& key);
        void chain_subspace(const hyperdex::entityid& from,
                            const hyperdex::entityid& to,
                            uint64_t rev,
                            std::auto_ptr<e::buffer> backing,
                            const e::slice& key,
                            const std::vector<e::slice>& value,
                            uint64_t nextpoint);
        void chain_ack(const hyperdex::entityid& from,
                       const hyperdex::entityid& to,
                       uint64_t rev,
                       std::auto_ptr<e::buffer> backing,
                       const e::slice& key);

    private:
        class deferred;
        class pending;
        class keyholder;
        typedef e::lockfree_hash_map<replication::keypair, e::intrusive_ptr<keyholder>, replication::keypair::hash>
                keyholder_map_t;
        friend class ongoing_state_transfers;

    private:
        replication_manager(const replication_manager&);

    private:
        replication_manager& operator = (const replication_manager&);

    private:
        void chain_common(bool has_value,
                          const hyperdex::entityid& from,
                          const hyperdex::entityid& to,
                          uint64_t newversion,
                          bool fresh,
                          std::auto_ptr<e::buffer> backing,
                          const e::slice& key,
                          const std::vector<e::slice>& newvalue);
        uint64_t get_lock_num(const hyperdex::regionid& reg, const e::slice& key);
        e::intrusive_ptr<keyholder> get_keyholder(const hyperdex::regionid& reg, const e::slice& key);
        void erase_keyholder(const hyperdex::regionid& reg, const e::slice& key);
        bool retrieve_latest(const hyperdex::regionid& reg,
                             const e::slice& key,
                             e::intrusive_ptr<keyholder> kh,
                             uint64_t* old_version,
                             bool* has_old_value,
                             std::vector<e::slice>* old_value,
                             hyperdex::datalayer::reference* ref);
        bool from_disk(const hyperdex::regionid& r, const e::slice& key,
                       bool* has_value, std::vector<e::slice>* value,
                       uint64_t* version, hyperdex::datalayer::reference* ref);
        bool put_to_disk(const hyperdex::regionid& pending_in,
                         e::intrusive_ptr<keyholder> kh,
                         uint64_t version);
        // Figure out the previous and next individuals to send to/receive from
        // for messages.
        bool prev_and_next(const hyperdex::regionid& r, const e::slice& key,
                           bool has_newvalue, const std::vector<e::slice>& newvalue,
                           bool has_oldvalue, const std::vector<e::slice>& oldvalue,
                           e::intrusive_ptr<pending> pend);
        // Heal problems that result from ongoing state transfers.  The kind of
        // problem that can arise stems from operations being labeled "deferred"
        // when the background state transfer doesn't arrive before the op is
        // enqueued.  This requires that the lock for the key/region be held.
        void check_for_deferred_operations(const hyperdex::regionid& r,
                                           uint64_t version,
                                           std::tr1::shared_ptr<e::buffer> backing,
                                           const e::slice& key,
                                           bool has_value,
                                           const std::vector<e::slice>& value);
        // Move operations between the queues in the keyholder.  Blocked
        // operations will have their blocking criteria checked.  Deferred
        // operations will be checked for continuity with the blocked
        // operations.
        void move_operations_between_queues(const hyperdex::entityid& us,
                                            const e::slice& key,
                                            e::intrusive_ptr<keyholder> kh);
        void send_message(const hyperdex::entityid& us,
                          uint64_t version,
                          const e::slice& key,
                          e::intrusive_ptr<pending> op);
        bool send_ack(const hyperdex::entityid& us,
                      const hyperdex::entityid& to,
                      uint64_t version,
                      const e::slice& key);
        void respond_to_client(const hyperdex::entityid& us,
                               const hyperdex::entityid& client,
                               uint64_t nonce,
                               hyperdex::network_msgtype type,
                               hyperdex::network_returncode ret);
        // Periodically do things related to replication.
        void periodic();
        // Retransmit current pending values.
        int retransmit();

    private:
        hyperdex::daemon* m_daemon;
        e::striped_lock<po6::threads::mutex> m_locks;
        po6::threads::mutex m_keyholders_lock;
        keyholder_map_t m_keyholders;
        volatile uint32_t m_shutdown;
        po6::threads::thread m_periodic_thread;
};

} // namespace hyperdaemon

#endif // hyperdaemon_replication_manager_h_
