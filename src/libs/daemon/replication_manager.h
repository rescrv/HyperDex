// Copyright (c) 2011, Cornell University
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
#include "hyperdex/configuration.h"
#include "hyperdex/network_constants.h"

// HyperDaemon
#include "replication/clientop.h"
#include "replication/keypair.h"

// Forward Declarations
namespace hyperdex
{
class coordinatorlink;
class instance;
}
namespace hyperdaemon
{
class datalayer;
class logical;
class ongoing_state_transfers;
}

namespace hyperdaemon
{

// Manage replication.
class replication_manager
{
    public:
        replication_manager(hyperdex::coordinatorlink* cl,
                            datalayer* dl,
                            logical* comm,
                            ongoing_state_transfers* ost);
        ~replication_manager() throw ();

    // Reconfigure this layer.
    public:
        void prepare(const hyperdex::configuration& newconfig, const hyperdex::instance& us);
        void reconfigure(const hyperdex::configuration& newconfig, const hyperdex::instance& us);
        void cleanup(const hyperdex::configuration& newconfig, const hyperdex::instance& us);
        void shutdown();

    // Netowrk workers call these methods.
    public:
        // These are called when the client initiates the action.  This implies
        // that only the point leader will call these methods.
        void client_put(const hyperdex::entityid& from,
                        const hyperdex::entityid& to,
                        uint64_t nonce,
                        std::auto_ptr<e::buffer> backing,
                        const e::slice& key,
                       const std::vector<std::pair<uint16_t, e::slice> >& value);
        void client_del(const hyperdex::entityid& from,
                        const hyperdex::entityid& to,
                        uint64_t nonce,
                        std::auto_ptr<e::buffer> backing,
                        const e::slice& key);
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

    private:
        replication_manager(const replication_manager&);

    private:
        replication_manager& operator = (const replication_manager&);

    private:
        void client_common(bool has_value,
                           const hyperdex::entityid& from,
                           const hyperdex::entityid& to,
                           uint64_t nonce,
                           std::auto_ptr<e::buffer> backing,
                           const e::slice& key,
                           const e::bitfield& newvalue_mask,
                           const std::vector<e::slice>& newvalue);
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
        bool from_disk(const hyperdex::regionid& r, const e::slice& key,
                       bool* has_value, std::vector<e::slice>* value,
                       uint64_t* version, hyperdisk::reference* ref);
        bool put_to_disk(const hyperdex::regionid& pending_in,
                         e::intrusive_ptr<keyholder> kh,
                         uint64_t version);
        // Figure out the previous and next individuals to send to/receive from
        // for messages.
        bool prev_and_next(const hyperdex::regionid& r, const e::slice& key,
                           bool has_newvalue, const std::vector<e::slice>& newvalue,
                           bool has_oldvalue, const std::vector<e::slice>& oldvalue,
                           e::intrusive_ptr<pending> pend);
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
        void retransmit();

    private:
        hyperdex::coordinatorlink* m_cl;
        datalayer* m_data;
        logical* m_comm;
        ongoing_state_transfers* m_ost;
        hyperdex::configuration m_config;
        e::striped_lock<po6::threads::mutex> m_locks;
        keyholder_map_t m_keyholders;
        hyperdex::instance m_us;
        bool m_shutdown;
        po6::threads::thread m_periodic_thread;
};

} // namespace hyperdaemon

#endif // hyperdaemon_replication_manager_h_
