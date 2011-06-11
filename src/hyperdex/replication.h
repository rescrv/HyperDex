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

#ifndef hyperdex_replication_h_
#define hyperdex_replication_h_

// STL
#include <tr1/functional>

// po6
#include <po6/threads/rwlock.h>

// e
#include <e/intrusive_ptr.h>
#include <e/set.h>

// HyperDex
#include <hyperdex/configuration.h>
#include <hyperdex/datalayer.h>
#include <hyperdex/instance.h>
#include <hyperdex/logical.h>
#include <hyperdex/op_t.h>

namespace hyperdex
{

// Manage replication.
class replication
{
    public:
        replication(datalayer* dl, logical* comm);
        ~replication() throw ();

    // Reconfigure this layer.
    public:
        void prepare(const configuration& newconfig, const instance& us);
        void reconfigure(const configuration& newconfig, const instance& us);
        void cleanup(const configuration& newconfig, const instance& us);
        void shutdown();

    // Netowrk workers call these methods.
    public:
        // These are called when the client initiates the action.  This implies
        // that only the point leader will call these methods.
        void client_put(const entityid& from, const entityid& to, uint32_t nonce,
                        const e::buffer& key, const std::vector<e::buffer>& value);
        void client_del(const entityid& from, const entityid& to, uint32_t nonce,
                        const e::buffer& key);
        // These are called in response to messages from other hosts.
        void chain_put(const entityid& from, const entityid& to, uint64_t rev,
                       bool fresh, const e::buffer& key,
                       const std::vector<e::buffer>& value);
        void chain_del(const entityid& from, const entityid& to, uint64_t rev,
                       const e::buffer& key);
        void chain_pending(const entityid& from, const entityid& to,
                           uint64_t rev, const e::buffer& key);
        void chain_ack(const entityid& from, const entityid& to, uint64_t rev,
                       const e::buffer& key);

    private:
        class clientop;
        class keypair;
        class pending;
        class keyholder;

    private:
        replication(const replication&);

    private:
        replication& operator = (const replication&);

    private:
        void client_common(op_t op, const entityid& from, const entityid& to,
                           uint32_t nonce, const e::buffer& key,
                           const std::vector<e::buffer>& newvalue);
        void chain_common(op_t op, const entityid& from, const entityid& to,
                          uint64_t newversion, bool fresh, const e::buffer& key,
                          const std::vector<e::buffer>& newvalue);
        po6::threads::mutex* get_lock(const keypair& kp);
        e::intrusive_ptr<keyholder> get_keyholder(const keypair& kp);
        void erase_keyholder(const keypair& kp);
        bool from_disk(const regionid& r, const e::buffer& key,
                       bool* have_value, std::vector<e::buffer>* value,
                       uint64_t* version);
        size_t expected_dimensions(const regionid& ri) const;
        // Figure out the previous and next individuals to send to/receive from
        // for messages.
        bool prev_and_next(const regionid& r, const e::buffer& key,
                           const std::vector<e::buffer>& value,
                           regionid* prev, regionid* next);
        bool prev_and_next(const regionid& r, const e::buffer& key,
                           const std::vector<e::buffer>& oldvalue,
                           const std::vector<e::buffer>& value,
                           regionid* prev, regionid* next);
        // If there are no messages in the pending queue, move all blocked
        // messages (up until the first DEL/PUT) to the queue of pending
        // messages, and send out messages to the next individuals in the chain.
        // The first form will only unblock messages when there are no pending
        // updates.  The second form trusts that the caller knows that it is
        // safe to unblock even though messages may still be pending.
        void unblock_messages(const regionid& r, const e::buffer& key, e::intrusive_ptr<keyholder> kh);
        // Move as many messages as possible from the deferred queue to the
        // pending queue.
        void move_deferred_to_pending(e::intrusive_ptr<keyholder> kh);
        // Send an ACK and notify the client.  If this is the last pending
        // message for the keypair, then it is safe to unblock more messages.
        void handle_point_leader_work(const regionid& pending_in,
                                      uint64_t version, const e::buffer& key,
                                      e::intrusive_ptr<keyholder> kh,
                                      e::intrusive_ptr<pending> update);
        // Send the message that the pending object needs to send in order to
        // make system-wide progress.
        void send_update(const hyperdex::regionid& pending_in,
                         uint64_t version, const e::buffer& key,
                         e::intrusive_ptr<pending> update);
        // Send an ack based on a pending object using chain rules.  That is,
        // use the send_backward function of the communication layer.
        void send_ack(const regionid& from, uint64_t version,
                      const e::buffer& key, e::intrusive_ptr<pending> update);
        // Send directly.
        void send_ack(const regionid& from, const entityid& to,
                      const e::buffer& key, uint64_t version);
        void respond_positively_to_client(clientop co, uint64_t version);
        void respond_negatively_to_client(clientop co, result_t result);
        // Retransmit current pending values.
        void retransmit();
        // Check that chain rules are followed very closely.
        bool sent_backward_or_from_head(const entityid& from, const entityid& to,
                                        const regionid& chain, const regionid& tail);
        bool sent_backward_or_from_tail(const entityid& from, const entityid& to,
                                        const regionid& chain, const regionid& tail);
        bool sent_forward_or_from_tail(const entityid& from, const entityid& to,
                                       const regionid& chain, const regionid& tail);

    private:
        datalayer* m_data;
        logical* m_comm;
        configuration m_config;
        // XXX a universal lock is a bad idea.  Convert this to a bucket lock.
        // To do this requires that all datastructures below be capable of
        // handling concurrent readers and writers without issue.  We do not use
        // a reader-writer lock here because we need ordering.  The bucketlock
        // will provide the ordering for keys which hash to the same value, and
        // threadsafe datastructures will provide us with the protection.
        po6::threads::mutex m_lock;
        po6::threads::mutex m_keyholders_lock;
        std::map<keypair, e::intrusive_ptr<keyholder> > m_keyholders;
        e::set<clientop> m_clientops;
        bool m_shutdown;
        po6::threads::thread m_retransmitter;
};

} // namespace hyperdex

#include <hyperdex/replication/clientop.h>
#include <hyperdex/replication/keypair.h>
#include <hyperdex/replication/pending.h>
#include <hyperdex/replication/keyholder.h>

#endif // hyperdex_replication_h_
