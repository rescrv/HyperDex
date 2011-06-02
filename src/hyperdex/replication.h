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

// HyperDex
#include <hyperdex/datalayer.h>
#include <hyperdex/logical.h>

namespace hyperdex
{

// Manage replication.
class replication
{
    public:
        replication(datalayer* dl, logical* comm);

    public:
        // Install the new configuration.  This implicitly remaps the logical
        // communcation layer.
        void reconfigure(const configuration& config);
        void drop(const regionid& reg);

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
        void chain_ack(const entityid& from, const entityid& to, uint64_t rev,
                       const e::buffer& key);


    private:
        enum op_t
        {
            PUT,
            DEL
        };

        struct clientop
        {
            clientop() : region(), from(), nonce() {}
            clientop(regionid r, entityid f, uint32_t n) : region(r), from(f), nonce(n) {}
            bool operator < (const clientop& rhs) const;
            regionid region;
            entityid from;
            uint32_t nonce;
        };

        struct keypair
        {
            keypair() : region(), key() {}
            keypair(const regionid& r, const e::buffer& k) : region(r), key(k) {}
            bool operator < (const keypair& rhs) const;
            const regionid region;
            const e::buffer key;
        };

        class pending
        {
            public:
                pending(op_t op, uint64_t version,
                        const e::buffer& key,
                        const std::vector<e::buffer>& value,
                        const clientop& co = clientop());

            public:
                const op_t op;
                const uint64_t version;
                const e::buffer& key;
                const std::vector<e::buffer> value;
                clientop co;
                bool fresh;
                bool acked;
                bool ondisk; // True if the pending update is already on disk.
                regionid prev;
                regionid thisold;
                regionid thisnew;
                regionid next;

            private:
                friend class e::intrusive_ptr<pending>;

            private:
                size_t m_ref;
        };

        class chainlink_calculator
        {
            public:
                chainlink_calculator(const configuration& config,
                                     const regionid& r);

            public:
                size_t expected_dimensions() const { return m_prev_dims.size(); }

            public:
                void four_regions(const e::buffer& key,
                                  const std::vector<e::buffer>& oldvalue,
                                  const std::vector<e::buffer>& value,
                                  regionid* prev,
                                  regionid* thisold,
                                  regionid* thisnew,
                                  regionid* next);
                void four_regions(const e::buffer& key,
                                  const std::vector<e::buffer>& value,
                                  regionid* prev,
                                  regionid* thisold,
                                  regionid* thisnew,
                                  regionid* next);

            private:
                friend class e::intrusive_ptr<chainlink_calculator>;

            private:
                size_t m_ref;
                regionid m_region;
                const uint16_t m_prev_subspace;
                const uint16_t m_next_subspace;
                const std::vector<bool> m_prev_dims;
                const std::vector<bool> m_this_dims;
                const std::vector<bool> m_next_dims;
        };

        class keyholder
        {
            public:
                keyholder();

            public:
                std::map<uint64_t, e::intrusive_ptr<pending> > pending_updates;
                std::map<uint64_t, e::intrusive_ptr<pending> > blocked_updates;
                std::map<uint64_t, e::intrusive_ptr<pending> > deferred_updates;

            private:
                friend class e::intrusive_ptr<keyholder>;

            private:
                size_t m_ref;
        };

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
        e::intrusive_ptr<chainlink_calculator> get_chainlink_calculator(const regionid& r);
        e::intrusive_ptr<keyholder> get_keyholder(const keypair& kp);
        bool from_disk(const regionid& r, const e::buffer& key,
                       bool* have_value, std::vector<e::buffer>* value,
                       uint64_t* version);
        // If there are no messages in the pending queue, move all blocked
        // messages (up until the first DEL/PUT) to the queue of pending
        // messages, and send out messages to the next individuals in the chain.
        // The first form will only unblock messages when there are no pending
        // updates.  The second form trusts that the caller knows that it is
        // safe to unblock even though messages may still be pending.
        void unblock_messages(const regionid& r, e::intrusive_ptr<keyholder> kh);
        void unconditionally_unblock_messages(const regionid& r, e::intrusive_ptr<keyholder> kh);
        // Move as many messages as possible from the deferred queue to the
        // pending queue.
        void move_deferred_to_pending(e::intrusive_ptr<keyholder> kh);
        // Send an ACK and notify the client.  If this is the last pending
        // message for the keypair, then it is safe to unblock more messages.
        void handle_point_leader_work(op_t op, const entityid& from,
                                      const entityid& to, uint64_t newversion,
                                      bool fresh, const e::buffer& key,
                                      const std::vector<e::buffer>& newvalue);
        void send_update(const hyperdex::regionid& pending_in,
                         e::intrusive_ptr<pending> update);
        // Send an ack based on a pending object using chain rules.  That is,
        // use the send_backward function of the communication layer.
        void send_ack(const regionid& from, const e::buffer& key,
                      uint64_t version, e::intrusive_ptr<pending> update);
        // Send directly.
        void send_ack(const regionid& from, const entityid& to,
                      const e::buffer& key, uint64_t version);
        bool have_seen_clientop(const clientop& co);
        void respond_positively_to_client(clientop co, uint64_t version);
        void respond_negatively_to_client(clientop co, result_t result);

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
        //
        // When the other datastructures become lock-free, m_config should be
        // passed in per-call rather than be a member variable.  XXX Even as-is
        // I'm not convinced that having the data layer operate on a different
        // config than the layer making the judgement calls about messages is a
        // good idea.
        po6::threads::mutex m_lock;
        po6::threads::mutex m_chainlink_calculators_lock;
        std::map<regionid, e::intrusive_ptr<chainlink_calculator> > m_chainlink_calculators;
        po6::threads::mutex m_keyholders_lock;
        std::map<keypair, e::intrusive_ptr<keyholder> > m_keyholders;
        po6::threads::mutex m_clientops_lock;
        std::set<clientop> m_clientops;
};

} // namespace hyperdex

#endif // hyperdex_replication_h_
