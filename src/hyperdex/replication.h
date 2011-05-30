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
        void client_put(const entityid& from, const regionid& to, uint32_t nonce,
                        const e::buffer& key, const std::vector<e::buffer>& value);
        void client_del(const entityid& from, const regionid& to, uint32_t nonce,
                        const e::buffer& key);
        // These are called in response to messages from other hosts.
        void chain_put(const entityid& from, const entityid& to, uint64_t rev,
                       bool fresh, const e::buffer& key,
                       const std::vector<e::buffer>& value);
        void chain_del(const entityid& from, const entityid& to, uint64_t rev,
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
            const regionid region;
            const entityid from;
            const uint32_t nonce;
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
                pending(bool f, const std::vector<e::buffer>& v,
                        std::tr1::function<void (uint64_t)> n,
                        const regionid& prev, const regionid& thisold,
                        const regionid& thisnew, const regionid& next);
                pending(std::tr1::function<void (uint64_t)> n,
                        const regionid& prev, const regionid& thisold,
                        const regionid& thisnew, const regionid& next);

            public:
                void forward_msg(e::buffer* msg, uint64_t version, const e::buffer& key);
                void ack_msg(e::buffer* msg, uint64_t version, const e::buffer& key);

            public:
                bool enabled;
                bool fresh;
                op_t op;
                std::vector<e::buffer> value;
                std::tr1::function<void (uint64_t)> notify;

            private:
                friend class e::intrusive_ptr<pending>;

            private:
                size_t m_ref;
                regionid m_prev;
                regionid m_thisold;
                regionid m_thisnew;
                regionid m_next;
        };

        class keyholder
        {
            public:
                keyholder(const configuration& config, const regionid& r, logical* comm);

            public:
                // Return true if the operation succeeded.  Return false if the
                // operation failed.
                bool add_pending(std::tr1::function<result_t (std::vector<e::buffer>*, uint64_t*)> read_from_disk,
                                 const e::buffer& key,
                                 const std::vector<e::buffer>& value,
                                 std::tr1::function<void (uint64_t)> notify);
                bool add_pending(std::tr1::function<result_t (std::vector<e::buffer>*, uint64_t*)> read_from_disk,
                                 const e::buffer& key,
                                 std::tr1::function<void (uint64_t)> notify);
                void add_pending(std::tr1::function<result_t (std::vector<e::buffer>*, uint64_t*)> read_from_disk,
                                 const entityid& from,
                                 const entityid& to,
                                 uint64_t version,
                                 bool fresh,
                                 const e::buffer& key,
                                 const std::vector<e::buffer>& value);
                void add_pending(std::tr1::function<result_t (std::vector<e::buffer>*, uint64_t*)> read_from_disk,
                                 const entityid& from,
                                 const entityid& to,
                                 uint64_t version,
                                 const e::buffer& key);

            public:
                size_t expected_dimensions() const { return m_prev_dims.size(); }

            private:
                friend class e::intrusive_ptr<keyholder>;

            private:
                keyholder(const keyholder&);

            private:
                // A wrapper to log error messages from the disk layer in a
                // consistent fashion.
                bool from_disk(std::tr1::function<result_t (std::vector<e::buffer>*, uint64_t*)> read_from_disk,
                               bool* have_value,
                               std::vector<e::buffer>* value,
                               uint64_t* version);
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
                keyholder& operator = (const keyholder&);

            private:
                size_t m_ref;
                logical* m_comm;
                const regionid m_region;
                const uint16_t m_prev_subspace;
                const uint16_t m_next_subspace;
                const std::vector<bool> m_prev_dims;
                const std::vector<bool> m_this_dims;
                const std::vector<bool> m_next_dims;
                std::map<uint64_t, e::intrusive_ptr<pending> > m_pending;
        };

    private:
        replication(const replication&);

    private:
        replication& operator = (const replication&);

    private:
        void client_common(op_t op, const entityid& from, const regionid& to,
                           uint32_t nonce, const e::buffer& key,
                           const std::vector<e::buffer>& newvalue);
        void chain_common(op_t op, const entityid& from, const entityid& to,
                          uint64_t newversion, bool fresh, const e::buffer& key,
                          const std::vector<e::buffer>& newvalue);
        po6::threads::mutex* get_lock(const keypair& kp);
        e::intrusive_ptr<keyholder> get_keyholder(const keypair& kp);
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
        po6::threads::mutex m_keyholders_lock;
        std::map<keypair, e::intrusive_ptr<keyholder> > m_keyholders;
        po6::threads::mutex m_clientops_lock;
        std::set<clientop> m_clientops;
};

} // namespace hyperdex

#endif // hyperdex_replication_h_
