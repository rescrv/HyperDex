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
                         const e::buffer& key, const std::vector<e::buffer>& value);
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

        class pending
        {
            public:
                pending(const regionid& region,
                        op_t op,
                        uint64_t version,
                        const e::buffer& key,
                        const std::vector<e::buffer>& val,
                        std::tr1::function<void (void)> n,
                        uint16_t prev_subspace,
                        uint16_t next_subspace,
                        const std::vector<bool>& prev_dims,
                        const std::vector<bool>& this_dims,
                        const std::vector<bool>& next_dims);

                pending(const regionid& region,
                        /* op_t op = PUT */
                        uint64_t version,
                        const e::buffer& key,
                        const std::vector<e::buffer>& oldval,
                        const std::vector<e::buffer>& newval,
                        std::tr1::function<void (void)> n,
                        uint16_t prev_subspace,
                        uint16_t next_subspace,
                        const std::vector<bool>& prev_dims,
                        const std::vector<bool>& this_dims,
                        const std::vector<bool>& next_dims);

            public:
                // Returns true if the pending operation should be removed.
                void enable() { m_enabled = true; }
                void tryforward(logical* comm);

            public:
                const regionid& region;
                const op_t op;
                const uint64_t version;
                const e::buffer key;
                const std::vector<e::buffer> value; // Only used if op == PUT.
                const std::tr1::function<void (void)> notify; // A callback called when the point-leader commits


            private:
                friend class e::intrusive_ptr<pending>;

            private:
                size_t m_ref;
                bool m_enabled;
                bool m_fresh;
                regionid m_prev;
                regionid m_thisold;
                regionid m_thisnew;
                regionid m_next;
        };

    private:
        replication(const replication&);

    private:
        replication& operator = (const replication&);

    private:
        void client_common(op_t op, const entityid& from, const regionid& to,
                           uint32_t nonce, const e::buffer& key,
                           const std::vector<e::buffer>& newvalue);
        // This assumes that iterating m_pending is safe without additional
        // synchronization.
        bool check_oldversion(const regionid& to, const e::buffer& key,
                              bool* have_value, uint64_t* version,
                              std::vector<e::buffer>* value);
        void respond_positively_to_client(clientop co, uint64_t version);
        void respond_negatively_to_client(clientop co, result_t result);

    private:
        datalayer* m_data;
        logical* m_comm;
        // XXX a universal lock is a bad idea.  Convert this to a bucket lock.
        // To do this requires that all datastructures below be capable of
        // handling concurrent readers and writers without issue.  We do not use
        // a reader-writer lock here because we need ordering.  The bucketlock
        // will provide the ordering for keys which hash to the same value, and
        // threadsafe datastructures will provide us with the protection.
        po6::threads::mutex m_lock;
        configuration m_config;
        std::set<clientop> m_clientops;
        std::multimap<regionid, e::intrusive_ptr<pending> > m_pending;
};

} // namespace hyperdex

#endif // hyperdex_replication_h_
