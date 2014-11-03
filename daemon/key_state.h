// Copyright (c) 2012-2014, Cornell University
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

#ifndef hyperdex_daemon_key_state_h_
#define hyperdex_daemon_key_state_h_

// HyperDex
#include "namespace.h"
#include "daemon/datalayer.h"
#include "daemon/key_operation.h"

BEGIN_HYPERDEX_NAMESPACE
class replication_manager;

class key_state
{
    public:
        key_state(const key_region& kr);
        ~key_state() throw ();

    public:
        key_region state_key() const;
        void lock();
        void unlock();
        bool finished();
        void mark_garbage();
        bool marked_garbage() const;

    public:
        bool initialized() const;
        datalayer::returncode initialize(datalayer* data,
                                         const region_id& ri);

        void enqueue_key_change(const server_id& from,
                                uint64_t nonce,
                                uint64_t version,
                                std::auto_ptr<key_change> kc,
                                std::auto_ptr<e::buffer> backing);
        e::intrusive_ptr<key_operation> get(uint64_t new_version);
        e::intrusive_ptr<key_operation>
            enqueue_continuous_key_op(uint64_t old_version,
                                      uint64_t new_version,
                                      bool fresh,
                                      bool has_value,
                                      const std::vector<e::slice>& value,
                                      std::auto_ptr<e::buffer> backing);
        e::intrusive_ptr<key_operation>
            enqueue_discontinuous_key_op(uint64_t old_version,
                                         uint64_t new_version,
                                         const std::vector<e::slice>& value,
                                         std::auto_ptr<e::buffer> backing,
                                         const region_id& prev_region,
                                         const region_id& this_old_region,
                                         const region_id& this_new_region,
                                         const region_id& next_region);
        void update_datalayer(replication_manager* rm, uint64_t version);
        void resend_committable(replication_manager* rm,
                                const virtual_server_id& us);
        void work_state_machine(replication_manager* rm,
                                const virtual_server_id& us,
                                const schema& sc);

        void reconfigure();
        void reset();

        void append_all_versions(std::vector<std::pair<region_id, uint64_t> >* versions);
        uint64_t max_version() const;

        void check_invariants() const;
        void debug_dump() const;

    private:
        struct deferred_key_change;
        typedef std::list<e::intrusive_ptr<key_operation> > key_operation_list_t;
        typedef std::list<e::intrusive_ptr<deferred_key_change> > key_change_list_t;
        friend class e::intrusive_ptr<key_state>;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }
        uint64_t highest_ackable();
        void get_latest(bool* has_old_value,
                        uint64_t* old_version,
                        const std::vector<e::slice>** old_value);
        static bool compare_key_op_ptrs(const e::intrusive_ptr<key_operation>& lhs,
                                        const e::intrusive_ptr<key_operation>& rhs);
        bool step_state_machine_changes(replication_manager* rm,
                                        const virtual_server_id& us,
                                        const schema& sc);
        bool step_state_machine_deferred(replication_manager* rm,
                                         const virtual_server_id& us,
                                         const schema& sc);
        bool step_state_machine_blocked(replication_manager* rm, const virtual_server_id& us);
        void hash_objects(const configuration* config,
                          const region_id& reg,
                          const schema& sc,
                          bool has_new_value,
                          const std::vector<e::slice>& new_value,
                          bool has_old_value,
                          const std::vector<e::slice>& old_value,
                          e::intrusive_ptr<key_operation> pend);

    private:
        size_t m_ref;
        const region_id m_ri;
        const std::string m_key_backing;
        const e::slice m_key;
        po6::threads::mutex m_lock;
        bool m_marked_garbage;
        bool m_initialized;

        // Does this key have a value (before operations are applied)
        bool m_has_old_value;
        uint64_t m_old_version;

        std::vector<e::slice> m_old_value;
        datalayer::reference m_old_disk_ref;
        e::intrusive_ptr<key_operation> m_old_op;

        // These operations are being actively replicated by HyperDex
        // and were passed to subsequent nodes in the value dependent chain.
        key_operation_list_t m_committable;

        // These operations delayed pending completion of all operations
        // in the committable state.  Generally, it happens on different sides
        // of deletions in order to enforce proper order of arrival for
        // operations.  It's like a memory barrier, but for distributed systems.
        key_operation_list_t m_blocked;

        // These messages arrived out of order and will be processed
        // pending future messages arrival.
        key_operation_list_t m_deferred;

        // These are client-submitted operations enqueued to be
        // processed.  Generally the changes queue will be moved to
        // blocked/committable immediate.
        key_change_list_t m_changes;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_key_state_h_
