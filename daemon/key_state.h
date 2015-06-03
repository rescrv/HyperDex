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

// e
#include <e/lockfree_mpsc_fifo.h>

// HyperDex
#include "namespace.h"
#include "daemon/datalayer.h"
#include "daemon/key_operation.h"

BEGIN_HYPERDEX_NAMESPACE
class replication_manager;

// Each enqueue* call is rather fast and sets the work up to be done.  For each
// call, you should call work_state_machine at least once.

class key_state
{
    public:
        key_state(const key_region& kr);
        ~key_state() throw ();

    public:
        key_region state_key() const;
        bool finished();

    public:
        bool initialized();
        datalayer::returncode initialize(datalayer* data,
                                         const schema& sc,
                                         const region_id& ri);

        void enqueue_client_atomic(replication_manager* rm,
                                   const virtual_server_id& us,
                                   const schema& sc,
                                   const server_id& from,
                                   uint64_t nonce,
                                   std::auto_ptr<key_change> kc,
                                   std::auto_ptr<e::buffer> backing);
        void enqueue_chain_op(replication_manager* rm,
                              const virtual_server_id& us,
                              const schema& sc,
                              const virtual_server_id& from,
                              uint64_t old_version,
                              uint64_t new_version,
                              bool fresh,
                              bool has_value,
                              const std::vector<e::slice>& value,
                              std::auto_ptr<e::buffer> backing);
        void enqueue_chain_subspace(replication_manager* rm,
                                    const virtual_server_id& us,
                                    const schema& sc,
                                    const virtual_server_id& from,
                                    uint64_t old_version,
                                    uint64_t new_version,
                                    const std::vector<e::slice>& value,
                                    std::auto_ptr<e::buffer> backing,
                                    const region_id& prev_region,
                                    const region_id& this_old_region,
                                    const region_id& this_new_region,
                                    const region_id& next_region);
        void enqueue_chain_ack(replication_manager* rm,
                                const virtual_server_id& us,
                                const schema& sc,
                                const virtual_server_id& from,
                               uint64_t version);
        void work_state_machine(replication_manager* rm,
                                const virtual_server_id& us,
                                const schema& sc);

        uint64_t max_version();
        void reconfigure(e::garbage_collector* gc);
        void reset(e::garbage_collector* gc);

        void resend_committable(replication_manager* rm,
                                const virtual_server_id& us);

        void append_all_versions(std::vector<std::pair<region_id, uint64_t> >* versions);

        void debug_dump();

    private:
        struct deferred_key_change;
        struct stub_client_atomic;
        struct stub_chain_op;
        struct stub_chain_subspace;
        struct stub_chain_ack;
        struct client_response;
        typedef std::list<e::intrusive_ptr<key_operation> > key_operation_list_t;
        typedef std::list<e::intrusive_ptr<deferred_key_change> > key_change_list_t;

    private:
        void check_invariants() const;
        void someone_needs_to_work_the_state_machine();
        void work_state_machine_or_pass_the_buck(replication_manager* rm,
                                                 const virtual_server_id& us,
                                                 const schema& sc);
        // returns when m_someone_is_working; must call wsm_with_work_bit after
        void takeover_state_machine();
        // returns whether or not the caller took over the state machine, will
        // return true if must call wsm_with_work_bit after; false otherwise
        bool possibly_takeover_state_machine();
        // releases inner state by setting m_someone_is_working = false.
        void work_state_machine_with_work_bit(replication_manager* rm,
                                              const virtual_server_id& us,
                                              const schema& sc);
        void do_client_atomic(replication_manager* rm,
                              const virtual_server_id& us,
                              const schema& sc,
                              const server_id& from,
                              uint64_t nonce,
                              std::auto_ptr<key_change> kc,
                              std::auto_ptr<e::buffer> backing);
        void do_chain_op(replication_manager* rm,
                         const virtual_server_id& us,
                         const schema& sc,
                         const virtual_server_id& from,
                         uint64_t old_version,
                         uint64_t new_version,
                         bool fresh,
                         bool has_value,
                         const std::vector<e::slice>& value,
                         std::auto_ptr<e::buffer> backing);
        void do_chain_subspace(replication_manager* rm,
                               const virtual_server_id& us,
                               const schema& sc,
                               const virtual_server_id& from,
                               uint64_t old_version,
                               uint64_t new_version,
                               const std::vector<e::slice>& value,
                               std::auto_ptr<e::buffer> backing,
                               const region_id& prev_region,
                               const region_id& this_old_region,
                               const region_id& this_new_region,
                               const region_id& next_region);
        void do_chain_ack(replication_manager* rm,
                          const virtual_server_id& us,
                          const schema& sc,
                          const virtual_server_id& from,
                          uint64_t version);
        void add_response(const client_response& cr);
        void send_responses(replication_manager* rm,
                            const virtual_server_id& us);
        e::intrusive_ptr<key_operation> get(uint64_t new_version);
        e::intrusive_ptr<key_operation>
            enqueue_continuous_key_op(uint64_t old_version,
                                      uint64_t new_version,
                                      bool fresh,
                                      bool has_value,
                                      const std::vector<e::slice>& value,
                                      std::auto_ptr<e::arena> memory);
        e::intrusive_ptr<key_operation>
            enqueue_discontinuous_key_op(uint64_t old_version,
                                         uint64_t new_version,
                                         const std::vector<e::slice>& value,
                                         std::auto_ptr<e::arena> memory,
                                         const region_id& prev_region,
                                         const region_id& this_old_region,
                                         const region_id& this_new_region,
                                         const region_id& next_region);
        void get_latest(bool* has_old_value,
                        uint64_t* old_version,
                        const std::vector<e::slice>** old_value);
        bool drain_queue(replication_manager* rm,
                         const virtual_server_id& us,
                         const schema& sc,
                         key_operation_list_t* q,
                         void (key_state::*f)(replication_manager* rm,
                                              const virtual_server_id& us,
                                              const schema& sc));
        bool drain_queue(replication_manager* rm,
                         const virtual_server_id& us,
                         const schema& sc,
                         key_change_list_t* q,
                         void (key_state::*f)(replication_manager* rm,
                                              const virtual_server_id& us,
                                              const schema& sc));
        void drain_changes(replication_manager* rm,
                           const virtual_server_id& us,
                           const schema& sc);
        static bool compare_key_op_ptrs(const e::intrusive_ptr<key_operation>& lhs,
                                        const e::intrusive_ptr<key_operation>& rhs);
        void drain_deferred(replication_manager* rm,
                            const virtual_server_id& us,
                            const schema& sc);
        void drain_blocked(replication_manager* rm,
                           const virtual_server_id& us,
                           const schema& sc);
        void drain_committable(replication_manager* rm,
                               const virtual_server_id& us,
                               const schema& sc);
        void hash_objects(const configuration* config,
                          const region_id& reg,
                          const schema& sc,
                          bool has_new_value,
                          const std::vector<e::slice>& new_value,
                          bool has_old_value,
                          const std::vector<e::slice>& old_value,
                          e::intrusive_ptr<key_operation> pend);

    // state synchronized without m_lock (either read-only or otherwise
    // synchronized)
    private:
        const region_id m_ri;
        const std::string m_key_backing;
        const e::slice m_key;

        e::lockfree_mpsc_fifo<stub_client_atomic> m_client_atomics;
        e::lockfree_mpsc_fifo<stub_chain_op> m_chain_ops;
        e::lockfree_mpsc_fifo<stub_chain_subspace> m_chain_subspaces;
        e::lockfree_mpsc_fifo<stub_chain_ack> m_chain_acks;

    // protected state, synchronized by m_lock;
    private:
        po6::threads::mutex m_lock;
        po6::threads::cond m_avail;
        bool m_someone_is_working_the_state_machine;
        bool m_someone_needs_to_work_the_state_machine;

        bool m_initialized;

        // Does this key have a value (before operations are applied)
        bool m_has_old_value;
        uint64_t m_old_version;

        std::vector<e::slice> m_old_value;
        datalayer::reference m_old_disk_ref;
        e::intrusive_ptr<key_operation> m_old_op;

        std::vector<client_response> m_client_responses_heap;

        // These operations are being actively replicated by HyperDex
        // and were passed to subsequent nodes in the value dependent chain.
        key_operation_list_t m_committable;
        bool m_committable_empty;

        // These operations delayed pending completion of all operations
        // in the committable state.  Generally, it happens on different sides
        // of deletions in order to enforce proper order of arrival for
        // operations.  It's like a memory barrier, but for distributed systems.
        key_operation_list_t m_blocked;
        bool m_blocked_empty;

        // These messages arrived out of order and will be processed
        // pending future messages arrival.
        key_operation_list_t m_deferred;
        bool m_deferred_empty;

        // These are client-submitted operations enqueued to be
        // processed.  Generally the changes queue will be moved to
        // blocked/committable immediate.
        key_change_list_t m_changes;
        bool m_changes_empty;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_key_state_h_
