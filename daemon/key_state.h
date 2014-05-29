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

    // for use with state_hash_table
    public:
        key_region state_key() const;
        void lock();
        void unlock();
        bool finished();
        void mark_garbage();
        bool marked_garbage() const;

    public:
        bool initialized() const;
        bool empty() const;
        void clear();
        uint64_t max_seq_id() const;
        uint64_t min_seq_id() const;
        e::intrusive_ptr<key_operation> get_version(uint64_t version) const;

    public:
        datalayer::returncode initialize(datalayer* data,
                                         const region_id& ri);
        bool check_against_latest_version(const schema& sc,
                                          bool erase,
                                          bool fail_if_not_found,
                                          bool fail_if_found,
                                          const std::vector<attribute_check>& checks,
                                          network_returncode* nrc);
        void delete_latest(const schema& sc,
                           const region_id& reg_id, uint64_t seq_id,
                           const server_id& client, uint64_t nonce);
        bool put_from_funcs(const schema& sc,
                            const region_id& reg_id, uint64_t seq_id,
                            const std::vector<funcall>& funcs,
                            const server_id& client, uint64_t nonce);
        void insert_deferred(uint64_t version, e::intrusive_ptr<key_operation> op);
        bool persist_to_datalayer(replication_manager* rm, const region_id& ri,
                                  const region_id& reg_id, uint64_t seq_id,
                                  uint64_t version);
        void clear_deferred();
        void clear_acked_prefix();
        void resend_committable(replication_manager* rm,
                                const virtual_server_id& us);
        void append_seq_ids(std::vector<std::pair<region_id, uint64_t> >* seq_ids);
        // Move operations between the queues in the key_state.  Blocked
        // operations will have their blocking criteria checked.  Deferred
        // operations will be checked for continuity with the blocked
        // operations.
        void move_operations_between_queues(replication_manager* rm,
                                            const virtual_server_id& us,
                                            const region_id& ri,
                                            const schema& sc);
        void debug_dump();
        void check_invariants() const;

    private:
        typedef std::list<std::pair<uint64_t, e::intrusive_ptr<key_operation> > >
                key_operation_list_t;
        friend class e::intrusive_ptr<key_state>;
        friend class key_state_reference;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }
        void get_latest(bool* has_old_value,
                        uint64_t* old_version,
                        std::vector<e::slice>** old_value);
        bool check_version(const schema& sc,
                           bool erase,
                           bool fail_if_not_found,
                           bool fail_if_found,
                           const std::vector<attribute_check>& checks,
                           bool has_old_value,
                           uint64_t old_version,
                           std::vector<e::slice>* old_value,
                           network_returncode* nrc);
        void hash_objects(const configuration* config,
                          const region_id& reg,
                          const schema& sc,
                          bool has_new_value,
                          const std::vector<e::slice>& new_value,
                          bool has_old_value,
                          const std::vector<e::slice>& old_value,
                          e::intrusive_ptr<key_operation> pend);

    private:
        const region_id m_ri;
        const std::string m_key_backing;
        const e::slice m_key;
        po6::threads::mutex m_lock;
        bool m_marked_garbage;
        bool m_initialized;
        size_t m_ref;
        key_operation_list_t m_committable;
        key_operation_list_t m_blocked;
        key_operation_list_t m_deferred;
        bool m_has_old_value;
        uint64_t m_old_version;
        std::vector<e::slice> m_old_value;
        datalayer::reference m_old_disk_ref;
        std::auto_ptr<e::buffer> m_old_backing;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_key_state_h_
