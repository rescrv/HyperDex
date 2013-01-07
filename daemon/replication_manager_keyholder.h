// Copyright (c) 2012, Cornell University
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

#ifndef hyperdex_daemon_replication_manager_keyholder_h_
#define hyperdex_daemon_replication_manager_keyholder_h_

// STL
#include <tr1/memory>

// HyperDex
#include "daemon/datalayer.h"
#include "daemon/replication_manager.h"

class hyperdex::replication_manager::keyholder
{
    public:
        keyholder();
        ~keyholder() throw ();

    public:
        bool empty() const;
        void get_latest_version(bool* has_old_value,
                                uint64_t* old_version,
                                std::vector<e::slice>** old_value);
        e::intrusive_ptr<pending> get_by_version(uint64_t version) const;
        uint64_t version_on_disk() const;
        uint64_t max_seq_id() const;
        uint64_t min_seq_id() const;

    public:
        bool has_committable_ops() const;
        uint64_t most_recent_committable_version() const;
        e::intrusive_ptr<pending> most_recent_committable_op() const;

    public:
        bool has_blocked_ops() const;
        uint64_t oldest_blocked_version() const;
        e::intrusive_ptr<pending> oldest_blocked_op() const;
        uint64_t most_recent_blocked_version() const;
        e::intrusive_ptr<pending> most_recent_blocked_op() const;

    public:
        bool has_deferred_ops() const;
        uint64_t oldest_deferred_version() const;
        e::intrusive_ptr<pending> oldest_deferred_op() const;

    public:
        void clear_committable_acked();
        void clear_deferred();
        void set_version_on_disk(uint64_t version);
        void insert_deferred(uint64_t version, e::intrusive_ptr<pending> op);
        void pop_oldest_deferred();
        void shift_one_blocked_to_committable();
        void shift_one_deferred_to_blocked();
        void resend_committable(replication_manager* rm,
                                const virtual_server_id& us,
                                const e::slice& key);

    public:
        bool& get_has_old_value() { return m_has_old_value; }
        uint64_t& get_old_version() { return m_old_version; }
        std::vector<e::slice>& get_old_value() { return m_old_value; }
        datalayer::reference& get_old_disk_ref() { return m_old_disk_ref; }

    private:
        typedef std::list<std::pair<uint64_t, e::intrusive_ptr<pending> > >
                committable_list_t;
        typedef std::list<std::pair<uint64_t, e::intrusive_ptr<pending> > >
                blocked_list_t;
        typedef std::list<std::pair<uint64_t, e::intrusive_ptr<pending> > >
                deferred_list_t;
        friend class e::intrusive_ptr<keyholder>;

    private:
        void inc() { __sync_add_and_fetch(&m_ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&m_ref, 1) == 0) delete this; }

    private:
        size_t m_ref;
        committable_list_t m_committable;
        blocked_list_t m_blocked;
        deferred_list_t m_deferred;
        bool m_has_old_value;
        uint64_t m_old_version;
        std::vector<e::slice> m_old_value;
        datalayer::reference m_old_disk_ref;
        std::tr1::shared_ptr<e::buffer> m_old_backing;
};

#endif // hyperdex_daemon_replication_manager_keyholder_h_
