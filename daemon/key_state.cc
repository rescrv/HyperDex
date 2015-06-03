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

#define __STDC_LIMIT_MACROS

// Google Log
#include <glog/logging.h>

// HyperDex
#include "common/hash.h"
#include "common/network_returncode.h"
#include "daemon/auth.h"
#include "daemon/daemon.h"
#include "daemon/key_region.h"
#include "daemon/key_state.h"
#include "daemon/key_operation.h"

#if 0
#define CHECK_INVARIANTS() this->check_invariants()
#else
#define CHECK_INVARIANTS() do {} while (0)
#endif

using hyperdex::key_operation;
using hyperdex::key_region;
using hyperdex::key_state;

struct key_state::deferred_key_change
{
    deferred_key_change(const server_id& _from,
                        uint64_t _nonce, uint64_t _version,
                        std::auto_ptr<key_change> _kc,
                        std::auto_ptr<e::buffer> _backing)
        : from(_from)
        , nonce(_nonce)
        , version(_version)
        , kc(_kc)
        , backing(_backing)
        , m_ref(0)
    {
    }

    void inc() { ++m_ref; }
    void dec() { if (--m_ref == 0) delete this; }

    const server_id from;
    const uint64_t nonce;
    const uint64_t version;
    const std::auto_ptr<key_change> kc;
    const std::auto_ptr<e::buffer> backing;

    private:
        size_t m_ref;

    private:
        deferred_key_change(const deferred_key_change&);
        deferred_key_change& operator = (const deferred_key_change&);
};

struct key_state::client_response
{
    client_response() : respond_after(0), client(), nonce(), ret() {}
    client_response(uint64_t _respond_after,
                    server_id _client,
                    uint64_t _nonce,
                    network_returncode _ret)
        : respond_after(_respond_after)
        , client(_client)
        , nonce(_nonce)
        , ret(_ret)
    {
    }
    ~client_response() throw () {}

    // intentionally backwards so the max heap becomes min heap.
    bool operator < (const client_response& rhs)
    {
        return respond_after > rhs.respond_after;
    }

    uint64_t respond_after;
    server_id client;
    uint64_t nonce;
    network_returncode ret;
};

key_state :: key_state(const key_region& kr)
    : m_ri(kr.region)
    , m_key_backing(reinterpret_cast<const char*>(kr.key.data()), kr.key.size())
    , m_key(m_key_backing.data(), m_key_backing.size())
    , m_client_atomics()
    , m_chain_ops()
    , m_chain_subspaces()
    , m_chain_acks()
    , m_lock()
    , m_avail(&m_lock)
    , m_someone_is_working_the_state_machine(false)
    , m_someone_needs_to_work_the_state_machine(false)
    , m_initialized(false)
    , m_has_old_value(false)
    , m_old_version(0)
    , m_old_value()
    , m_old_disk_ref()
    , m_old_op()
    , m_client_responses_heap()
    , m_committable()
    , m_committable_empty(true)
    , m_blocked()
    , m_blocked_empty(true)
    , m_deferred()
    , m_deferred_empty(true)
    , m_changes()
    , m_changes_empty(true)
{
}

key_state :: ~key_state() throw ()
{
    m_lock.lock();
    m_lock.unlock();
}

key_region
key_state :: state_key() const
{
    return key_region(m_ri, m_key);
}

bool
key_state :: finished()
{
    po6::threads::mutex::hold hold(&m_lock);
    return !m_someone_is_working_the_state_machine &&
           !m_someone_needs_to_work_the_state_machine &&
           m_committable_empty &&
           m_blocked_empty &&
           m_deferred_empty &&
           m_changes_empty;
}

bool
key_state :: initialized()
{
    po6::threads::mutex::hold hold(&m_lock);
    return m_initialized;
}

hyperdex::datalayer::returncode
key_state :: initialize(datalayer* data,
                        const schema&,
                        const region_id& ri)
{
    po6::threads::mutex::hold hold(&m_lock);

    // maybe we get lucky
    if (m_initialized)
    {
        return datalayer::SUCCESS;
    }

    while (m_someone_is_working_the_state_machine)
    {
        m_avail.wait();
    }

    // make sure someone else didn't initialize while we waited
    if (m_initialized)
    {
        return datalayer::SUCCESS;
    }

    datalayer::returncode rc = data->get(ri, m_key, &m_old_value, &m_old_version, &m_old_disk_ref);

    switch (rc)
    {
        case datalayer::SUCCESS:
            m_has_old_value = true;
            break;
        case datalayer::NOT_FOUND:
        case datalayer::BAD_ENCODING:
        case datalayer::CORRUPTION:
        case datalayer::IO_ERROR:
        case datalayer::LEVELDB_ERROR:
        default:
            m_has_old_value = false;
            m_old_version = 0;
            break;
    }

    m_initialized = true;
    CHECK_INVARIANTS();
    return rc;
}

struct key_state::stub_client_atomic
{
    stub_client_atomic(const server_id& f,
                       uint64_t n,
                       std::auto_ptr<key_change> k,
                       std::auto_ptr<e::buffer> b)
        : from(f), nonce(n), kc(k), backing(b) {}
    ~stub_client_atomic() throw () {}

    server_id from;
    uint64_t nonce;
    std::auto_ptr<key_change> kc;
    std::auto_ptr<e::buffer> backing;
};

void
key_state :: enqueue_client_atomic(replication_manager* rm,
                                   const virtual_server_id& us,
                                   const schema& sc,
                                   const server_id& from,
                                   uint64_t nonce,
                                   std::auto_ptr<key_change> kc,
                                   std::auto_ptr<e::buffer> backing)
{
    bool have_it = possibly_takeover_state_machine();

    if (have_it)
    {
        do_client_atomic(rm, us, sc, from, nonce, kc, backing);
        work_state_machine_with_work_bit(rm, us, sc);
    }
    else
    {
        m_client_atomics.push(new stub_client_atomic(from, nonce, kc, backing));
        someone_needs_to_work_the_state_machine();
        work_state_machine_or_pass_the_buck(rm, us, sc);
    }
}

struct key_state::stub_chain_op
{
    stub_chain_op(const virtual_server_id& _from,
                  uint64_t _old_version,
                  uint64_t _new_version,
                  bool _fresh,
                  bool _has_value,
                  const std::vector<e::slice>& _value,
                  std::auto_ptr<e::buffer> _backing)
        : from(_from)
        , old_version(_old_version)
        , new_version(_new_version)
        , fresh(_fresh)
        , has_value(_has_value)
        , value(_value)
        , backing(_backing)
    {
    }
    ~stub_chain_op() throw () {}

    virtual_server_id from;
    uint64_t old_version;
    uint64_t new_version;
    bool fresh;
    bool has_value;
    std::vector<e::slice> value;
    std::auto_ptr<e::buffer> backing;
};

void
key_state :: enqueue_chain_op(replication_manager* rm,
                              const virtual_server_id& us,
                              const schema& sc,
                              const virtual_server_id& from,
                              uint64_t old_version,
                              uint64_t new_version,
                              bool fresh,
                              bool has_value,
                              const std::vector<e::slice>& value,
                              std::auto_ptr<e::buffer> backing)
{
    bool have_it = possibly_takeover_state_machine();

    if (have_it)
    {
        do_chain_op(rm, us, sc, from, old_version, new_version, fresh, has_value, value, backing);
        work_state_machine_with_work_bit(rm, us, sc);
    }
    else
    {
        m_chain_ops.push(new stub_chain_op(from, old_version, new_version, fresh, has_value, value, backing));
        someone_needs_to_work_the_state_machine();
        work_state_machine_or_pass_the_buck(rm, us, sc);
    }
}

struct key_state::stub_chain_subspace
{
    stub_chain_subspace(const virtual_server_id& _from,
                        uint64_t _old_version,
                        uint64_t _new_version,
                        const std::vector<e::slice>& _value,
                        std::auto_ptr<e::buffer> _backing,
                        const region_id& _prev_region,
                        const region_id& _this_old_region,
                        const region_id& _this_new_region,
                        const region_id& _next_region)
        : from(_from)
        , old_version(_old_version)
        , new_version(_new_version)
        , value(_value)
        , backing(_backing)
        , prev_region(_prev_region)
        , this_old_region(_this_old_region)
        , this_new_region(_this_new_region)
        , next_region(_next_region)
    {
    }
    ~stub_chain_subspace() throw () {}

    virtual_server_id from;
    uint64_t old_version;
    uint64_t new_version;
    std::vector<e::slice> value;
    std::auto_ptr<e::buffer> backing;
    region_id prev_region;
    region_id this_old_region;
    region_id this_new_region;
    region_id next_region;
};

void
key_state :: enqueue_chain_subspace(replication_manager* rm,
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
                                    const region_id& next_region)
{
    bool have_it = possibly_takeover_state_machine();

    if (have_it)
    {
        do_chain_subspace(rm, us, sc, from, old_version, new_version, value, backing, prev_region, this_old_region, this_new_region, next_region);
        work_state_machine_with_work_bit(rm, us, sc);
    }
    else
    {
        m_chain_subspaces.push(new stub_chain_subspace(from, old_version, new_version, value, backing, prev_region, this_old_region, this_new_region, next_region));
        someone_needs_to_work_the_state_machine();
        work_state_machine_or_pass_the_buck(rm, us, sc);
    }
}

struct key_state::stub_chain_ack
{
    stub_chain_ack(const virtual_server_id& _from,
                   uint64_t _version)
        : from(_from)
        , version(_version)
    {
    }
    ~stub_chain_ack() throw () {}

    virtual_server_id from;
    uint64_t version;
};

void
key_state :: enqueue_chain_ack(replication_manager* rm,
                               const virtual_server_id& us,
                               const schema& sc,
                               const virtual_server_id& from,
                               uint64_t version)
{
    bool have_it = possibly_takeover_state_machine();

    if (have_it)
    {
        do_chain_ack(rm, us, sc, from, version);
        work_state_machine_with_work_bit(rm, us, sc);
    }
    else
    {
        m_chain_acks.push(new stub_chain_ack(from, version));
        someone_needs_to_work_the_state_machine();
        work_state_machine_or_pass_the_buck(rm, us, sc);
    }
}

void
key_state :: work_state_machine(replication_manager* rm,
                                const virtual_server_id& us,
                                const schema& sc)
{
    work_state_machine_or_pass_the_buck(rm, us, sc);
}

uint64_t
key_state :: max_version()
{
    po6::threads::mutex::hold hold(&m_lock);

    while (m_someone_is_working_the_state_machine)
    {
        m_avail.wait();
    }

    uint64_t ret = 0;

    for (key_operation_list_t::const_iterator it = m_committable.begin();
            it != m_committable.end(); ++it)
    {
        ret = std::max(ret, (*it)->this_version());
    }

    for (key_operation_list_t::const_iterator it = m_blocked.begin();
            it != m_blocked.end(); ++it)
    {
        ret = std::max(ret, (*it)->this_version());
    }

    for (key_operation_list_t::const_iterator it = m_deferred.begin();
            it != m_deferred.end(); ++it)
    {
        ret = std::max(ret, (*it)->this_version());
    }

    for (key_change_list_t::const_iterator it = m_changes.begin();
            it != m_changes.end(); ++it)
    {
        ret = std::max(ret, (*it)->version);
    }

    return ret;
}

void
key_state :: reconfigure(e::garbage_collector* gc)
{
    po6::threads::mutex::hold hold(&m_lock);

    while (m_someone_is_working_the_state_machine)
    {
        m_avail.wait();
    }

    stub_client_atomic* sca;
    stub_chain_op* sco;
    stub_chain_subspace* scs;
    stub_chain_ack* scack;

    while (m_client_atomics.pop(gc, &sca))
    {
        delete sca;
    }

    while (m_chain_ops.pop(gc, &sco))
    {
        delete sco;
    }

    while (m_chain_subspaces.pop(gc, &scs))
    {
        delete scs;
    }

    while (m_chain_acks.pop(gc, &scack))
    {
        delete scack;
    }

    m_deferred.clear();
    CHECK_INVARIANTS();
}

void
key_state :: reset(e::garbage_collector* gc)
{
    po6::threads::mutex::hold hold(&m_lock);

    while (m_someone_is_working_the_state_machine)
    {
        m_avail.wait();
    }

    stub_client_atomic* sca;
    stub_chain_op* sco;
    stub_chain_subspace* scs;
    stub_chain_ack* scack;

    while (m_client_atomics.pop(gc, &sca))
    {
        delete sca;
    }

    while (m_chain_ops.pop(gc, &sco))
    {
        delete sco;
    }

    while (m_chain_subspaces.pop(gc, &scs))
    {
        delete scs;
    }

    while (m_chain_acks.pop(gc, &scack))
    {
        delete scack;
    }

    m_someone_is_working_the_state_machine = false;
    m_someone_needs_to_work_the_state_machine = false;
    m_avail.broadcast();

    m_committable.clear();
    m_blocked.clear();
    m_deferred.clear();
    m_changes.clear();
    CHECK_INVARIANTS();
}

void
key_state :: resend_committable(replication_manager* rm,
                                const virtual_server_id& us)
{
    po6::threads::mutex::hold hold(&m_lock);

    while (m_someone_is_working_the_state_machine)
    {
        m_avail.wait();
    }

    CHECK_INVARIANTS();

    for (key_operation_list_t::iterator it = m_committable.begin();
            it != m_committable.end(); ++it)
    {
        // skip those messages already sent in this version
        if ((*it)->sent_version() >= rm->m_daemon->m_config.version())
        {
            continue;
        }

        (*it)->set_sent(0, virtual_server_id());
        rm->send_message(us, m_key, *it);
    }

    CHECK_INVARIANTS();
}

void
key_state :: append_all_versions(std::vector<std::pair<region_id, uint64_t> >* versions)
{
    po6::threads::mutex::hold hold(&m_lock);

    while (m_someone_is_working_the_state_machine)
    {
        m_avail.wait();
    }

    size_t sz = versions->size() + m_committable.size() + m_blocked.size() + m_deferred.size();

    if (versions->capacity() < sz)
    {
        versions->reserve(std::max(sz, versions->capacity() + (versions->capacity() >> 1ULL)));
    }

    for (key_operation_list_t::iterator it = m_committable.begin();
            it != m_committable.end(); ++it)
    {
        versions->push_back(std::make_pair(m_ri, (*it)->this_version()));
    }

    for (key_operation_list_t::iterator it = m_blocked.begin();
            it != m_blocked.end(); ++it)
    {
        versions->push_back(std::make_pair(m_ri, (*it)->this_version()));
    }

    for (key_operation_list_t::iterator it = m_deferred.begin();
            it != m_deferred.end(); ++it)
    {
        versions->push_back(std::make_pair(m_ri, (*it)->this_version()));
    }

    // XXX
}

void
key_state :: debug_dump()
{
    po6::threads::mutex::hold hold(&m_lock);

    while (m_someone_is_working_the_state_machine)
    {
        m_avail.wait();
    }

    LOG(INFO) << " old " << m_old_version;

    for (key_operation_list_t::const_iterator it = m_committable.begin();
            it != m_committable.end(); ++it)
    {
        LOG(INFO) << " committable " << (*it)->this_version();
        (*it)->debug_dump();
    }

    for (key_operation_list_t::const_iterator it = m_blocked.begin();
            it != m_blocked.end(); ++it)
    {
        LOG(INFO) << " blocked " << (*it)->this_version();
        (*it)->debug_dump();
    }

    for (key_operation_list_t::const_iterator it = m_deferred.begin();
            it != m_deferred.end(); ++it)
    {
        LOG(INFO) << " deferred " << (*it)->this_version();
        (*it)->debug_dump();
    }
}

void
key_state :: check_invariants() const
{
    assert(m_initialized);
    uint64_t version = 0;

    for (key_operation_list_t::const_iterator it = m_committable.begin();
            it != m_committable.end(); ++it)
    {
        key_operation* op = it->get();
        assert(op);
        assert(op->prev_version() < op->this_version());
        assert(version == 0 || version == op->prev_version());
        version = op->this_version();
        assert(op->is_continuous() || op->is_discontinuous());
    }

    for (key_operation_list_t::const_iterator it = m_blocked.begin();
            it != m_blocked.end(); ++it)
    {
        key_operation* op = it->get();
        assert(op);
        assert(op->prev_version() < op->this_version());
        assert(version == 0 || version == op->prev_version());
        version = op->this_version();
        assert(op->is_continuous() || op->is_discontinuous());
    }

    uint64_t deferred_max_version = version;

    for (key_operation_list_t::const_iterator it = m_deferred.begin();
            it != m_deferred.end(); ++it)
    {
        key_operation* op = it->get();
        assert(op);
        assert(op->prev_version() < op->this_version());
        assert(version == 0 || version <= op->prev_version());
        assert(version == 0 || version < op->this_version());
        deferred_max_version = std::max(op->this_version(), deferred_max_version);
        assert(op->is_continuous() || op->is_discontinuous());
    }

    version = deferred_max_version;

    for (key_change_list_t::const_iterator it = m_changes.begin();
            it != m_changes.end(); ++it)
    {
        deferred_key_change* dkc = it->get();
        assert(dkc);
        assert(version == 0 || version < dkc->version);
        version = dkc->version;
    }

    assert(m_committable.empty() || m_old_version <= m_committable.back()->this_version());
    assert(m_blocked.empty() || m_old_version < m_blocked.front()->this_version());
    assert(m_deferred.empty() || m_old_version < m_deferred.front()->this_version());
    assert(m_changes.empty() || m_old_version < m_changes.front()->version);

    assert(m_committable_empty == m_committable.empty());
    assert(m_blocked_empty == m_blocked.empty());
    assert(m_deferred_empty == m_deferred.empty());
    assert(m_changes_empty == m_changes.empty());
}

void
key_state :: someone_needs_to_work_the_state_machine()
{
    po6::threads::mutex::hold hold(&m_lock);
    m_someone_needs_to_work_the_state_machine = true;
}

void
key_state :: work_state_machine_or_pass_the_buck(replication_manager* rm,
                                                 const virtual_server_id& us,
                                                 const schema& sc)
{
    if (possibly_takeover_state_machine())
    {
        work_state_machine_with_work_bit(rm, us, sc);
    }
}

void
key_state :: takeover_state_machine()
{
    po6::threads::mutex::hold hold(&m_lock);

    while (m_someone_is_working_the_state_machine)
    {
        m_avail.wait();
    }

    m_someone_is_working_the_state_machine = true;
}

bool
key_state :: possibly_takeover_state_machine()
{
    po6::threads::mutex::hold hold(&m_lock);
    bool have_it = !m_someone_is_working_the_state_machine;
    m_someone_is_working_the_state_machine = true;
    return have_it;
}

void
key_state :: work_state_machine_with_work_bit(replication_manager* rm,
                                              const virtual_server_id& us,
                                              const schema& sc)
{
    while (true)
    {
        m_lock.lock();
        m_someone_needs_to_work_the_state_machine = false;
        m_lock.unlock();
        e::garbage_collector* gc = &rm->m_daemon->m_gc;
        stub_client_atomic* sca;
        stub_chain_op* sco;
        stub_chain_subspace* scs;
        stub_chain_ack* scack;

        while (m_client_atomics.pop(gc, &sca))
        {
            do_client_atomic(rm, us, sc, sca->from, sca->nonce, sca->kc, sca->backing);
            delete sca;
        }

        while (m_chain_ops.pop(gc, &sco))
        {
            do_chain_op(rm, us, sc, sco->from, sco->old_version, sco->new_version, sco->fresh, sco->has_value, sco->value, sco->backing);
            delete sco;
        }

        while (m_chain_subspaces.pop(gc, &scs))
        {
            do_chain_subspace(rm, us, sc, scs->from, scs->old_version, scs->new_version, scs->value, scs->backing,
                              scs->prev_region, scs->this_old_region, scs->this_new_region, scs->next_region);
            delete scs;
        }

        while (m_chain_acks.pop(gc, &scack))
        {
            do_chain_ack(rm, us, sc, scack->from, scack->version);
            delete scack;
        }

        bool did_work = false;
        CHECK_INVARIANTS();

        // Drain the blocked and deferred items first, because order does not
        // matter.
        //
        // Then drain the changes queue as it relies upon the deferred queue to
        // be empty.
        //
        // Finally drain the committable queue to batch-write as many items as
        // possible to the disk.

        if (drain_queue(rm, us, sc, &m_blocked, &key_state::drain_blocked))
        {
            did_work = true;
        }

        if (drain_queue(rm, us, sc, &m_deferred, &key_state::drain_deferred))
        {
            did_work = true;
        }

        if (drain_queue(rm, us, sc, &m_changes, &key_state::drain_changes))
        {
            did_work = true;
        }

        if (did_work && m_committable.size() < 64)
        {
            // continue here because it will not hurt, and the more we can build
            // up the "committable" queue, the more progress we can make
            continue;
        }

        if (drain_queue(rm, us, sc, &m_committable, &key_state::drain_committable))
        {
            did_work = true;
        }

        CHECK_INVARIANTS();
        send_responses(rm, us);

        if (did_work)
        {
            continue;
        }

        bool done = false;
        po6::threads::mutex::hold hold(&m_lock);
        done = !m_someone_needs_to_work_the_state_machine;
        m_someone_is_working_the_state_machine = !done;

        if (done)
        {
            m_committable_empty = m_committable.empty();
            m_blocked_empty = m_blocked.empty();
            m_deferred_empty = m_deferred.empty();
            m_changes_empty = m_changes.empty();
            m_avail.broadcast();
            break;
        }
    }
}

void
key_state :: do_client_atomic(replication_manager* rm,
                              const virtual_server_id&,
                              const schema&,
                              const server_id& from,
                              uint64_t nonce,
                              std::auto_ptr<key_change> kc,
                              std::auto_ptr<e::buffer> backing)
{
    uint64_t version = rm->m_idgen.generate_id(m_ri);

    if (version % datalayer::REGION_PERIODIC == 0)
    {
        rm->m_daemon->m_data.bump_version(m_ri, version);
    }

    e::intrusive_ptr<deferred_key_change> dkc;
    dkc = new deferred_key_change(from, nonce, version, kc, backing);
    m_changes.push_back(dkc);
}

void
key_state :: do_chain_op(replication_manager* rm,
                         const virtual_server_id& us,
                         const schema&,
                         const virtual_server_id& from,
                         uint64_t old_version,
                         uint64_t new_version,
                         bool fresh,
                         bool has_value,
                         const std::vector<e::slice>& value,
                         std::auto_ptr<e::buffer> backing)
{
    e::intrusive_ptr<key_operation> op = get(new_version);
    std::auto_ptr<e::arena> memory(new e::arena());
    memory->takeover(backing.release());

    if (!op)
    {
        op = enqueue_continuous_key_op(old_version, new_version, fresh,
                                       has_value, value, memory);
    }

    assert(op);
    op->set_recv(rm->m_daemon->m_config.version(), from);

    if (op->ackable())
    {
        rm->send_ack(us, m_key, op);
    }
}

void
key_state :: do_chain_subspace(replication_manager* rm,
                               const virtual_server_id& us,
                               const schema&,
                               const virtual_server_id& from,
                               uint64_t old_version,
                               uint64_t new_version,
                               const std::vector<e::slice>& value,
                               std::auto_ptr<e::buffer> backing,
                               const region_id& prev_region,
                               const region_id& this_old_region,
                               const region_id& this_new_region,
                               const region_id& next_region)
{
    e::intrusive_ptr<key_operation> op = get(new_version);
    std::auto_ptr<e::arena> memory(new e::arena());
    memory->takeover(backing.release());

    if (!op)
    {
        op = enqueue_discontinuous_key_op(old_version, new_version,
                                          value, memory,
                                          prev_region, this_old_region,
                                          this_new_region, next_region);
    }

    assert(op);
    op->set_recv(rm->m_daemon->m_config.version(), from);

    if (op->ackable())
    {
        rm->send_ack(us, m_key, op);
    }
}

void
key_state :: do_chain_ack(replication_manager* rm,
                          const virtual_server_id& us,
                          const schema&,
                          const virtual_server_id& from,
                          uint64_t version)
{
    e::intrusive_ptr<key_operation> op = get(version);

    if (!op)
    {
        LOG(ERROR) << "dropping CHAIN_ACK for update we haven't seen";
        LOG(ERROR) << "troubleshoot info: from=" << from << " to=" << us
                   << " version=" << version << " key=" << m_key.hex();
        return;
    }

    if (!op->sent_to(rm->m_daemon->m_config.version(), from))
    {
        return;
    }

    op->mark_acked();
    rm->send_ack(us, m_key, op);
    rm->collect(m_ri, op);
}

void
key_state :: add_response(const client_response& cr)
{
    m_client_responses_heap.push_back(cr);
    std::push_heap(m_client_responses_heap.begin(),
                   m_client_responses_heap.end());
}

void
key_state :: send_responses(replication_manager* rm,
                            const virtual_server_id& us)
{
    while (!m_client_responses_heap.empty() &&
           m_client_responses_heap[0].respond_after <= m_old_version)
    {
        const client_response& cr(m_client_responses_heap[0]);
        rm->respond_to_client(us, cr.client, cr.nonce, cr.ret);

        std::pop_heap(m_client_responses_heap.begin(),
                      m_client_responses_heap.end());
        m_client_responses_heap.pop_back();
    }
}

namespace
{

bool
get_by_version(const std::list<e::intrusive_ptr<key_operation> >& list,
               uint64_t version, e::intrusive_ptr<key_operation>* op)
{
    if (list.empty() || list.back()->this_version() < version)
    {
        return false;
    }

    for (std::list<e::intrusive_ptr<key_operation> >::const_iterator it = list.begin();
            it != list.end(); ++it)
    {
        uint64_t v = (*it)->this_version();

        if (v == version)
        {
            *op = *it;
            return true;
        }
        else if (v > version)
        {
            return false;
        }
    }

    return false;
}

} // namespace

e::intrusive_ptr<key_operation>
key_state :: get(uint64_t new_version)
{
    CHECK_INVARIANTS();
    e::intrusive_ptr<key_operation> op;

    if (get_by_version(m_committable, new_version, &op) ||
        get_by_version(m_blocked, new_version, &op) ||
        get_by_version(m_deferred, new_version, &op))
    {
        assert(op);
        return op;
    }

    return NULL;
}

e::intrusive_ptr<key_operation>
key_state :: enqueue_continuous_key_op(uint64_t old_version,
                                       uint64_t new_version,
                                       bool fresh,
                                       bool has_value,
                                       const std::vector<e::slice>& value,
                                       std::auto_ptr<e::arena> memory)
{
    e::intrusive_ptr<key_operation> op;
    op = new key_operation(old_version, new_version, fresh,
                           has_value, value, memory);
    op->set_continuous();

    if ((!has_value && !m_has_old_value) ||
        (new_version <= m_old_version))
    {
        op->mark_acked();
        return op;
    }

    m_deferred.push_back(op);
    return op;
}

e::intrusive_ptr<key_operation>
key_state :: enqueue_discontinuous_key_op(uint64_t old_version,
                                          uint64_t new_version,
                                          const std::vector<e::slice>& value,
                                          std::auto_ptr<e::arena> memory,
                                          const region_id& prev_region,
                                          const region_id& this_old_region,
                                          const region_id& this_new_region,
                                          const region_id& next_region)
{
    e::intrusive_ptr<key_operation> op;
    op = new key_operation(old_version, new_version, false,
                           true, value, memory);
    op->set_discontinuous(prev_region, this_old_region, this_new_region, next_region);
    m_deferred.push_back(op);
    return op;
}

void
key_state :: get_latest(bool* has_old_value,
                        uint64_t* old_version,
                        const std::vector<e::slice>** old_value)
{
    CHECK_INVARIANTS();
    *old_version = 0;

    // We try to pick the highest version of the key we know of here
    // We know that the pending and commitable operations are sorted so we only look at their back

    if (!m_blocked.empty() && m_blocked.back()->this_version() > *old_version)
    {
        *has_old_value = m_blocked.back()->has_value();
        *old_version = m_blocked.back()->this_version();
        *old_value = &m_blocked.back()->value();
    }

    if (!m_committable.empty() && m_committable.back()->this_version() > *old_version)
    {
        *has_old_value = m_committable.back()->has_value();
        *old_version = m_committable.back()->this_version();
        *old_value = &m_committable.back()->value();
    }

    if (m_old_version > *old_version)
    {
        *has_old_value = m_has_old_value;
        *old_version = m_old_version;
        *old_value = &m_old_value;
    }
}

bool
key_state :: drain_queue(replication_manager* rm,
                         const virtual_server_id& us,
                         const schema& sc,
                         key_operation_list_t* q,
                         void (key_state::*f)(replication_manager* rm,
                                              const virtual_server_id& us,
                                              const schema& sc))
{
    bool did_work = false;

    while (!q->empty())
    {
        CHECK_INVARIANTS();
        size_t before = q->size();
        (this->*f)(rm, us, sc);
        size_t after = q->size();

        if (before > after)
        {
            did_work = true;
        }
        else
        {
            break;
        }
    }

    return did_work;
}

bool
key_state :: drain_queue(replication_manager* rm,
                         const virtual_server_id& us,
                         const schema& sc,
                         key_change_list_t* q,
                         void (key_state::*f)(replication_manager* rm,
                                              const virtual_server_id& us,
                                              const schema& sc))
{
    bool did_work = false;

    while (!q->empty())
    {
        CHECK_INVARIANTS();
        size_t before = q->size();
        (this->*f)(rm, us, sc);
        size_t after = q->size();

        if (before > after)
        {
            did_work = true;
        }
        else
        {
            break;
        }
    }

    return did_work;
}

void
key_state :: drain_changes(replication_manager*,
                           const virtual_server_id&,
                           const schema& sc)
{
    assert(!m_changes.empty());

    if (!m_deferred.empty())
    {
        return;
    }

    assert(sc.attrs_sz > 0);
    bool has_old_value = false;
    uint64_t old_version = 0;
    const std::vector<e::slice>* old_value = NULL;
    get_latest(&has_old_value, &old_version, &old_value);

    e::intrusive_ptr<deferred_key_change> dkc = m_changes.front();
    m_changes.pop_front();
    key_change* kc = dkc->kc.get();

    if (!auth_verify_write(sc, has_old_value, old_value, *kc))
    {
        add_response(client_response(old_version, dkc->from, dkc->nonce, NET_UNAUTHORIZED));
        return;
    }

    network_returncode nrc = kc->check(sc, has_old_value, old_value);

    if (nrc != NET_SUCCESS)
    {
        add_response(client_response(old_version, dkc->from, dkc->nonce, nrc));
        return;
    }

    if (kc->erase)
    {
        e::intrusive_ptr<key_operation> op;
        op = new key_operation(old_version, dkc->version, false,
                               false, std::vector<e::slice>(sc.attrs_sz - 1),
                               std::auto_ptr<e::arena>());
        op->set_continuous();
        add_response(client_response(dkc->version, dkc->from, dkc->nonce, NET_SUCCESS));
        m_deferred.push_back(op);
        return;
    }

    std::auto_ptr<e::arena> memory(new e::arena());
    std::vector<e::slice> new_value(sc.attrs_sz - 1);

    // if there is no old value, pretend it is "new_value" which is
    // zero-initialized
    if (!has_old_value)
    {
        old_value = &new_value;
    }

    size_t funcs_passed = apply_funcs(sc, kc->funcs, m_key, *old_value, memory.get(), &new_value);

    if (funcs_passed < kc->funcs.size())
    {
        add_response(client_response(old_version, dkc->from, dkc->nonce, NET_CMPFAIL));
        return;
    }

    e::intrusive_ptr<key_operation> op;
    op = new key_operation(old_version, dkc->version, !has_old_value,
                           true, new_value, memory);
    op->set_continuous();
    add_response(client_response(dkc->version, dkc->from, dkc->nonce, NET_SUCCESS));
    m_deferred.push_back(op);
}

bool
key_state :: compare_key_op_ptrs(const e::intrusive_ptr<key_operation>& lhs,
                                 const e::intrusive_ptr<key_operation>& rhs)
{
    return lhs->this_version() < rhs->this_version();
}

void
key_state :: drain_deferred(replication_manager* rm,
                            const virtual_server_id& us,
                            const schema& sc)
{
    assert(!m_deferred.empty());
    m_deferred.sort(compare_key_op_ptrs);
    bool has_old_value = false;
    uint64_t old_version = 0;
    const std::vector<e::slice>* old_value = NULL;
    get_latest(&has_old_value, &old_version, &old_value);
    e::intrusive_ptr<key_operation> op = m_deferred.front();

    if (old_version >= op->this_version())
    {
        LOG(WARNING) << "dropping deferred CHAIN_* which was sent out of order: "
                     << "we're using key " << e::slice(state_key().key).hex() << " in region "
                     << state_key().region
                     << ".  We've already seen " << old_version << " and the CHAIN_* "
                     << "is for version " << op->this_version();
        m_deferred.pop_front();
        return;
    }

    // if the op is continous (the version numbers are assumed to line up), and
    // the op is not fresh (the only case a continuous op can have discontinuous
    // version numbers), then it cannot be applied yet
    if (op->is_continuous() && !op->is_fresh() && old_version != op->prev_version())
    {
        return;
    }

    if (op->is_continuous())
    {
        hash_objects(&rm->m_daemon->m_config, m_ri, sc,
                     op->has_value(), op->value(),
                     has_old_value, old_value ? *old_value : op->value(), op);
    }

    // check that this host is supposed to process this op
    if (op->this_old_region() != m_ri && op->this_new_region() != m_ri)
    {
        LOG(WARNING) << "dropping deferred CHAIN_* which didn't get sent to the right host: "
                     << "we're using key " << e::slice(state_key().key).hex() << " in region "
                     << state_key().region
                     << ".  We've already seen " << old_version << " and the CHAIN_* "
                     << "is for version " << op->this_version();
        m_deferred.pop_front();
        return;
    }

    // check that the sender was the correct sender
    if (op->is_continuous() &&
        op->recv_from() != virtual_server_id() &&
        rm->m_daemon->m_config.next_in_region(op->recv_from()) != us &&
        !rm->m_daemon->m_config.subspace_adjacent(op->recv_from(), us))
    {
        LOG(WARNING) << "dropping deferred CHAIN_OP which didn't come from the right host: "
                     << "we're using key " << e::slice(state_key().key).hex() << " in region "
                     << state_key().region
                     << ".  We received the bad update from " << op->recv_from();
        m_deferred.pop_front();
        return;
    }

    if (op->is_discontinuous() &&
        op->recv_from() != virtual_server_id() &&
        rm->m_daemon->m_config.next_in_region(op->recv_from()) != us &&
        rm->m_daemon->m_config.tail_of_region(op->this_old_region()) != op->recv_from())
    {
        LOG(WARNING) << "dropping deferred CHAIN_SUBSPACE which didn't come from the right host: "
                     << "we're using key " << e::slice(state_key().key).hex() << " in region "
                     << state_key().region
                     << ".  We received the bad update from " << op->recv_from();
        m_deferred.pop_front();
        return;
    }

    if (op->is_fresh() || op->is_discontinuous() || old_version == op->prev_version())
    {
        m_blocked.push_back(op);
        m_deferred.pop_front();
        return;
    }
}

void
key_state :: drain_blocked(replication_manager* rm,
                           const virtual_server_id& us,
                           const schema&)
{
    assert(!m_blocked.empty());
    e::intrusive_ptr<key_operation> op = m_blocked.front();
    bool withold = false;
    // if the op is on either side of a delete, or is a delete itself
    withold = withold || op->is_fresh();
    withold = withold || !op->has_value();
    withold = withold || (!m_committable.empty() && !m_committable.back()->has_value());
    withold = withold || op->is_discontinuous();
    withold = withold || (!m_committable.empty() && m_committable.back()->is_discontinuous());

    if (withold && !m_committable.empty())
    {
        return;
    }

    m_committable.push_back(op);
    m_blocked.pop_front();
    rm->send_message(us, m_key, op);
}

void
key_state :: drain_committable(replication_manager* rm,
                               const virtual_server_id&,
                               const schema&)
{
    assert(!m_committable.empty());
    bool found = false;
    uint64_t version = 0;

    for (key_operation_list_t::iterator it = m_committable.begin();
            it != m_committable.end(); ++it)
    {
        key_operation* op = it->get();

        if (op->ackable())
        {
            found = true;
            assert(version <= op->this_version());
            version = op->this_version();
        }
    }

    if (found && m_old_version < version)
    {
        CHECK_INVARIANTS();
        e::intrusive_ptr<key_operation> op = get(version);
        assert(op);
        assert(op->this_version() == version);
        datalayer::returncode rc = datalayer::SUCCESS;

        // if this is a case where we are to remove the object from disk
        // because of a delete or the first half of a subspace transfer
        if (!op->has_value() ||
            (op->this_old_region() != op->this_new_region() && m_ri == op->this_old_region()))
        {
            if (m_has_old_value)
            {
                rc = rm->m_daemon->m_data.del(m_ri, m_key, m_old_value);
            }
        }
        // otherwise it is a case where we are to place this object on disk
        else
        {
            if (m_has_old_value)
            {
                rc = rm->m_daemon->m_data.overput(m_ri, m_key, m_old_value, op->value(), version);
            }
            else
            {
                rc = rm->m_daemon->m_data.put(m_ri, m_key, op->value(), version);
            }
        }

        switch (rc)
        {
            case datalayer::SUCCESS:
                break;
            case datalayer::NOT_FOUND:
            case datalayer::BAD_ENCODING:
            case datalayer::CORRUPTION:
            case datalayer::IO_ERROR:
            case datalayer::LEVELDB_ERROR:
                return; // XXX
            default:
                return; // XXX
        }

        m_has_old_value = op->has_value();
        m_old_version = version;
        m_old_value = op->value();
        m_old_op = op;
        CHECK_INVARIANTS();
    }

    while (!m_committable.empty() && m_committable.front()->ackable())
    {
        assert(m_committable.front()->this_version() <= m_old_version);
        m_committable.pop_front();
        CHECK_INVARIANTS();
    }
}

void
key_state :: hash_objects(const configuration* config,
                          const region_id& reg,
                          const schema& sc,
                          bool has_new_value,
                          const std::vector<e::slice>& new_value,
                          bool has_old_value,
                          const std::vector<e::slice>& old_value,
                          e::intrusive_ptr<key_operation> op)
{
    std::vector<uint64_t> old_hashes(sc.attrs_sz);
    std::vector<uint64_t> new_hashes(sc.attrs_sz);
    region_id this_old_region;
    region_id this_new_region;
    region_id prev_region;
    region_id next_region;
    subspace_id subspace_this = config->subspace_of(reg);
    subspace_id subspace_prev = config->subspace_prev(subspace_this);
    subspace_id subspace_next = config->subspace_next(subspace_this);

    if (has_old_value && has_new_value)
    {
        hyperdex::hash(sc, m_key, new_value, &new_hashes.front());
        hyperdex::hash(sc, m_key, old_value, &old_hashes.front());

        if (subspace_prev != subspace_id())
        {
            config->lookup_region(subspace_prev, new_hashes, &prev_region);
        }

        config->lookup_region(subspace_this, old_hashes, &this_old_region);
        config->lookup_region(subspace_this, new_hashes, &this_new_region);

        if (subspace_next != subspace_id())
        {
            config->lookup_region(subspace_next, old_hashes, &next_region);
        }
    }
    else if (has_old_value)
    {
        hyperdex::hash(sc, m_key, old_value, &old_hashes.front());

        for (size_t i = 0; i < sc.attrs_sz; ++i)
        {
            new_hashes[i] = old_hashes[i];
        }

        if (subspace_prev != subspace_id())
        {
            config->lookup_region(subspace_prev, old_hashes, &prev_region);
        }

        config->lookup_region(subspace_this, old_hashes, &this_old_region);
        this_new_region = this_old_region;

        if (subspace_next != subspace_id())
        {
            config->lookup_region(subspace_next, old_hashes, &next_region);
        }
    }
    else if (has_new_value)
    {
        hyperdex::hash(sc, m_key, new_value, &new_hashes.front());

        for (size_t i = 0; i < sc.attrs_sz; ++i)
        {
            old_hashes[i] = new_hashes[i];
        }

        if (subspace_prev != subspace_id())
        {
            config->lookup_region(subspace_prev, old_hashes, &prev_region);
        }

        config->lookup_region(subspace_this, old_hashes, &this_new_region);
        this_old_region = this_new_region;

        if (subspace_next != subspace_id())
        {
            config->lookup_region(subspace_next, old_hashes, &next_region);
        }
    }
    else
    {
        abort();
    }

    op->set_continuous_hashes(prev_region, this_old_region, this_new_region, next_region);
}
