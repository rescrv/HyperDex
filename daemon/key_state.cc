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
};

key_state :: key_state(const key_region& kr)
    : m_ref(0)
    , m_ri(kr.region)
    , m_key_backing(reinterpret_cast<const char*>(kr.key.data()), kr.key.size())
    , m_key(m_key_backing.data(), m_key_backing.size())
    , m_lock()
    , m_marked_garbage(false)
    , m_initialized(false)
    , m_has_old_value(false)
    , m_old_version(0)
    , m_old_value()
    , m_old_disk_ref()
    , m_old_op()
    , m_committable()
    , m_blocked()
    , m_deferred()
    , m_changes()
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

void
key_state :: lock()
{
    m_lock.lock();
}

void
key_state :: unlock()
{
    m_lock.unlock();
}

bool
key_state :: finished()
{
    return m_committable.empty() && m_blocked.empty() && m_deferred.empty() && m_changes.empty();
}

void
key_state :: mark_garbage()
{
    m_marked_garbage = true;
}

bool
key_state :: marked_garbage() const
{
    return m_marked_garbage;
}

bool
key_state :: initialized() const
{
    return m_initialized;
}

hyperdex::datalayer::returncode
key_state :: initialize(datalayer* data, const region_id& ri)
{
    assert(!m_initialized);
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

void
key_state :: enqueue_key_change(const server_id& from,
                                uint64_t nonce,
                                uint64_t seq_id,
                                std::auto_ptr<key_change> kc,
                                std::auto_ptr<e::buffer> backing)
{
    e::intrusive_ptr<deferred_key_change> dkc;
    dkc = new deferred_key_change(from, nonce, seq_id, kc, backing);
    m_changes.push_back(dkc);
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
                                       std::auto_ptr<e::buffer> backing)
{
    e::intrusive_ptr<key_operation> op;
    op = new key_operation(old_version, new_version, fresh,
                           has_value, value, backing);
    op->set_continuous();

    if ((!has_value && finished() && !m_has_old_value) ||
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
                                          std::auto_ptr<e::buffer> backing,
                                          const region_id& prev_region,
                                          const region_id& this_old_region,
                                          const region_id& this_new_region,
                                          const region_id& next_region)
{
    e::intrusive_ptr<key_operation> op;
    op = new key_operation(old_version, new_version, false,
                           true, value, backing);
    op->set_discontinuous(prev_region, this_old_region, this_new_region, next_region);
    m_deferred.push_back(op);
    return op;
}

void
key_state :: update_datalayer(replication_manager* rm, uint64_t version)
{
    if (m_old_version < version)
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
key_state :: resend_committable(replication_manager* rm,
                                const virtual_server_id& us)
{
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
key_state :: work_state_machine(replication_manager* rm,
                                const virtual_server_id& us,
                                const schema& sc)
{
    CHECK_INVARIANTS();

    while (!m_changes.empty() ||
           !m_deferred.empty() ||
           !m_blocked.empty())
    {
        CHECK_INVARIANTS();

        if (!step_state_machine_changes(rm, us, sc) &&
            !step_state_machine_deferred(rm, us, sc) &&
            !step_state_machine_blocked(rm, us))
        {
            break;
        }
    }

    CHECK_INVARIANTS();
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
key_state :: step_state_machine_changes(replication_manager* rm,
                                        const virtual_server_id& us,
                                        const schema& sc)
{
    if (!m_deferred.empty() || m_changes.empty())
    {
        return false;
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
        rm->respond_to_client(us, dkc->from, dkc->nonce, NET_UNAUTHORIZED);
        return true;
    }

    network_returncode nrc = kc->check(sc, has_old_value, old_value);

    if (nrc != NET_SUCCESS)
    {
        rm->respond_to_client(us, dkc->from, dkc->nonce, nrc);
        return true;
    }

    if (kc->erase)
    {
        e::intrusive_ptr<key_operation> op;
        op = new key_operation(old_version, dkc->version, false,
                               false, std::vector<e::slice>(sc.attrs_sz - 1),
                               std::auto_ptr<e::buffer>());
        op->set_continuous();
        op->set_client(dkc->from, dkc->nonce);
        m_deferred.push_back(op);
        return true;
    }

    std::auto_ptr<e::buffer> backing;
    std::vector<e::slice> new_value(sc.attrs_sz - 1);

    // if there is no old value, pretend it is "new_value" which is
    // zero-initialized
    if (!has_old_value)
    {
        old_value = &new_value;
    }

    size_t funcs_passed = apply_funcs(sc, kc->funcs, m_key, *old_value, &backing, &new_value);

    if (funcs_passed < kc->funcs.size())
    {
        rm->respond_to_client(us, dkc->from, dkc->nonce, NET_OVERFLOW);
        return true;
    }

    e::intrusive_ptr<key_operation> op;
    op = new key_operation(old_version, dkc->version, !has_old_value,
                           true, new_value, backing);
    op->set_continuous();
    op->set_client(dkc->from, dkc->nonce);
    m_deferred.push_back(op);

    return true;
}

bool
key_state :: compare_key_op_ptrs(const e::intrusive_ptr<key_operation>& lhs,
                                 const e::intrusive_ptr<key_operation>& rhs)
{
    return lhs->this_version() < rhs->this_version();
}

bool
key_state :: step_state_machine_deferred(replication_manager* rm,
                                         const virtual_server_id& us,
                                         const schema& sc)
{
    if (m_deferred.empty())
    {
        return false;
    }

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
        return true;
    }

    // if the op is continous (the version numbers are assumed to line up), and
    // the op is not fresh (the only case a continuous op can have discontinuous
    // version numbers), then it cannot be applied yet
    if (op->is_continuous() && !op->is_fresh() && old_version != op->prev_version())
    {
        return false;
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
        return true;
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
        return true;
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
        return true;
    }

    if (op->is_fresh() || op->is_discontinuous() || old_version == op->prev_version())
    {
        m_blocked.push_back(op);
        m_deferred.pop_front();
        return true;
    }

    return false;
}

bool
key_state :: step_state_machine_blocked(replication_manager* rm, const virtual_server_id& us)
{
    if (m_blocked.empty())
    {
        return false;
    }

    assert(m_deferred.empty());
    assert(!m_blocked.empty());
    e::intrusive_ptr<key_operation> op = m_blocked.front();
    bool withold = false;
    // if the op is on either side of a delete, or is a delete itself
    withold = withold || op->is_fresh();
    withold = withold || !op->has_value();
    withold = withold || (!m_committable.empty() && !m_committable.back()->has_value());
    withold = withold || !op->is_discontinuous();
    withold = withold || (!m_committable.empty() && !m_committable.back()->is_discontinuous());

    if (withold && !m_committable.empty())
    {
        return false;
    }

    m_committable.push_back(op);
    m_blocked.pop_front();
    rm->send_message(us, m_key, op);
    return true;
}

void
key_state :: reconfigure()
{
    CHECK_INVARIANTS();
    m_deferred.clear();
    CHECK_INVARIANTS();
}

void
key_state :: reset()
{
    CHECK_INVARIANTS();
    m_committable.clear();
    m_blocked.clear();
    m_deferred.clear();
    m_changes.clear();
    assert(finished());
    CHECK_INVARIANTS();
}

void
key_state :: append_all_versions(std::vector<std::pair<region_id, uint64_t> >* versions)
{
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
}

uint64_t
key_state :: max_version() const
{
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
key_state :: debug_dump() const
{
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
    assert(!m_marked_garbage);
    assert(m_initialized);
    assert(m_ref > 0);
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

    for (key_operation_list_t::const_iterator it = m_deferred.begin();
            it != m_deferred.end(); ++it)
    {
        key_operation* op = it->get();
        assert(op);
        assert(op->prev_version() < op->this_version());
        assert(version == 0 || version <= op->prev_version());
        version = op->this_version();
        assert(op->is_continuous() || op->is_discontinuous());
    }

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

    op->set_continuous(prev_region, this_old_region, this_new_region, next_region);
}
