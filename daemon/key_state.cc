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

key_state :: key_state(const key_region& kr)
    : m_ri(kr.region)
    , m_key_backing(reinterpret_cast<const char*>(kr.key.data()), kr.key.size())
    , m_key(m_key_backing.data(), m_key_backing.size())
    , m_lock()
    , m_marked_garbage(false)
    , m_initialized(false)
    , m_ref(0)
    , m_committable()
    , m_blocked()
    , m_deferred()
    , m_has_old_value(false)
    , m_old_version(0)
    , m_old_value()
    , m_old_disk_ref()
    , m_old_backing()
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
    return empty();
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

bool
key_state :: empty() const
{
    return m_committable.empty() && m_blocked.empty() && m_deferred.empty();
}

void
key_state :: clear()
{
    m_committable.clear();
    m_blocked.clear();
    m_deferred.clear();
    assert(empty());
    CHECK_INVARIANTS();
}

uint64_t
key_state :: max_seq_id() const
{
    uint64_t ret = 0;

    for (key_operation_list_t::const_iterator it = m_committable.begin();
            it != m_committable.end(); ++it)
    {
        ret = std::max(ret, it->second->seq_id);
    }

    for (key_operation_list_t::const_iterator it = m_blocked.begin();
            it != m_blocked.end(); ++it)
    {
        ret = std::max(ret, it->second->seq_id);
    }

    for (key_operation_list_t::const_iterator it = m_deferred.begin();
            it != m_deferred.end(); ++it)
    {
        ret = std::max(ret, it->second->seq_id);
    }

    CHECK_INVARIANTS();
    return ret;
}

uint64_t
key_state :: min_seq_id() const
{
    uint64_t ret = 0;

    for (key_operation_list_t::const_iterator it = m_committable.begin();
            it != m_committable.end(); ++it)
    {
        ret = std::min(ret, it->second->seq_id);
    }

    for (key_operation_list_t::const_iterator it = m_blocked.begin();
            it != m_blocked.end(); ++it)
    {
        ret = std::min(ret, it->second->seq_id);
    }

    for (key_operation_list_t::const_iterator it = m_deferred.begin();
            it != m_deferred.end(); ++it)
    {
        ret = std::min(ret, it->second->seq_id);
    }

    CHECK_INVARIANTS();
    return ret;
}

e::intrusive_ptr<key_operation>
key_state :: get_version(uint64_t version) const
{
    CHECK_INVARIANTS();

    if (!m_committable.empty() && m_committable.back().first >= version)
    {
        for (key_operation_list_t::const_iterator c = m_committable.begin();
                c != m_committable.end(); ++c)
        {
            if (c->first == version)
            {
                return c->second;
            }
            else if (c->first > version)
            {
                return NULL;
            }
        }
    }

    if (!m_blocked.empty() && m_blocked.back().first >= version)
    {
        for (key_operation_list_t::const_iterator b = m_blocked.begin();
                b != m_blocked.end(); ++b)
        {
            if (b->first == version)
            {
                return b->second;
            }
            else if (b->first > version)
            {
                return NULL;
            }
        }
    }

    return NULL;
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

bool
key_state :: check_against_latest_version(const schema& sc,
                                          bool erase,
                                          bool fail_if_not_found,
                                          bool fail_if_found,
                                          const std::vector<attribute_check>& checks,
                                          network_returncode* nrc)
{
    assert(m_deferred.empty());
    assert(sc.attrs_sz > 0);
    bool has_old_value = false;
    uint64_t old_version = 0;
    std::vector<e::slice>* old_value = NULL;
    get_latest(&has_old_value, &old_version, &old_value);
    CHECK_INVARIANTS();
    return check_version(sc, erase, fail_if_not_found, fail_if_found, checks,
                         has_old_value, old_version, old_value, nrc);
}

void
key_state :: delete_latest(const schema& sc,
                           const region_id& reg_id, uint64_t seq_id,
                           const server_id& client, uint64_t nonce)
{
    assert(m_deferred.empty());
    assert(sc.attrs_sz > 0);
    e::intrusive_ptr<key_operation> op;
    op = new key_operation(std::auto_ptr<e::buffer>(),
                           reg_id, seq_id, false,
                           false, std::vector<e::slice>(sc.attrs_sz - 1),
                           client, nonce,
                           0, virtual_server_id());
    bool has_old_value = false;
    uint64_t old_version = 0;
    std::vector<e::slice>* old_value = NULL;
    get_latest(&has_old_value, &old_version, &old_value);
    insert_deferred(old_version + 1, op);
    CHECK_INVARIANTS();
}

bool
key_state :: put_from_funcs(const schema& sc,
                            const region_id& reg_id, uint64_t seq_id,
                            const std::vector<funcall>& funcs,
                            const server_id& client, uint64_t nonce)
{
    assert(m_deferred.empty());
    bool has_old_value = false;
    uint64_t old_version = 0;
    std::vector<e::slice>* old_value = NULL;
    get_latest(&has_old_value, &old_version, &old_value);
    std::auto_ptr<e::buffer> backing;
    std::vector<e::slice> new_value(sc.attrs_sz - 1);

    // if there is no old value, pretend it is "new_value" which is
    // zero-initialized
    if (!has_old_value)
    {
        old_value = &new_value;
    }

    size_t funcs_passed = apply_funcs(sc, funcs, m_key, *old_value, &backing, &new_value);
    e::intrusive_ptr<key_operation> op;
    op = new key_operation(backing,
                           reg_id, seq_id, !has_old_value,
                           true, new_value,
                           client, nonce,
                           0, virtual_server_id());

    CHECK_INVARIANTS();
    assert(m_committable.empty() || old_version + 1 > m_committable.back().first);
    assert(m_blocked.empty() || old_version + 1 > m_blocked.back().first);
    assert(m_deferred.empty() || old_version + 1 > m_deferred.back().first);

    if (funcs_passed == funcs.size())
    {
        insert_deferred(old_version + 1, op);
        CHECK_INVARIANTS();
        return true;
    }

    return false;
}

void
key_state :: insert_deferred(uint64_t version, e::intrusive_ptr<key_operation> op)
{
    CHECK_INVARIANTS();
    key_operation_list_t::iterator d = m_deferred.begin();

    while (d != m_deferred.end() && d->first <= version)
    {
        ++d;
    }

    assert(d == m_deferred.end() || d->first > version);
    m_deferred.insert(d, std::make_pair(version, op));
    CHECK_INVARIANTS();
}

bool
key_state :: persist_to_datalayer(replication_manager* rm,
                                  const region_id& ri,
                                  const region_id& reg_id,
                                  uint64_t seq_id,
                                  uint64_t version)
{
    if (m_old_version < version)
    {
        CHECK_INVARIANTS();
        e::intrusive_ptr<key_operation> op = get_version(version);
        assert(op);
        datalayer::returncode rc;

        // if this is a case where we are to remove the object from disk
        if (!op->has_value || (op->this_old_region != op->this_new_region && ri == op->this_old_region))
        {
            if (m_has_old_value)
            {
                rc = rm->m_daemon->m_data.del(ri, reg_id, seq_id, m_key, m_old_value);
            }
            else
            {
                rm->m_daemon->m_data.mark_acked(ri, reg_id, seq_id);
                rc = datalayer::SUCCESS;
            }
        }
        // otherwise it is a case where we are to place this object on disk
        else
        {
            if (m_has_old_value)
            {
                rc = rm->m_daemon->m_data.overput(ri, reg_id, seq_id, m_key, m_old_value, op->value, version);
            }
            else
            {
                rc = rm->m_daemon->m_data.put(ri, reg_id, seq_id, m_key, op->value, version);
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
                return false;
            default:
                return false;
        }

        m_has_old_value = op->has_value;
        m_old_version = version;
        m_old_value = op->value;
        m_old_backing = op->backing;
        CHECK_INVARIANTS();
    }
    else
    {
        rm->m_daemon->m_data.mark_acked(ri, reg_id, seq_id);
    }

    CHECK_INVARIANTS();
    return true;
}

void
key_state :: clear_deferred()
{
    CHECK_INVARIANTS();
    m_deferred.clear();
    CHECK_INVARIANTS();
}

void
key_state :: clear_acked_prefix()
{
    CHECK_INVARIANTS();

    while (!m_committable.empty() && m_committable.front().second->acked)
    {
        assert(m_committable.front().first <= m_old_version);
        m_committable.pop_front();
    }

    CHECK_INVARIANTS();
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
        if (it->second->sent_config_version == rm->m_daemon->m_config.version())
        {
            continue;
        }

        it->second->sent = virtual_server_id();
        it->second->sent_config_version = 0;
        rm->send_message(us, true, it->first, m_key, it->second);
    }

    CHECK_INVARIANTS();
}

void
key_state :: append_seq_ids(std::vector<std::pair<region_id, uint64_t> >* seq_ids)
{
    size_t sz = seq_ids->size() + m_committable.size() + m_blocked.size() + m_deferred.size();

    if (seq_ids->capacity() < sz)
    {
        seq_ids->reserve(sz);
    }

    for (key_operation_list_t::iterator it = m_committable.begin();
            it != m_committable.end(); ++it)
    {
        seq_ids->push_back(std::make_pair(m_ri, it->second->seq_id));
    }

    for (key_operation_list_t::iterator it = m_blocked.begin();
            it != m_blocked.end(); ++it)
    {
        seq_ids->push_back(std::make_pair(m_ri, it->second->seq_id));
    }

    for (key_operation_list_t::iterator it = m_deferred.begin();
            it != m_deferred.end(); ++it)
    {
        seq_ids->push_back(std::make_pair(m_ri, it->second->seq_id));
    }
}

void
key_state :: move_operations_between_queues(replication_manager* rm,
                                            const virtual_server_id& us,
                                            const region_id& ri,
                                            const schema& sc)
{
    // Apply deferred operations
    while (!m_deferred.empty())
    {
        CHECK_INVARIANTS();
        bool has_old_value = false;
        uint64_t old_version = 0;
        std::vector<e::slice>* old_value = NULL;
        get_latest(&has_old_value, &old_version, &old_value);

        if (old_version >= m_deferred.front().first)
        {
            LOG(WARNING) << "dropping deferred CHAIN_* which was sent out of order: "
                         << "we're using key " << e::slice(state_key().key).hex() << " in region "
                         << state_key().region
                         << ".  We've already seen " << old_version << " and the CHAIN_* "
                         << "is for version " << m_deferred.front().first;
            m_deferred.pop_front();
            continue;
        }

        e::intrusive_ptr<key_operation> op = m_deferred.front().second;

        // If the version numbers don't line up, and this is not fresh, and it
        // is not a subspace transfer.  Note that if this were a subspace
        // transfer, "op->this_new_region == ri" and "op->this_old_region !=
        // op->this_new_region".
        if (old_version + 1 != m_deferred.front().first &&
            !op->fresh &&
            (op->this_old_region == op->this_new_region ||
             op->this_old_region == ri))
        {
            break;
        }

        // If this is not a subspace transfer
        if (op->this_old_region == op->this_new_region ||
            op->this_old_region == ri)
        {
            hash_objects(&rm->m_daemon->m_config, ri, sc, op->has_value, op->value, has_old_value, old_value ? *old_value : op->value, op);

            if (op->this_old_region != ri && op->this_new_region != ri)
            {
                LOG(INFO) << "dropping deferred CHAIN_* which didn't get sent to the right host";
                m_deferred.pop_front();
                continue;
            }

            if (op->recv != virtual_server_id() &&
                rm->m_daemon->m_config.next_in_region(op->recv) != us &&
                !rm->m_daemon->m_config.subspace_adjacent(op->recv, us))
            {
                LOG(INFO) << "dropping deferred CHAIN_* which didn't come from the right host";
                m_deferred.pop_front();
                continue;
            }
        }

        m_blocked.push_back(m_deferred.front());
        m_deferred.pop_front();
    }

    // Issue blocked operations
    while (!m_blocked.empty())
    {
        CHECK_INVARIANTS();
        uint64_t version = m_blocked.front().first;
        e::intrusive_ptr<key_operation> op = m_blocked.front().second;

        // If the op is on either side of a delete, wait until there are no more
        // committable ops
        if ((op->fresh || !op->has_value) && !m_committable.empty())
        {
            break;
        }

        m_committable.push_back(m_blocked.front());
        m_blocked.pop_front();
        rm->send_message(us, false, version, m_key, op);
    }

    CHECK_INVARIANTS();
}

void
key_state :: debug_dump()
{
    for (key_operation_list_t::iterator it = m_committable.begin();
            it != m_committable.end(); ++it)
    {
        LOG(INFO) << " committable " << it->first;
        it->second->debug_dump();
    }

    for (key_operation_list_t::iterator it = m_blocked.begin();
            it != m_blocked.end(); ++it)
    {
        LOG(INFO) << " blocked " << it->first;
        it->second->debug_dump();
    }

    for (key_operation_list_t::iterator it = m_deferred.begin();
            it != m_deferred.end(); ++it)
    {
        LOG(INFO) << " deferred " << it->first;
        it->second->debug_dump();
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
        assert(version < it->first);
        version = it->first;
    }

    for (key_operation_list_t::const_iterator it = m_blocked.begin();
            it != m_blocked.end(); ++it)
    {
        assert(version < it->first);
        version = it->first;
    }

    for (key_operation_list_t::const_iterator it = m_deferred.begin();
            it != m_deferred.end(); ++it)
    {
        assert(version < it->first);
        version = it->first;
    }

    assert(m_committable.empty() || m_old_version <= m_committable.back().first);
    assert(m_blocked.empty() || m_old_version < m_blocked.front().first);
    assert(m_deferred.empty() || m_old_version < m_deferred.front().first);
}

void
key_state :: get_latest(bool* has_old_value,
                        uint64_t* old_version,
                        std::vector<e::slice>** old_value)
{
    CHECK_INVARIANTS();
    *old_version = 0;

    if (!m_blocked.empty() && m_blocked.back().first > *old_version)
    {
        *has_old_value = m_blocked.back().second->has_value;
        *old_version = m_blocked.back().first;
        *old_value = &m_blocked.back().second->value;
    }

    if (!m_committable.empty() && m_committable.back().first > *old_version)
    {
        *has_old_value = m_committable.back().second->has_value;
        *old_version = m_committable.back().first;
        *old_value = &m_committable.back().second->value;
    }

    if (m_old_version > *old_version)
    {
        *has_old_value = m_has_old_value;
        *old_version = m_old_version;
        *old_value = &m_old_value;
    }
}

bool
key_state :: check_version(const schema& sc,
                           bool erase,
                           bool fail_if_not_found,
                           bool fail_if_found,
                           const std::vector<attribute_check>& checks,
                           bool has_old_value,
                           uint64_t,
                           std::vector<e::slice>* old_value,
                           network_returncode* nrc)
{
    *nrc = NET_CMPFAIL;

    if (!has_old_value && erase)
    {
        *nrc = NET_NOTFOUND;
        return false;
    }

    if (!has_old_value && fail_if_not_found)
    {
        *nrc = NET_NOTFOUND;
        return false;
    }

    if (has_old_value && fail_if_found)
    {
        return false;
    }

    if (checks.empty())
    {
        return true;
    }

    // we can assume !checks.empty() and so we must have an old value to work
    // with
    if (!has_old_value)
    {
        return false;
    }

    return passes_attribute_checks(sc, checks, m_key, *old_value) == checks.size();
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
    op->old_hashes.resize(sc.attrs_sz);
    op->new_hashes.resize(sc.attrs_sz);
    op->this_old_region = region_id();
    op->this_new_region = region_id();
    op->prev_region = region_id();
    op->next_region = region_id();
    subspace_id subspace_this = config->subspace_of(reg);
    subspace_id subspace_prev = config->subspace_prev(subspace_this);
    subspace_id subspace_next = config->subspace_next(subspace_this);

    if (has_old_value && has_new_value)
    {
        hyperdex::hash(sc, m_key, new_value, &op->new_hashes.front());
        hyperdex::hash(sc, m_key, old_value, &op->old_hashes.front());

        if (subspace_prev != subspace_id())
        {
            config->lookup_region(subspace_prev, op->new_hashes, &op->prev_region);
        }

        config->lookup_region(subspace_this, op->old_hashes, &op->this_old_region);
        config->lookup_region(subspace_this, op->new_hashes, &op->this_new_region);

        if (subspace_next != subspace_id())
        {
            config->lookup_region(subspace_next, op->old_hashes, &op->next_region);
        }
    }
    else if (has_old_value)
    {
        hyperdex::hash(sc, m_key, old_value, &op->old_hashes.front());

        for (size_t i = 0; i < sc.attrs_sz; ++i)
        {
            op->new_hashes[i] = op->old_hashes[i];
        }

        if (subspace_prev != subspace_id())
        {
            config->lookup_region(subspace_prev, op->old_hashes, &op->prev_region);
        }

        config->lookup_region(subspace_this, op->old_hashes, &op->this_old_region);
        op->this_new_region = op->this_old_region;

        if (subspace_next != subspace_id())
        {
            config->lookup_region(subspace_next, op->old_hashes, &op->next_region);
        }
    }
    else if (has_new_value)
    {
        hyperdex::hash(sc, m_key, new_value, &op->new_hashes.front());

        for (size_t i = 0; i < sc.attrs_sz; ++i)
        {
            op->old_hashes[i] = op->new_hashes[i];
        }

        if (subspace_prev != subspace_id())
        {
            config->lookup_region(subspace_prev, op->old_hashes, &op->prev_region);
        }

        config->lookup_region(subspace_this, op->old_hashes, &op->this_new_region);
        op->this_old_region = op->this_new_region;

        if (subspace_next != subspace_id())
        {
            config->lookup_region(subspace_next, op->old_hashes, &op->next_region);
        }
    }
    else
    {
        abort();
    }
}
