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

// Google Log
#include <glog/logging.h>

// HyperDex
#include "common/hash.h"
#include "daemon/daemon.h"
#include "daemon/replication_manager_key_region.h"
#include "daemon/replication_manager_key_state.h"
#include "daemon/replication_manager_pending.h"

using hyperdex::replication_manager;

replication_manager :: key_state :: key_state(const region_id& r, const e::slice& k)
    : m_ri(r)
    , m_key_backing(reinterpret_cast<const char*>(k.data()), k.size())
    , m_key(m_key_backing.data(), m_key_backing.size())
    , m_lock()
    , m_marked_garbage(false)
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

replication_manager :: key_state :: ~key_state() throw ()
{
}

e::slice
replication_manager :: key_state :: key() const
{
    return m_key;
}

replication_manager::key_region
replication_manager :: key_state :: kr() const
{
    return key_region(m_ri, m_key);
}

bool
replication_manager :: key_state :: marked_garbage() const
{
    return m_marked_garbage;
}

bool
replication_manager :: key_state :: empty() const
{
    return m_committable.empty() && m_blocked.empty() && m_deferred.empty();
}

uint64_t
replication_manager :: key_state :: max_seq_id() const
{
    uint64_t ret = 0;

    if (!m_committable.empty())
    {
        ret = std::max(ret, m_committable.back().second->seq_id);
    }

    if (!m_blocked.empty())
    {
        ret = std::max(ret, m_blocked.back().second->seq_id);
    }

    if (!m_deferred.empty())
    {
        ret = std::max(ret, m_deferred.back().second->seq_id);
    }

    return ret;
}

uint64_t
replication_manager :: key_state :: min_seq_id() const
{
    uint64_t ret = 0;

    if (!m_committable.empty())
    {
        ret = std::min(ret, m_committable.front().second->seq_id);
    }

    if (!m_blocked.empty())
    {
        ret = std::min(ret, m_blocked.front().second->seq_id);
    }

    if (!m_deferred.empty())
    {
        ret = std::min(ret, m_deferred.front().second->seq_id);
    }

    return ret;
}

e::intrusive_ptr<replication_manager::pending>
replication_manager :: key_state :: get_version(uint64_t version) const
{
    if (!m_committable.empty() && m_committable.back().first >= version)
    {
        for (pending_list_t::const_iterator c = m_committable.begin();
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
        for (pending_list_t::const_iterator b = m_blocked.begin();
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
replication_manager :: key_state :: initialize(datalayer* data,
                                               const region_id& ri)
{
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

    return rc;
}

bool
replication_manager :: key_state :: check_against_latest_version(const schema& sc,
                                                                 bool erase,
                                                                 bool fail_if_not_found,
                                                                 bool fail_if_found,
                                                                 const std::vector<attribute_check>& checks,
                                                                 network_returncode* nrc)
{
    bool has_old_value = false;
    uint64_t old_version = 0;
    std::vector<e::slice>* old_value = NULL;
    get_latest(&has_old_value, &old_version, &old_value);
    return check_version(sc, erase, fail_if_not_found, fail_if_found, checks,
                         has_old_value, old_version, old_value, nrc);
}

void
replication_manager :: key_state :: delete_latest(const schema& sc,
                                                  const region_id& reg_id, uint64_t seq_id,
                                                  const server_id& client, uint64_t nonce)
{
    assert(sc.attrs_sz > 0);
    e::intrusive_ptr<pending> op;
    op = new pending(std::auto_ptr<e::buffer>(),
                     reg_id, seq_id, false,
                     false, std::vector<e::slice>(sc.attrs_sz - 1),
                     client, nonce,
                     0, virtual_server_id());

    uint64_t new_version = 0;

    if (!m_blocked.empty())
    {
        new_version = m_blocked.back().first + 1;
    }
    else if (!m_committable.empty())
    {
        new_version = m_committable.back().first + 1;
    }
    else
    {
        new_version = m_old_version + 1;
    }

    insert_deferred(new_version, op);
}

bool
replication_manager :: key_state :: put_from_funcs(const schema& sc,
                                                   const region_id& reg_id, uint64_t seq_id,
                                                   const std::vector<funcall>& funcs,
                                                   const server_id& client, uint64_t nonce)
{
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
    e::intrusive_ptr<pending> op;
    op = new pending(backing,
                     reg_id, seq_id, !has_old_value,
                     true, new_value,
                     client, nonce,
                     0, virtual_server_id());

    if (funcs_passed == funcs.size())
    {
        insert_deferred(old_version + 1, op);
        return true;
    }

    return false;
}

void
replication_manager :: key_state :: insert_deferred(uint64_t version, e::intrusive_ptr<pending> op)
{
    pending_list_t::iterator d = m_deferred.begin();

    while (d != m_deferred.end() && d->first <= version)
    {
        ++d;
    }

    assert(d == m_deferred.end() || d->first != version);
    m_deferred.insert(d, std::make_pair(version, op));
}

bool
replication_manager :: key_state :: persist_to_datalayer(replication_manager* rm,
                                                         const region_id& ri,
                                                         const region_id& reg_id,
                                                         uint64_t seq_id,
                                                         uint64_t version)
{
    if (m_old_version < version)
    {
        e::intrusive_ptr<pending> op = get_version(version);
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
    }
    else
    {
        rm->m_daemon->m_data.mark_acked(ri, reg_id, seq_id);
    }

    return true;
}

void
replication_manager :: key_state :: clear_deferred()
{
    m_deferred.clear();
}

void
replication_manager :: key_state :: clear_acked_prefix()
{
    while (!m_committable.empty() && m_committable.front().second->acked)
    {
        assert(m_committable.front().first <= m_old_version);
        m_committable.pop_front();
    }
}

void
replication_manager :: key_state :: resend_committable(replication_manager* rm,
                                                       const virtual_server_id& us)
{
    for (pending_list_t::iterator it = m_committable.begin();
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
}

void
replication_manager :: key_state :: move_operations_between_queues(replication_manager* rm,
                                                                   const virtual_server_id& us,
                                                                   const region_id& ri,
                                                                   const schema& sc)
{
    // Apply deferred operations
    while (!m_deferred.empty())
    {
        bool has_old_value = false;
        uint64_t old_version = 0;
        std::vector<e::slice>* old_value = NULL;
        get_latest(&has_old_value, &old_version, &old_value);
        assert(old_version < m_deferred.front().first);
        e::intrusive_ptr<pending> op = m_deferred.front().second;

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
        uint64_t version = m_blocked.front().first;
        e::intrusive_ptr<pending> op = m_blocked.front().second;

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
}

void
replication_manager :: key_state :: get_latest(bool* has_old_value,
                                               uint64_t* old_version,
                                               std::vector<e::slice>** old_value)
{
    if (!m_blocked.empty())
    {
        *has_old_value = m_blocked.back().second->has_value;
        *old_version = m_blocked.back().first;
        *old_value = &m_blocked.back().second->value;
    }
    else if (!m_committable.empty())
    {
        *has_old_value = m_committable.back().second->has_value;
        *old_version = m_committable.back().first;
        *old_value = &m_committable.back().second->value;
    }
    else
    {
        *has_old_value = m_has_old_value;
        *old_version = m_old_version;
        *old_value = &m_old_value;
    }
}

bool
replication_manager :: key_state :: check_version(const schema& sc, 
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
replication_manager :: key_state :: hash_objects(const configuration* config,
                                                 const region_id& reg,
                                                 const schema& sc,
                                                 bool has_new_value,
                                                 const std::vector<e::slice>& new_value,
                                                 bool has_old_value,
                                                 const std::vector<e::slice>& old_value,
                                                 e::intrusive_ptr<pending> pend)
{
    pend->old_hashes.resize(sc.attrs_sz);
    pend->new_hashes.resize(sc.attrs_sz);
    pend->this_old_region = region_id();
    pend->this_new_region = region_id();
    pend->prev_region = region_id();
    pend->next_region = region_id();
    subspace_id subspace_this = config->subspace_of(reg);
    subspace_id subspace_prev = config->subspace_prev(subspace_this);
    subspace_id subspace_next = config->subspace_next(subspace_this);

    if (has_old_value && has_new_value)
    {
        hyperdex::hash(sc, m_key, new_value, &pend->new_hashes.front());
        hyperdex::hash(sc, m_key, old_value, &pend->old_hashes.front());

        if (subspace_prev != subspace_id())
        {
            config->lookup_region(subspace_prev, pend->new_hashes, &pend->prev_region);
        }

        config->lookup_region(subspace_this, pend->old_hashes, &pend->this_old_region);
        config->lookup_region(subspace_this, pend->new_hashes, &pend->this_new_region);

        if (subspace_next != subspace_id())
        {
            config->lookup_region(subspace_next, pend->old_hashes, &pend->next_region);
        }
    }
    else if (has_old_value)
    {
        hyperdex::hash(sc, m_key, old_value, &pend->old_hashes.front());

        for (size_t i = 0; i < sc.attrs_sz; ++i)
        {
            pend->new_hashes[i] = pend->old_hashes[i];
        }

        if (subspace_prev != subspace_id())
        {
            config->lookup_region(subspace_prev, pend->old_hashes, &pend->prev_region);
        }

        config->lookup_region(subspace_this, pend->old_hashes, &pend->this_old_region);
        pend->this_new_region = pend->this_old_region;

        if (subspace_next != subspace_id())
        {
            config->lookup_region(subspace_next, pend->old_hashes, &pend->next_region);
        }
    }
    else if (has_new_value)
    {
        hyperdex::hash(sc, m_key, new_value, &pend->new_hashes.front());

        for (size_t i = 0; i < sc.attrs_sz; ++i)
        {
            pend->old_hashes[i] = pend->new_hashes[i];
        }

        if (subspace_prev != subspace_id())
        {
            config->lookup_region(subspace_prev, pend->old_hashes, &pend->prev_region);
        }

        config->lookup_region(subspace_this, pend->old_hashes, &pend->this_new_region);
        pend->this_old_region = pend->this_new_region;

        if (subspace_next != subspace_id())
        {
            config->lookup_region(subspace_next, pend->old_hashes, &pend->next_region);
        }
    }
    else
    {
        abort();
    }
}
