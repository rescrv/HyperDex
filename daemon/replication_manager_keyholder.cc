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

// HyperDex
#include "daemon/replication_manager_keyholder.h"
#include "daemon/replication_manager_pending.h"

using hyperdex::replication_manager;

replication_manager :: keyholder :: keyholder()
    : m_ref(0)
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

replication_manager :: keyholder :: ~keyholder() throw ()
{
}

bool
replication_manager :: keyholder :: empty() const
{
    return m_committable.empty() && m_blocked.empty() && m_deferred.empty();
}

void
replication_manager :: keyholder :: get_latest_version(bool* has_old_value,
                                                       uint64_t* old_version,
                                                       std::vector<e::slice>** old_value)
{
    *has_old_value = false;

    if (has_blocked_ops())
    {
        *has_old_value = true;
        *old_version = most_recent_blocked_version();
        *old_value = &most_recent_blocked_op()->value;
    }
    else if (has_committable_ops())
    {
        *has_old_value = true;
        *old_version = most_recent_committable_version();
        *old_value = &most_recent_committable_op()->value;
    }
    else
    {
        *has_old_value = m_has_old_value;
        *old_version = m_old_version;
        *old_value = &m_old_value;
    }
}

e::intrusive_ptr<replication_manager::pending>
replication_manager :: keyholder :: get_by_version(uint64_t version) const
{
    if (!m_committable.empty() && m_committable.back().first >= version)
    {
        for (committable_list_t::const_iterator c = m_committable.begin();
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
        for (blocked_list_t::const_iterator b = m_blocked.begin();
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

uint64_t
replication_manager :: keyholder :: version_on_disk() const
{
    return m_old_version;
}

bool
replication_manager :: keyholder :: has_committable_ops() const
{
    return !m_committable.empty();
}

uint64_t
replication_manager :: keyholder :: most_recent_committable_version() const
{
    assert(!m_committable.empty());
    return m_committable.back().first;
}

e::intrusive_ptr<replication_manager::pending>
replication_manager :: keyholder :: most_recent_committable_op() const
{
    assert(!m_committable.empty());
    return m_committable.back().second;
}

bool
replication_manager :: keyholder :: has_blocked_ops()
                                                   const
{
    return !m_blocked.empty();
}

uint64_t
replication_manager :: keyholder :: oldest_blocked_version() const
{
    assert(!m_blocked.empty());
    return m_blocked.front().first;
}

e::intrusive_ptr<replication_manager::pending>
replication_manager :: keyholder :: oldest_blocked_op() const
{
    assert(!m_blocked.empty());
    return m_blocked.front().second;
}

uint64_t
replication_manager :: keyholder :: most_recent_blocked_version() const
{
    assert(!m_blocked.empty());
    return m_blocked.back().first;
}

e::intrusive_ptr<replication_manager::pending>
replication_manager :: keyholder :: most_recent_blocked_op() const
{
    assert(!m_blocked.empty());
    return m_blocked.back().second;
}

bool
replication_manager :: keyholder :: has_deferred_ops() const
{
    return !m_deferred.empty();
}

uint64_t
replication_manager :: keyholder :: oldest_deferred_version() const
{
    assert(!m_deferred.empty());
    return m_deferred.front().first;
}

e::intrusive_ptr<replication_manager::pending>
replication_manager :: keyholder :: oldest_deferred_op() const
{
    assert(!m_deferred.empty());
    return m_deferred.front().second;
}

void
replication_manager :: keyholder :: clear_committable_acked()
{
    while (!m_committable.empty() && m_committable.front().second->acked)
    {
        assert(m_committable.front().first <= m_old_version);
        m_committable.pop_front();
    }
}

void
replication_manager :: keyholder :: clear_deferred()
{
    m_deferred.clear();
}

void
replication_manager :: keyholder :: set_version_on_disk(uint64_t version)
{
    assert(m_old_version < version);
    e::intrusive_ptr<pending> op = get_by_version(version);
    assert(op);
    m_has_old_value = op->has_value;
    m_old_version = version;
    m_old_value = op->value;
    m_old_backing = op->backing;
}

void
replication_manager :: keyholder :: insert_blocked(uint64_t version,
                                                    e::intrusive_ptr<pending> op)
{
    assert(m_blocked.empty() || m_blocked.back().first < version);
    m_blocked.push_back(std::make_pair(version, op));
}

void
replication_manager :: keyholder :: insert_deferred(uint64_t version,
                                                    e::intrusive_ptr<pending> op)
{
    deferred_list_t::iterator d = m_deferred.begin();

    while (d != m_deferred.end() && d->first <= version)
    {
        ++d;
    }

    m_deferred.insert(d, std::make_pair(version, op));
}

void
replication_manager :: keyholder :: shift_one_blocked_to_committable()
{
    assert(!m_blocked.empty());
    m_committable.push_back(m_blocked.front());
    m_blocked.pop_front();
}

void
replication_manager :: keyholder :: shift_one_deferred_to_blocked()
{
    assert(!m_blocked.empty());
    m_committable.push_back(m_blocked.front());
    m_blocked.pop_front();
}

void
replication_manager :: keyholder :: resend_committable(replication_manager* rm,
                                                       const virtual_server_id& us,
                                                       const e::slice& key)
{
    for (committable_list_t::iterator it = m_committable.begin();
            it != m_committable.end(); ++it)
    {
        it->second->sent = virtual_server_id();
        it->second->sent_config_version = 0;
        rm->send_message(us, true, it->first, key, it->second);
    }
}
