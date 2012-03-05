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

#ifndef hyperdaemon_replication_manager_keyholder
#define hyperdaemon_replication_manager_keyholder

class hyperdaemon::replication_manager::keyholder
{
    public:
        keyholder();
        ~keyholder() throw ();

    public:
        bool empty() const;
        bool has_committable_ops() const;
        bool has_blocked_ops() const;
        bool has_deferred_ops() const;
        e::intrusive_ptr<pending> get_by_version(uint64_t version) const;
        uint64_t most_recent_blocked_version() const;
        e::intrusive_ptr<pending> most_recent_blocked_op() const;
        uint64_t most_recent_committable_version() const;
        e::intrusive_ptr<pending> most_recent_committable_op() const;
        e::intrusive_ptr<pending> oldest_committable_op() const;
        uint64_t oldest_committable_version() const;
        uint64_t oldest_blocked_version() const;
        e::intrusive_ptr<pending> oldest_blocked_op() const;
        uint64_t oldest_deferred_version() const;
        e::intrusive_ptr<deferred> oldest_deferred_op() const;
        uint64_t version_on_disk() const { return m_version_on_disk; }

    public:
        void append_blocked(uint64_t version, e::intrusive_ptr<pending> op);
        void insert_deferred(uint64_t version, e::intrusive_ptr<deferred> op);
        void remove_oldest_committable_op();
        void remove_oldest_deferred_op();
        void set_version_on_disk(uint64_t version);
        void transfer_blocked_to_committable(); // Just transfers 1

    private:
        typedef std::deque<std::pair<uint64_t, e::intrusive_ptr<pending> > >
                committable_list_t;
        typedef std::deque<std::pair<uint64_t, e::intrusive_ptr<pending> > >
                blocked_list_t;
        typedef std::deque<std::pair<uint64_t, e::intrusive_ptr<deferred> > >
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
        uint64_t m_version_on_disk;
};

hyperdaemon :: replication_manager :: keyholder :: keyholder()
    : m_ref(0)
    , m_committable()
    , m_blocked()
    , m_deferred()
    , m_version_on_disk()
{
}

hyperdaemon :: replication_manager :: keyholder :: ~keyholder()
                                                   throw ()
{
}

bool
hyperdaemon :: replication_manager :: keyholder :: empty()
                                                   const
{
    return m_committable.empty() && m_blocked.empty() && m_deferred.empty();
}

bool
hyperdaemon :: replication_manager :: keyholder :: has_committable_ops()
                                                   const
{
    return !m_committable.empty();
}

bool
hyperdaemon :: replication_manager :: keyholder :: has_blocked_ops()
                                                   const
{
    return !m_blocked.empty();
}

bool
hyperdaemon :: replication_manager :: keyholder :: has_deferred_ops()
                                                   const
{
    return !m_deferred.empty();
}

e::intrusive_ptr<hyperdaemon::replication_manager::pending>
hyperdaemon :: replication_manager :: keyholder :: get_by_version(uint64_t version)
                                                   const
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
hyperdaemon :: replication_manager :: keyholder :: most_recent_blocked_version()
                                                   const
{
    assert(!m_blocked.empty());
    return m_blocked.back().first;
}

e::intrusive_ptr<hyperdaemon::replication_manager::pending>
hyperdaemon :: replication_manager :: keyholder :: most_recent_blocked_op()
                                                   const
{
    assert(!m_blocked.empty());
    return m_blocked.back().second;
}

uint64_t
hyperdaemon :: replication_manager :: keyholder :: most_recent_committable_version()
                                                   const
{
    assert(!m_committable.empty());
    return m_committable.back().first;
}

e::intrusive_ptr<hyperdaemon::replication_manager::pending>
hyperdaemon :: replication_manager :: keyholder :: most_recent_committable_op()
                                                   const
{
    assert(!m_committable.empty());
    return m_committable.back().second;
}

e::intrusive_ptr<hyperdaemon::replication_manager::pending>
hyperdaemon :: replication_manager :: keyholder :: oldest_committable_op() const
{
    assert(!m_committable.empty());
    return m_committable.front().second;
}

uint64_t
hyperdaemon :: replication_manager :: keyholder :: oldest_committable_version() const
{
    assert(!m_committable.empty());
    return m_committable.front().first;
}

uint64_t
hyperdaemon :: replication_manager :: keyholder :: oldest_blocked_version() const
{
    assert(!m_blocked.empty());
    return m_blocked.front().first;
}

e::intrusive_ptr<hyperdaemon::replication_manager::pending>
hyperdaemon :: replication_manager :: keyholder :: oldest_blocked_op() const
{
    assert(!m_blocked.empty());
    return m_blocked.front().second;
}

uint64_t
hyperdaemon :: replication_manager :: keyholder :: oldest_deferred_version() const
{
    assert(!m_deferred.empty());
    return m_deferred.front().first;
}

e::intrusive_ptr<hyperdaemon::replication_manager::deferred>
hyperdaemon :: replication_manager :: keyholder :: oldest_deferred_op() const
{
    assert(!m_deferred.empty());
    return m_deferred.front().second;
}

void
hyperdaemon :: replication_manager :: keyholder :: append_blocked(uint64_t version,
                                                                  e::intrusive_ptr<pending> op)
{
    m_blocked.push_back(std::make_pair(version, op));
}

void
hyperdaemon :: replication_manager :: keyholder :: insert_deferred(uint64_t version,
                                                                   e::intrusive_ptr<deferred> op)
{
    deferred_list_t::iterator d = m_deferred.begin();

    while (d != m_deferred.end() && d->first <= version)
    {
        ++d;
    }

    m_deferred.insert(d, std::make_pair(version, op));
}

void
hyperdaemon :: replication_manager :: keyholder :: remove_oldest_committable_op()
{
    assert(!m_committable.empty());
    m_committable.pop_front();
}

void
hyperdaemon :: replication_manager :: keyholder :: remove_oldest_deferred_op()
{
    assert(!m_deferred.empty());
    m_deferred.pop_front();
}

void
hyperdaemon :: replication_manager :: keyholder :: set_version_on_disk(uint64_t version)
{
    assert(m_version_on_disk < version);
    m_version_on_disk = version;
}

void
hyperdaemon :: replication_manager :: keyholder :: transfer_blocked_to_committable()
{
    assert(!m_blocked.empty());
    m_committable.push_back(m_blocked.front());
    m_blocked.pop_front();
}

#endif // hyperdaemon_replication_manager_keyholder
