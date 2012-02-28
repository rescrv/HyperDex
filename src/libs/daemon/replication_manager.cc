// Copyright (c) 2011, Cornell University
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

// C
#include <stdint.h>

// STL
#include <queue>
#include <tr1/functional>
#include <utility>

// Google CityHash
#include <city.h>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/guard.h>
#include <e/intrusive_ptr.h>
#include <e/timer.h>

// HyperDex
#include "hyperdex/coordinatorlink.h"
#include "hyperdex/network_constants.h"
#include "hyperdex/packing.h"

// HyperDaemon
#include "datalayer.h"
#include "logical.h"
#include "ongoing_state_transfers.h"
#include "replication_manager.h"
#include "runtimeconfig.h"

using hyperspacehashing::prefix::coordinate;
using hyperdex::configuration;
using hyperdex::coordinatorlink;
using hyperdex::entityid;
using hyperdex::instance;
using hyperdex::network_msgtype;
using hyperdex::network_returncode;
using hyperdex::regionid;
using hyperdaemon::replication::clientop;
using hyperdaemon::replication::keypair;

#define _CONCAT(x, y) x ## y
#define CONCAT(x, y) _CONCAT(x, y)

// This macro should be used in the body of non-static members to hold the
// appropriate lock for the request.  E should be an entity whose region the key
// resides in.  K is the key for the object being protected.
#define HOLD_LOCK_FOR_KEY(E, K) \
    e::striped_lock<po6::threads::mutex>::hold CONCAT(_anon, __LINE__)(&m_locks, get_lock_num(E.get_region(), K))

//////////////////////////////// Deferred Class ////////////////////////////////

class hyperdaemon::replication_manager::deferred
{
    public:
        deferred(const bool has_value,
                 std::auto_ptr<e::buffer> backing,
                 const e::slice& key,
                 const std::vector<e::slice>& value,
                 const hyperdex::entityid& from_ent,
                 const hyperdex::instance& from_inst,
                 const hyperdisk::reference& ref);
        ~deferred() throw ();

    public:
        std::tr1::shared_ptr<e::buffer> backing;
        const bool has_value;
        const e::slice key;
        const std::vector<e::slice> value;
        const hyperdex::entityid from_ent;
        const hyperdex::instance from_inst;
        hyperdisk::reference ref;

    private:
        friend class e::intrusive_ptr<deferred>;

    private:
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }

    private:
        size_t m_ref;
};

hyperdaemon :: replication_manager :: deferred :: deferred(const bool hv,
                                                           std::auto_ptr<e::buffer> b,
                                                           const e::slice& k,
                                                           const std::vector<e::slice>& val,
                                                           const hyperdex::entityid& e,
                                                           const hyperdex::instance& i,
                                                           const hyperdisk::reference& r)
    : backing(b.release())
    , has_value(hv)
    , key(k)
    , value(val)
    , from_ent(e)
    , from_inst(i)
    , ref(r)
    , m_ref(0)
{
}

hyperdaemon :: replication_manager :: deferred :: ~deferred()
                                                  throw ()
{
}

///////////////////////////////// Pending Class ////////////////////////////////

class hyperdaemon::replication_manager::pending
{
    public:
        pending(bool has_value,
                std::tr1::shared_ptr<e::buffer> backing,
                const e::slice& key,
                const std::vector<e::slice>& value,
                const clientop& co = clientop());
        ~pending() throw ();

    public:
        std::tr1::shared_ptr<e::buffer> backing;
        e::slice key;
        std::vector<e::slice> value;
        const bool has_value;
        bool fresh;
        bool acked;
        uint8_t retransmit;
        clientop co;
        hyperdex::network_msgtype retcode;
        e::intrusive_ptr<pending> backing2;
        hyperdisk::reference ref;

        hyperdex::entityid recv; // We recv from here
        hyperdex::entityid sent; // We sent to here
        uint16_t subspace_prev;
        uint16_t subspace_next;
        uint64_t point_prev;
        uint64_t point_this;
        uint64_t point_next;
        uint64_t point_next_next;

    private:
        friend class e::intrusive_ptr<pending>;

    private:
        void inc() { ++m_ref; }
        void dec() { if (--m_ref == 0) delete this; }

    private:
        size_t m_ref;
};

hyperdaemon :: replication_manager :: pending :: pending(bool hv,
                                                         std::tr1::shared_ptr<e::buffer> b,
                                                         const e::slice& k,
                                                         const std::vector<e::slice>& val,
                                                         const clientop& c)
    : backing(b)
    , key(k)
    , value(val)
    , has_value(hv)
    , fresh(false)
    , acked(false)
    , retransmit(0)
    , co(c)
    , retcode()
    , backing2()
    , ref()
    , recv()
    , sent()
    , subspace_prev(0)
    , subspace_next(0)
    , point_prev(0)
    , point_this(0)
    , point_next(0)
    , point_next_next(0)
    , m_ref(0)
{
}

hyperdaemon :: replication_manager :: pending :: ~pending()
                                                 throw ()
{
}

//////////////////////////////// Keyholder Class ///////////////////////////////

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

///////////////////////////////// Public Class /////////////////////////////////

hyperdaemon :: replication_manager :: replication_manager(coordinatorlink* cl,
                                                          datalayer* data,
                                                          logical* comm,
                                                          ongoing_state_transfers* ost)
    : m_cl(cl)
    , m_data(data)
    , m_comm(comm)
    , m_ost(ost)
    , m_config()
    , m_locks(LOCK_STRIPING)
    , m_keyholders(REPLICATION_HASHTABLE_SIZE)
    , m_us()
    , m_shutdown(false)
    , m_periodic_thread(std::tr1::bind(&replication_manager::periodic, this))
{
    m_periodic_thread.start();
}

hyperdaemon :: replication_manager :: ~replication_manager() throw ()
{
    if (!m_shutdown)
    {
        shutdown();
    }

    m_periodic_thread.join();
}

void
hyperdaemon :: replication_manager :: prepare(const configuration&, const instance&)
{
    // Do nothing.
}

void
hyperdaemon :: replication_manager :: reconfigure(const configuration& newconfig, const instance& us)
{
    // Install a new configuration.
    configuration oldconfig(m_config);
    m_config = newconfig;
    m_us = us;

    for (keyholder_map_t::iterator khiter = m_keyholders.begin();
            khiter != m_keyholders.begin(); khiter.next())
    {
        // Drop the keyholder if we don't need it.
        if (!m_config.in_region(us, khiter.key().region))
        {
            m_keyholders.remove(khiter.key());
            continue;
        }
    }
}

void
hyperdaemon :: replication_manager :: cleanup(const configuration&, const instance&)
{
    // Do nothing.
}

void
hyperdaemon :: replication_manager :: shutdown()
{
    m_shutdown = true;
}

void
hyperdaemon :: replication_manager :: client_put(const hyperdex::entityid& from,
                                                 const hyperdex::entityid& to,
                                                 uint64_t nonce,
                                                 std::auto_ptr<e::buffer> backing,
                                                 const e::slice& key,
                                                 const std::vector<std::pair<uint16_t, e::slice> >& value)
{
    size_t dims = m_config.dimensions(to.get_space());
    assert(dims > 0);
    e::bitfield bf(dims - 1);
    std::vector<e::slice> realvalue(dims - 1);

    for (size_t i = 0; i < value.size(); ++i)
    {
        if (value[i].first == 0 || value[i].first == dims)
        {
            respond_to_client(to, from, nonce,
                              hyperdex::RESP_PUT,
                              hyperdex::NET_WRONGARITY);
            return;
        }

        realvalue[value[i].first - 1] = value[i].second;
        bf.set(value[i].first - 1);
    }

    client_common(true, from, to, nonce, backing, key, bf, realvalue);
}

void
hyperdaemon :: replication_manager :: client_del(const entityid& from,
                                                 const entityid& to,
                                                 uint64_t nonce,
                                                 std::auto_ptr<e::buffer> backing,
                                                 const e::slice& key)
{
    size_t dims = m_config.dimensions(to.get_space());
    assert(dims > 0);
    e::bitfield b(dims - 1);
    std::vector<e::slice> v(dims - 1);
    client_common(false, from, to, nonce, backing, key, b, v);
}

void
hyperdaemon :: replication_manager :: chain_put(const entityid& from,
                                                const entityid& to,
                                                uint64_t newversion,
                                                bool fresh,
                                                std::auto_ptr<e::buffer> backing,
                                                const e::slice& key,
                                                const std::vector<e::slice>& newvalue)
{
    chain_common(true, from, to, newversion, fresh, backing, key, newvalue);
}

void
hyperdaemon :: replication_manager :: chain_del(const entityid& from,
                                                const entityid& to,
                                                uint64_t newversion,
                                                std::auto_ptr<e::buffer> backing,
                                                const e::slice& key)
{
    chain_common(false, from, to, newversion, false, backing, key, std::vector<e::slice>());
}

void
hyperdaemon :: replication_manager :: chain_subspace(const entityid& from,
                                                     const entityid& to,
                                                     uint64_t version,
                                                     std::auto_ptr<e::buffer> backing,
                                                     const e::slice& key,
                                                     const std::vector<e::slice>& value,
                                                     uint64_t nextpoint)
{
    // Grab the lock that protects this key.
    HOLD_LOCK_FOR_KEY(to, key);
    // Get the keyholder for this key.
    e::intrusive_ptr<keyholder> kh = get_keyholder(to.get_region(), key);

    // Check that a chain's put matches the dimensions of the space.
    if (m_config.dimensions(to.get_space()) != value.size() + 1)
    {
        return;
    }

    // Find the pending or committed version with the largest number.
    uint64_t oldversion = 0;
    bool has_oldvalue = false;
    std::vector<e::slice> oldvalue;
    hyperdisk::reference ref;

    if (kh->has_blocked_ops())
    {
        oldversion = kh->most_recent_blocked_version();
        has_oldvalue = kh->most_recent_blocked_op()->has_value;
        oldvalue = kh->most_recent_blocked_op()->value;
    }
    else if (kh->has_committable_ops())
    {
        oldversion = kh->most_recent_committable_version();
        has_oldvalue = kh->most_recent_committable_op()->has_value;
        oldvalue = kh->most_recent_committable_op()->value;
    }
    else if (!from_disk(to.get_region(), key, &has_oldvalue, &oldvalue, &oldversion, &ref))
    {
        return;
    }

    if (oldversion >= version)
    {
        send_ack(to, from, version, key);
        return;
    }

    // Figure out how many subspaces (in total) there are.
    size_t subspaces = m_config.subspaces(to.get_space());
    assert(subspaces > 0);

    // Create a new pending object to set as pending.
    e::intrusive_ptr<pending> newpend;
    std::tr1::shared_ptr<e::buffer> sharedbacking(backing.release());
    newpend = new pending(true, sharedbacking, key, value);
    newpend->recv = from;
    newpend->subspace_prev = to.subspace;
    newpend->subspace_next = to.subspace < subspaces - 1 ? to.subspace + 1 : UINT16_MAX;
    newpend->point_prev = from.mask;
    hyperspacehashing::prefix::hasher hasher_this = m_config.repl_hasher(to.get_subspace());
    newpend->point_this = hasher_this.hash(key, value).point;
    newpend->point_next = nextpoint;

    if (from.get_subspace() != to.get_subspace()
            || (!(from.get_region() == to.get_region()
                    && m_config.chain_adjacent(from, to))
                && !(from.get_region() != to.get_region()
                    && m_config.is_tail(from)
                    && m_config.is_head(to))))
    {
        LOG(INFO) << "dropping CHAIN_SUBSPACE message which didn't come from the right host.";
        return;
    }

    if (!to.coord().contains(coordinate(64, newpend->point_this)))
    {
        LOG(INFO) << "dropping CHAIN_SUBSPACE message which didn't come to the right host.";
        return;
    }

    kh->append_blocked(version, newpend);
    move_operations_between_queues(to, key, kh);
}

void
hyperdaemon :: replication_manager :: chain_ack(const entityid& from,
                                                const entityid& to,
                                                uint64_t version,
                                                std::auto_ptr<e::buffer>,
                                                const e::slice& key)
{
    // Grab the lock that protects this key.
    HOLD_LOCK_FOR_KEY(to, key);
    // Get the keyholder for this key.
    e::intrusive_ptr<keyholder> kh = get_keyholder(to.get_region(), key);
    // Get the state for this operation.
    e::intrusive_ptr<pending> pend = kh->get_by_version(version);

    if (!pend)
    {
        LOG(INFO) << "dropping CHAIN_ACK for update we haven't seen";
        return;
    }

    if (pend->sent == entityid())
    {
        LOG(INFO) << "dropping CHAIN_ACK for update we haven't sent";
        return;
    }

    if (from != pend->sent)
    {
        LOG(INFO) << "dropping CHAIN_ACK that came from the wrong host";
        return;
    }

    m_ost->add_trigger(to.get_region(), key, version);
    pend->acked = true;
    put_to_disk(to.get_region(), kh, version);

    while (kh->has_committable_ops()
            && kh->oldest_committable_op()->acked)
    {
        kh->remove_oldest_committable_op();
    }

    if (m_config.is_point_leader(to))
    {
        move_operations_between_queues(to, key, kh);

        if (pend->co.from.space == UINT32_MAX)
        {
            respond_to_client(to, pend->co.from, pend->co.nonce,
                              pend->retcode, hyperdex::NET_SUCCESS);
            pend->co = clientop();
        }
    }
    else
    {
        send_ack(to, pend->recv, version, key);
    }

    if (kh->empty())
    {
        erase_keyholder(to.get_region(), key);
    }
}

void
hyperdaemon :: replication_manager :: client_common(bool has_value,
                                                    const entityid& from,
                                                    const entityid& to,
                                                    uint64_t nonce,
                                                    std::auto_ptr<e::buffer> backing,
                                                    const e::slice& key,
                                                    const e::bitfield& value_mask,
                                                    const std::vector<e::slice>& value)
{
    // Make sure this message is from a client.
    if (from.space != UINT32_MAX)
    {
        LOG(INFO) << "dropping client-only message from " << from << " (it is not a client).";
        return;
    }

    clientop co(to.get_region(), from, nonce);
    hyperdex::network_msgtype retcode = has_value ?
        hyperdex::RESP_PUT : hyperdex::RESP_DEL;

    // Make sure this message is to the point-leader.
    if (!m_config.is_point_leader(to))
    {
        respond_to_client(to, from, nonce, retcode, hyperdex::NET_NOTUS);
        return;
    }

    // Automatically respond with "SERVERERROR" whenever we return without g.dismiss()
    e::guard g = e::makeobjguard(*this, &replication_manager::respond_to_client, to, from, nonce, retcode, hyperdex::NET_SERVERERROR);

    // Grab the lock that protects this key.
    HOLD_LOCK_FOR_KEY(to, key);
    // Get the keyholder for this key.
    e::intrusive_ptr<keyholder> kh = get_keyholder(to.get_region(), key);

    // Find the pending or committed version with the largest number.
    uint64_t oldversion = 0;
    bool has_oldvalue = false;
    std::vector<e::slice> oldvalue;
    hyperdisk::reference ref;

    if (kh->has_blocked_ops())
    {
        oldversion = kh->most_recent_blocked_version();
        has_oldvalue = kh->most_recent_blocked_op()->has_value;
        oldvalue = kh->most_recent_blocked_op()->value;
    }
    else if (kh->has_committable_ops())
    {
        oldversion = kh->most_recent_committable_version();
        has_oldvalue = kh->most_recent_committable_op()->has_value;
        oldvalue = kh->most_recent_committable_op()->value;
    }
    else if (!from_disk(to.get_region(), key, &has_oldvalue, &oldvalue, &oldversion, &ref))
    {
        return;
    }

    e::intrusive_ptr<pending> newpend;
    std::tr1::shared_ptr<e::buffer> sharedbacking(backing.release());
    newpend = new pending(has_value, sharedbacking, key, value, co);
    newpend->retcode = retcode;
    newpend->ref = ref;

    if (!has_value && !has_oldvalue)
    {
        respond_to_client(to, from, nonce, retcode, hyperdex::NET_SUCCESS);
        g.dismiss();
        return;
    }

    if (has_value && !has_oldvalue)
    {
        newpend->fresh = true;
    }

    std::tr1::shared_ptr<e::buffer> old_backing = newpend->backing;

    if (has_value && has_oldvalue)
    {
        size_t need_moar = 0;

        for (size_t i = 0; i < value.size(); ++i)
        {
            if (!value_mask.get(i))
            {
                need_moar += oldvalue[i].size();
            }
        }

        if (need_moar)
        {
#define REBASE(X) \
            ((X) - newpend->backing->data() + new_backing->data())
            std::tr1::shared_ptr<e::buffer> new_backing(e::buffer::create(newpend->backing->size() + need_moar));
            new_backing->resize(newpend->backing->size() + need_moar);
            memmove(new_backing->data(), newpend->backing->data(), newpend->backing->size());
            e::slice oldkey = newpend->key;
            newpend->key = e::slice(REBASE(newpend->key.data()), newpend->key.size());
            assert(oldkey == newpend->key);
            size_t curdata = newpend->backing->size();

            for (size_t i = 0; i < value.size(); ++i)
            {
                if (value_mask.get(i))
                {
                    e::slice oldslice = newpend->value[i];
                    newpend->value[i] = e::slice(REBASE(newpend->value[i].data()), newpend->value[i].size());
                    assert(oldslice == newpend->value[i]);
                }
                else
                {
                    e::slice oldslice = oldvalue[i];
                    memmove(new_backing->data() + curdata, oldvalue[i].data(), oldvalue[i].size());
                    newpend->value[i] = e::slice(new_backing->data() + curdata, oldvalue[i].size());
                    curdata += oldvalue[i].size();
                    assert(oldslice == newpend->value[i]);
                }
            }

            assert(curdata == newpend->backing->size() + need_moar);
            newpend->backing = new_backing;
        }
    }

    if (!prev_and_next(to.get_region(), key, has_value, newpend->value, has_oldvalue, oldvalue, newpend))
    {
        respond_to_client(to, from, nonce, retcode, hyperdex::NET_NOTUS);
        g.dismiss();
        return;
    }

    assert(!kh->has_deferred_ops());
    kh->append_blocked(oldversion + 1, newpend);
    move_operations_between_queues(to, key, kh);
    g.dismiss();
}

void
hyperdaemon :: replication_manager :: chain_common(bool has_value,
                                                   const entityid& from,
                                                   const entityid& to,
                                                   uint64_t version,
                                                   bool fresh,
                                                   std::auto_ptr<e::buffer> backing,
                                                   const e::slice& key,
                                                   const std::vector<e::slice>& value)
{
    // Grab the lock that protects this key.
    HOLD_LOCK_FOR_KEY(to, key);
    // Get the keyholder for this key.
    e::intrusive_ptr<keyholder> kh = get_keyholder(to.get_region(), key);

    // Check that a chain's put matches the dimensions of the space.
    if (has_value && m_config.dimensions(to.get_space()) != value.size() + 1)
    {
        LOG(INFO) << "dropping CHAIN_* because the dimensions are incorrect";
        LOG(INFO) << m_config.dimensions(to.get_space());
        LOG(INFO) << value.size();
        return;
    }

    // Find the pending or committed version with the largest number.
    uint64_t oldversion = 0;
    bool has_oldvalue = false;
    std::vector<e::slice> oldvalue;
    hyperdisk::reference ref;
    e::intrusive_ptr<pending> oldop = kh->get_by_version(version - 1);
    e::intrusive_ptr<pending> newop = kh->get_by_version(version);

    if (newop)
    {
        // XXX Check that the saved state and new message match
        newop->recv = from;
        send_ack(to, from, version, key);
        return;
    }

    if (oldop)
    {
        oldversion = version - 1;
        has_oldvalue = oldop->has_value;
        oldvalue = oldop->value;
    }
    else
    {
        if (!from_disk(to.get_region(), key, &has_oldvalue, &oldvalue, &oldversion, &ref))
        {
            LOG(INFO) << "dropping CHAIN_* because we could not read from the hyperdisk.";
            return;
        }

        if (oldversion >= version)
        {
            send_ack(to, from, version, key);
            return;
        }

        if (oldversion < version - 1)
        {
            oldversion = 0;
        }
    }

    // If the update needs to be deferred.
    if (oldversion == 0 && !fresh)
    {
        e::intrusive_ptr<deferred> newdefer;
        newdefer = new deferred(has_value, backing, key, value, from, m_config.instancefor(from), ref);
        kh->insert_deferred(version, newdefer);
        return;
    }

    // Create a new pending object to set as pending.
    e::intrusive_ptr<pending> newpend;
    std::tr1::shared_ptr<e::buffer> sharedbacking(backing.release());
    newpend = new pending(has_value, sharedbacking, key, value);
    newpend->fresh = fresh;
    newpend->ref = ref;
    newpend->recv = from;

    if (!prev_and_next(to.get_region(), key, has_value, value, has_oldvalue, oldvalue, newpend))
    {
        LOG(INFO) << "dropping CHAIN_* which does not match this host";
        return;
    }

    if (!(from.get_region() == to.get_region() && m_config.chain_adjacent(from, to))
            && !(from.space == to.space
                && from.subspace + 1 == to.subspace
                && m_config.is_tail(from)
                && m_config.is_head(to)))
    {
        LOG(INFO) << "dropping CHAIN_* which didn't come from the right host";
        return;
    }

    kh->append_blocked(version, newpend);
    move_operations_between_queues(to, key, kh);
}

uint64_t
hyperdaemon :: replication_manager :: get_lock_num(const hyperdex::regionid& reg,
                                                   const e::slice& key)
{
    return CityHash64WithSeed(reinterpret_cast<const char*>(key.data()),
                              key.size(),
                              reg.hash());
}

e::intrusive_ptr<hyperdaemon::replication_manager::keyholder>
hyperdaemon :: replication_manager :: get_keyholder(const hyperdex::regionid& reg,
                                                    const e::slice& key)
{
    keypair kp(reg, std::string(reinterpret_cast<const char*>(key.data()), key.size()));
    e::intrusive_ptr<keyholder> kh;

    while (!m_keyholders.lookup(kp, &kh))
    {
        kh = new keyholder();

        if (m_keyholders.insert(kp, kh))
        {
            break;
        }
    }

    return kh;
}

void
hyperdaemon :: replication_manager :: erase_keyholder(const hyperdex::regionid& reg,
                                                      const e::slice& key)
{
    keypair kp(reg, std::string(reinterpret_cast<const char*>(key.data()), key.size()));
    m_keyholders.remove(kp);
}

bool
hyperdaemon :: replication_manager :: from_disk(const regionid& r,
                                                const e::slice& key,
                                                bool* has_value,
                                                std::vector<e::slice>* value,
                                                uint64_t* version,
                                                hyperdisk::reference* ref)
{
    switch (m_data->get(r, key, value, version, ref))
    {
        case hyperdisk::SUCCESS:
            *has_value = true;
            return true;
        case hyperdisk::NOTFOUND:
            *version = 0;
            *has_value = false;
            return true;
        case hyperdisk::MISSINGDISK:
            LOG(ERROR) << "m_data returned MISSINGDISK.";
            return false;
        case hyperdisk::WRONGARITY:
        case hyperdisk::DATAFULL:
        case hyperdisk::SEARCHFULL:
        case hyperdisk::SYNCFAILED:
        case hyperdisk::DROPFAILED:
        case hyperdisk::SPLITFAILED:
        case hyperdisk::DIDNOTHING:
        default:
            LOG(WARNING) << "Data layer returned unexpected result when reading old value.";
            return false;
    }
}

bool
hyperdaemon :: replication_manager :: put_to_disk(const regionid& pending_in,
                                                  e::intrusive_ptr<keyholder> kh,
                                                  uint64_t version)
{
    if (version <= kh->version_on_disk())
    {
        return true;
    }

    e::intrusive_ptr<pending> op = kh->get_by_version(version);

    bool success = true;
    hyperdisk::returncode rc;

    if (!op->has_value
            || (pending_in.subspace == op->subspace_next && pending_in.subspace != 0))
    {
        switch (m_data->del(pending_in, op->backing, op->key))
        {
            case hyperdisk::SUCCESS:
                success = true;
                break;
            case hyperdisk::MISSINGDISK:
            case hyperdisk::WRONGARITY:
            case hyperdisk::NOTFOUND:
            case hyperdisk::DATAFULL:
            case hyperdisk::SEARCHFULL:
            case hyperdisk::SYNCFAILED:
            case hyperdisk::DROPFAILED:
            case hyperdisk::SPLITFAILED:
            case hyperdisk::DIDNOTHING:
                LOG(ERROR) << "commit caused error " << rc;
                success = false;
                break;
            default:
                LOG(ERROR) << "commit caused unknown error";
                success = false;
                break;
        }
    }
    else if (op->has_value)
    {
        switch (m_data->put(pending_in, op->backing, op->key, op->value, version))
        {
            case hyperdisk::SUCCESS:
                success = true;
                break;
            case hyperdisk::MISSINGDISK:
            case hyperdisk::WRONGARITY:
            case hyperdisk::NOTFOUND:
            case hyperdisk::DATAFULL:
            case hyperdisk::SEARCHFULL:
            case hyperdisk::SYNCFAILED:
            case hyperdisk::DROPFAILED:
            case hyperdisk::SPLITFAILED:
            case hyperdisk::DIDNOTHING:
                LOG(ERROR) << "commit caused error " << rc;
                success = false;
                break;
            default:
                LOG(ERROR) << "commit caused unknown error";
                success = false;
                break;
        }
    }

    kh->set_version_on_disk(version);
    return success;
}

bool
hyperdaemon :: replication_manager :: prev_and_next(const regionid& r,
                                                    const e::slice& key,
                                                    bool has_newvalue,
                                                    const std::vector<e::slice>& newvalue,
                                                    bool has_oldvalue,
                                                    const std::vector<e::slice>& oldvalue,
                                                    e::intrusive_ptr<pending> pend)
{
    // Figure out how many subspaces (in total) there are.
    size_t subspaces = m_config.subspaces(r.get_space());
    assert(subspaces > 0);

    // Figure out which subspaces are adjacent to us (or UINT16_MAX if there are none).
    pend->subspace_prev = r.subspace > 0 ? r.subspace - 1 : UINT16_MAX;
    pend->subspace_next = r.subspace < subspaces - 1 ? r.subspace + 1 : UINT16_MAX;

    // Get the hasher for this subspace.
    hyperspacehashing::prefix::hasher hasher_this = m_config.repl_hasher(r.get_subspace());
    hyperspacehashing::prefix::coordinate coord_this_old;
    hyperspacehashing::prefix::coordinate coord_this_new;

    if (has_oldvalue && has_newvalue)
    {
        coord_this_old = hasher_this.hash(key, oldvalue);
        coord_this_new = hasher_this.hash(key, newvalue);
    }
    else if (has_oldvalue)
    {
        coord_this_old = hasher_this.hash(key, oldvalue);
        coord_this_new = coord_this_old;
    }
    else if (has_newvalue)
    {
        coord_this_old = hasher_this.hash(key, newvalue);
        coord_this_new = coord_this_old;
    }
    else
    {
        abort();
    }

    bool set_next = false;

    if (r.coord().contains(coord_this_old)
            && r.coord().contains(coord_this_new))
    {
        pend->point_this = coord_this_new.point;
    }
    else if (r.coord().contains(coord_this_old))
    {
        // Special case for when we are sending to someone with a CHAIN_SUBSPACE
        // message.
        if (pend->subspace_next != UINT16_MAX)
        {
            hyperspacehashing::prefix::hasher hasher(m_config.repl_hasher(hyperdex::subspaceid(r.space, pend->subspace_next)));
            pend->point_next_next = hasher.hash(key, oldvalue).point;
        }

        pend->subspace_next = r.subspace;
        pend->point_this = coord_this_old.point;
        pend->point_next = coord_this_new.point;
        set_next = true;
    }
    else if (r.coord().contains(coord_this_new))
    {
        return false;
    }
    else
    {
        return false;
    }

    if (pend->subspace_prev != UINT16_MAX)
    {
        hyperspacehashing::prefix::hasher hasher_prev(m_config.repl_hasher(hyperdex::subspaceid(r.space, pend->subspace_prev)));

        // If it has both, it has to come from the *new* value.
        if (has_oldvalue && has_newvalue)
        {
            pend->point_prev = hasher_prev.hash(key, newvalue).point;
        }
        else if (has_oldvalue)
        {
            pend->point_prev = hasher_prev.hash(key, oldvalue).point;
        }
        else if (has_newvalue)
        {
            pend->point_prev = hasher_prev.hash(key, newvalue).point;
        }
    }

    if (!set_next && pend->subspace_next != UINT16_MAX)
    {
        hyperspacehashing::prefix::hasher hasher_next(m_config.repl_hasher(hyperdex::subspaceid(r.space, pend->subspace_next)));

        // If it has both, it has to go to the *old* value.
        if (has_oldvalue && has_newvalue)
        {
            pend->point_next = hasher_next.hash(key, oldvalue).point;
        }
        else if (has_oldvalue)
        {
            pend->point_next = hasher_next.hash(key, oldvalue).point;
        }
        else if (has_newvalue)
        {
            pend->point_next = hasher_next.hash(key, newvalue).point;
        }
    }

    return true;
}

void
hyperdaemon :: replication_manager :: move_operations_between_queues(const hyperdex::entityid& us,
                                                                     const e::slice& key,
                                                                     e::intrusive_ptr<keyholder> kh)
{
    while (kh->has_deferred_ops())
    {
        uint64_t oldversion = 0;
        e::intrusive_ptr<pending> oldop;

        if (kh->has_blocked_ops())
        {
            oldversion = kh->most_recent_blocked_version();
            oldop = kh->most_recent_blocked_op();
        }
        else if (kh->has_committable_ops())
        {
            oldversion = kh->most_recent_committable_version();
            oldop = kh->most_recent_committable_op();
        }

        if (oldversion >= kh->oldest_deferred_version())
        {
            LOG(INFO) << "We are dropping a deferred message because we've already seen the version";
            kh->remove_oldest_deferred_op();
            continue;
        }

        if (oldversion + 1 != kh->oldest_deferred_version())
        {
            break;
        }

        // Create a new pending object to set as pending.
        e::intrusive_ptr<deferred> op = kh->oldest_deferred_op();
        e::intrusive_ptr<pending> newop;
        newop = new pending(op->has_value, op->backing, op->key, op->value);
        newop->fresh = false;
        newop->ref = op->ref;
        newop->recv = op->from_ent;

        if (!prev_and_next(us.get_region(), key, newop->has_value, newop->value, oldop->has_value, oldop->value, newop))
        {
            LOG(INFO) << "dropping deferred CHAIN_* which does not match this host";
            return;
        }

        if (!(newop->recv.get_region() == us.get_region() && m_config.chain_adjacent(newop->recv, us))
                && !(newop->recv.space == us.space
                    && newop->recv.subspace + 1 == us.subspace
                    && m_config.is_tail(newop->recv)
                    && m_config.is_head(us)))
        {
            LOG(INFO) << "dropping deferred CHAIN_* which didn't come from the right host";
            return;
        }

        kh->append_blocked(kh->oldest_deferred_version(), newop);
        kh->remove_oldest_deferred_op();
    }

    while (kh->has_blocked_ops())
    {
        uint64_t version = kh->oldest_blocked_version();
        e::intrusive_ptr<pending> op = kh->oldest_blocked_op();

        if ((op->fresh || !op->has_value) && kh->has_committable_ops())
        {
            break;
        }

        kh->transfer_blocked_to_committable();
        send_message(us, version, key, op);
    }
}

void
hyperdaemon :: replication_manager :: send_message(const entityid& us,
                                                   uint64_t version,
                                                   const e::slice& key,
                                                   e::intrusive_ptr<pending> op)
{
    size_t sz_msg = m_comm->header_size()
                  + sizeof(uint64_t)
                  + sizeof(uint8_t)
                  + sizeof(uint32_t)
                  + key.size()
                  + hyperdex::packspace(op->value)
                  + sizeof(uint64_t);
    size_t sz_revkey = m_comm->header_size()
                     + sizeof(uint64_t)
                     + sizeof(uint32_t)
                     + key.size();

    entityid dst;

    if (m_config.is_tail(us))
    {
        // If we're the end of the line.
        if (op->subspace_next == UINT16_MAX)
        {
            std::auto_ptr<e::buffer> revkey(e::buffer::create(sz_revkey));
            bool packed = !(revkey->pack_at(m_comm->header_size()) << version << key).error();
            assert(packed);

            if (m_comm->send(us, us, hyperdex::CHAIN_ACK, revkey))
            {
                op->sent = us;
            }

            return;
        }
        // If we're doing a subspace transfer
        else if (op->subspace_next == us.subspace)
        {
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz_msg));
            bool packed = !(msg->pack_at(m_comm->header_size()) << version << key << op->value << op->point_next_next).error();
            assert(packed);
            dst = entityid(us.space, us.subspace, 64, op->point_next, 0);
            dst = m_config.sloppy_lookup(dst);

            if (m_comm->send(us, dst, hyperdex::CHAIN_SUBSPACE, msg))
            {
                op->sent = dst;
            }

            return;
        }
        // Otherwise its a normal CHAIN_PUT/CHAIN_DEL; fall through.
        else if (op->subspace_next == us.subspace + 1)
        {
            dst = entityid(us.space, op->subspace_next, 64, op->point_next, 0);
            dst = m_config.sloppy_lookup(dst);
        }
        // We must match one of the above.
        else
        {
            abort();
        }
    }
    else
    {
        // If we received this as a chain_subspace
        if (op->subspace_prev == us.subspace)
        {
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz_msg));
            bool packed = !(msg->pack_at(m_comm->header_size()) << version << key << op->value << op->point_next).error();
            assert(packed);
            dst = m_config.chain_next(us);

            if (m_comm->send(us, dst, hyperdex::CHAIN_SUBSPACE, msg))
            {
                op->sent = dst;
            }

            return;
        }
        // Otherwise its a normal CHAIN_PUT/CHAIN_DEL; fall through.
        else
        {
            dst = m_config.chain_next(us);
        }
    }

    std::auto_ptr<e::buffer> msg(e::buffer::create(sz_msg));
    network_msgtype type;

    if (op->has_value)
    {
        uint8_t flags = op->fresh ? 1 : 0;
        bool packed = !(msg->pack_at(m_comm->header_size()) << version << flags << key << op->value).error();
        assert(packed);
        type = hyperdex::CHAIN_PUT;
    }
    else
    {
        bool packed = !(msg->pack_at(m_comm->header_size()) << version << key).error();
        assert(packed);
        type = hyperdex::CHAIN_DEL;
    }

    if (m_comm->send(us, dst, type, msg))
    {
        op->sent = dst;
    }
}

bool
hyperdaemon :: replication_manager :: send_ack(const entityid& from,
                                               const entityid& to,
                                               uint64_t version,
                                               const e::slice& key)
{
    size_t sz = m_comm->header_size()
              + sizeof(uint64_t)
              + sizeof(uint32_t)
              + key.size();
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    bool packed = !(msg->pack_at(m_comm->header_size()) << version << key).error();
    assert(packed);
    return m_comm->send(from, to, hyperdex::CHAIN_ACK, msg);
}

void
hyperdaemon :: replication_manager :: respond_to_client(const entityid& us,
                                                        const entityid& client,
                                                        uint64_t nonce,
                                                        network_msgtype type,
                                                        network_returncode ret)
{
    uint16_t result = static_cast<uint16_t>(ret);
    size_t sz = m_comm->header_size() + sizeof(uint64_t) +sizeof(uint16_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    bool packed = !(msg->pack_at(m_comm->header_size()) << nonce << result).error();
    assert(packed);
    m_comm->send(us, client, type, msg);
}

void
hyperdaemon :: replication_manager :: periodic()
{
    LOG(WARNING) << "Replication \"cron\" thread started.";

    for (uint64_t i = 0; !m_shutdown; ++i)
    {
        try
        {
            retransmit();
        }
        catch (std::exception& e)
        {
            LOG(INFO) << "Uncaught exception when retransmitting: " << e.what();
        }

        e::sleep_ms(250);
    }
}

void
hyperdaemon :: replication_manager :: retransmit()
{
#if 0
    // XXX NEED mutual exclusion with reconfigure
    for (keyholder_map_t::iterator khiter = m_keyholders.begin();
            khiter != m_keyholders.end(); khiter.next())
    {
        // Grab the lock that protects this object.
        uint64_t lock_num = get_lock_num(khiter.key().region,
                                         e::slice(khiter.key().key.data(), khiter.key().key.size()));
        e::striped_lock<po6::threads::mutex>::hold hold(&m_locks, lock_num);

        // Grab some references.
        e::intrusive_ptr<keyholder> kh = khiter.value();
        entityid ent = m_config.entityfor(m_us, khiter.key().region);
        e::slice key(khiter.key().key.data(), khiter.key().key.size());

        // We want to recheck this condition after moving things around.
        if (!kh->has_committable_ops())
        {
            continue;
        }

        // We only touch the first pending update.  If there is an issue which
        // requires retransmission, we shouldn't hit hosts with a number of
        // excess messages.
        uint64_t version = kh->pending_updates.begin()->first;
        e::intrusive_ptr<pending> pend = kh->pending_updates.begin()->second;

        if (pend->sent == entityid() && pend->retransmit >= 1 &&
                ((pend->retransmit != 0) && !(pend->retransmit & (pend->retransmit - 1))))
        {
            send_update(ent, version, key, pend);
        }

        ++pend->retransmit;

        if (pend->retransmit == 64)
        {
            pend->retransmit = 32;
        }
    }
#endif
}
