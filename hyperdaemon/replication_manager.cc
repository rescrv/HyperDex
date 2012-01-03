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
#include <tr1/functional>

// Google CityHash
#include <city.h>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/guard.h>
#include <e/timer.h>

// HyperDex
#include <hyperdex/coordinatorlink.h>
#include <hyperdex/network_constants.h>
#include <hyperdex/packing.h>

// HyperDaemon
#include "datalayer.h"
#include "logical.h"
#include "ongoing_state_transfers.h"
#include "replication_manager.h"
#include "runtimeconfig.h"

using hyperdex::configuration;
using hyperdex::coordinatorlink;
using hyperdex::entityid;
using hyperdex::instance;
using hyperdex::network_msgtype;
using hyperdex::network_returncode;
using hyperdex::regionid;
using hyperdaemon::replication::clientop;
using hyperdaemon::replication::deferred;
using hyperdaemon::replication::keyholder;
using hyperdaemon::replication::keypair;
using hyperdaemon::replication::pending;
using hyperdaemon::replication::transfer_in;
using hyperdaemon::replication::transfer_out;

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
    , m_clientops(REPLICATION_HASHTABLE_SIZE)
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

    for (keyholder_map_t::iterator khiter = m_keyholders.begin();
            khiter != m_keyholders.begin(); khiter.next())
    {
        // Drop the keyholder if we don't need it.
        if (!m_config.in_region(us, khiter.key().region))
        {
            m_keyholders.remove(khiter.key());
            continue;
        }

        // Drop messages which were deferred but are no longer valid.
        std::map<uint64_t, e::intrusive_ptr<replication::deferred> >::iterator duiter;
        duiter = khiter.value()->deferred_updates.begin();

        while (duiter != khiter.value()->deferred_updates.end())
        {
            if (m_config.instancefor(duiter->second->from_ent) != duiter->second->from_inst)
            {
                std::map<uint64_t, e::intrusive_ptr<replication::deferred> >::iterator to_erase;
                to_erase = duiter;
                ++duiter;
                khiter.value()->deferred_updates.erase(to_erase);
            }
            else
            {
                ++duiter;
            }
        }

        // Promote messages which we have tagged as "mayack".
        if (m_config.instancefor(m_config.headof(khiter.key().region)) == us)
        {
            // We do NOT send_ack here.  This is not an issue because
            // retransmission will, and we are in a state where we cannot
            // transmit messages.
            std::map<uint64_t, e::intrusive_ptr<pending> >::reverse_iterator penditer;
            penditer = khiter.value()->pending_updates.rbegin();

            while (penditer != khiter.value()->pending_updates.rend())
            {
                if (penditer->second->mayack)
                {
                    put_to_disk(khiter.key().region, khiter.value(), penditer->first);
                    break;
                }

                ++penditer;
            }
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
    size_t dims = m_config.dimensions(to.get_space()) - 1;
    e::bitfield bf(dims);
    std::vector<e::slice> realvalue(dims);

    for (size_t i = 0; i < value.size(); ++i)
    {
        // XXX This assert should become a real check.
        assert(0 < value[i].first && value[i].first <= dims);
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
    e::bitfield bf(0);
    client_common(false, from, to, nonce, backing, key, bf, std::vector<e::slice>());
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
hyperdaemon :: replication_manager :: chain_pending(const entityid& from,
                                                    const entityid& to,
                                                    uint64_t version,
                                                    std::auto_ptr<e::buffer>,
                                                    const e::slice& key)
{
    // PENDING messages may only go to the first subspace.
    if (to.subspace != 0)
    {
        return;
    }

    // Grab the lock that protects this keypair.
    keypair kp(to.get_region(), key);
    e::striped_lock<po6::threads::mutex>::hold hold(&m_locks, get_lock_num(kp));

    // Get a reference to the keyholder for the keypair.
    e::intrusive_ptr<keyholder> kh = get_keyholder(kp);

    // Check that the reference is valid.
    if (!kh)
    {
        return;
    }

    std::map<uint64_t, e::intrusive_ptr<pending> >::iterator to_allow_ack;
    to_allow_ack = kh->pending_updates.find(version);

    if (to_allow_ack == kh->pending_updates.end())
    {
        return;
    }

    e::intrusive_ptr<pending> pend = to_allow_ack->second;
    assert(pend->this_old == pend->this_new);

    if (!sent_backward_or_from_tail(from, to, pend->this_old, pend->prev))
    {
        LOG(INFO) << "dropping inappropriately routed CHAIN_PENDING";
        return;
    }

    pend->mayack = true;

    if (m_config.is_point_leader(to))
    {
        send_ack(kp.region, version, key, pend);
        put_to_disk(kp.region, kh, version);

        if (pend->co.from.space == UINT32_MAX)
        {
            clientop co = pend->co;
            pend->co = clientop();
            respond_to_client(co, pend->retcode, hyperdex::NET_SUCCESS);
        }
    }
    else
    {
        size_t sz = m_comm->header_size() + sizeof(uint64_t) + sizeof(uint32_t) + key.size();
        std::auto_ptr<e::buffer> info(e::buffer::create(sz));
        bool packed = !(info->pack_at(m_comm->header_size()) << version << key).error();
        assert(packed);
        m_comm->send_backward(to.get_region(), hyperdex::CHAIN_PENDING, info);
    }
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
    // Grab the lock that protects this keypair.
    keypair kp(to.get_region(), key);
    e::striped_lock<po6::threads::mutex>::hold hold(&m_locks, get_lock_num(kp));

    // Get a reference to the keyholder for the keypair.
    e::intrusive_ptr<keyholder> kh = get_keyholder(kp);

    // Check that the reference is valid.
    if (!kh)
    {
        return;
    }

    // Check that a chain's put matches the dimensions of the space.
    if (m_config.dimensions(to.get_space()) != value.size() + 1)
    {
        return;
    }

    std::map<uint64_t, e::intrusive_ptr<pending> >::iterator lower_bound;
    lower_bound = kh->pending_updates.lower_bound(version);

    if (lower_bound != kh->pending_updates.end() && lower_bound->first > version)
    {
        LOG(INFO) << "received a CHAIN_SUBSPACE message for a revision less than what we've seen.";
    }
    else if (lower_bound != kh->pending_updates.end())
    {
        return; // This is a retransmission.
    }

    size_t subspaces = m_config.subspaces(to.get_space());
    assert(subspaces);
    uint16_t next_subspace;
    next_subspace = to.subspace < subspaces - 1 ? to.subspace + 1 : 0;

    // Create a new pending object to set as pending.
    e::intrusive_ptr<pending> newpend;
    newpend = new pending(true, backing, key, value);
    newpend->prev = regionid();
    newpend->this_old = from.get_region();
    newpend->this_new = to.get_region();
    newpend->next = regionid(to.space, next_subspace, 64, nextpoint);

    if (!sent_forward_or_from_tail(from, to, newpend->this_new, newpend->this_old))
    {
        LOG(INFO) << "dropping CHAIN_SUBSPACE message which didn't come from the right host.";
        return;
    }

    // We may ack this if we are not subspace 0.
    newpend->mayack = kp.region.subspace != 0;

    kh->pending_updates.insert(std::make_pair(version, newpend));
    send_update(to.get_region(), version, key, newpend);
    move_deferred_to_pending(to, key, kh);
}

void
hyperdaemon :: replication_manager :: chain_ack(const entityid& from,
                                                const entityid& to,
                                                uint64_t version,
                                                std::auto_ptr<e::buffer>,
                                                const e::slice& key)
{
    // Grab the lock that protects this keypair.
    keypair kp(to.get_region(), key);
    e::striped_lock<po6::threads::mutex>::hold hold(&m_locks, get_lock_num(kp));

    // Get a reference to the keyholder for the keypair.
    e::intrusive_ptr<keyholder> kh = get_keyholder(kp);

    // Check that the reference is valid.
    if (!kh)
    {
        return;
    }

    std::map<uint64_t, e::intrusive_ptr<pending> >::iterator to_ack;
    to_ack = kh->pending_updates.find(version);

    if (to_ack == kh->pending_updates.end())
    {
        return;
    }

    e::intrusive_ptr<pending> pend = to_ack->second;
    bool drop = false;

    if (pend->this_old == pend->this_new && to.get_region() == pend->this_old)
    {
        drop = !sent_backward_or_from_head(from, to, pend->this_old, pend->next);
    }
    else if (to.get_region() == pend->this_old)
    {
        drop = !sent_backward_or_from_head(from, to, pend->this_old, pend->this_new);
    }
    else if (to.get_region() == pend->this_new)
    {
        drop = !sent_backward_or_from_head(from, to, pend->this_new, pend->next);
    }

    if (drop)
    {
        LOG(INFO) << "dropping inappropriately routed CHAIN_ACK";
        return;
    }

    // If we may not ack the message yet, just drop it.
    if (!pend->mayack)
    {
        LOG(INFO) << "dropping CHAIN_ACK received before we may ACK.";
        return;
    }

    m_ost->add_trigger(kp.region, key, version);
    pend->acked = true;
    put_to_disk(kp.region, kh, version);

    while (!kh->pending_updates.empty() && kh->pending_updates.begin()->second->acked)
    {
        kh->pending_updates.erase(kh->pending_updates.begin());
    }

    if (!m_config.is_point_leader(to))
    {
        send_ack(to.get_region(), version, key, pend);
    }
    else
    {
        unblock_messages(to.get_region(), key, kh);
    }

    if (kh->pending_updates.empty() &&
        kh->blocked_updates.empty() &&
        kh->deferred_updates.empty())
    {
        erase_keyholder(kp);
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
        respond_to_client(co, retcode, hyperdex::NET_NOTUS);
        return;
    }

    // If we have seen this (from, nonce) before and it is still active.
    if (!m_clientops.insert(co))
    {
        return;
    }

    // Automatically respond with "SERVERERROR" whenever we return without g.dismiss()
    e::guard g = e::makeobjguard(*this, &replication_manager::respond_to_client, co, retcode, hyperdex::NET_SERVERERROR);

    // Grab the lock that protects this keypair.
    keypair kp(to.get_region(), key);
    e::striped_lock<po6::threads::mutex>::hold hold(&m_locks, get_lock_num(kp));

    // Get a reference to the keyholder for the keypair.
    e::intrusive_ptr<keyholder> kh = get_keyholder(kp);

    // Check that the reference is valid.
    if (!kh)
    {
        return;
    }

    // Check that a client's put matches the dimensions of the space.
    if (has_value && m_config.dimensions(to.get_space()) != value.size() + 1)
    {
        respond_to_client(co, retcode, hyperdex::NET_WRONGARITY);
        g.dismiss();
        return;
    }

    // Find the pending or committed version with the largest number.
    bool blocked = false;
    uint64_t oldversion = 0;
    bool has_oldvalue = false;
    std::vector<e::slice> oldvalue;
    hyperdisk::reference ref;
    e::intrusive_ptr<pending> oldpend;

    if (kh->blocked_updates.empty())
    {
        if (kh->pending_updates.empty())
        {
            // Get old version from disk.
            if (!from_disk(to.get_region(), key, &has_oldvalue, &oldvalue, &oldversion, &ref))
            {
                return;
            }
        }
        else
        {
            // Get old version from pending updates.
            std::map<uint64_t, e::intrusive_ptr<pending> >::reverse_iterator biggest;
            biggest = kh->pending_updates.rbegin();
            oldversion = biggest->first;
            oldpend = biggest->second;
            has_oldvalue = biggest->second->has_value;
            oldvalue = biggest->second->value;
        }

        blocked = false;
    }
    else
    {
        // Get old version from blocked updates.
        std::map<uint64_t, e::intrusive_ptr<pending> >::reverse_iterator biggest;
        biggest = kh->blocked_updates.rbegin();
        oldversion = biggest->first;
        oldpend = biggest->second;
        has_oldvalue = biggest->second->has_value;
        oldvalue = biggest->second->value;
        blocked = true;
    }

    e::intrusive_ptr<pending> newpend;
    newpend = new pending(has_value, backing, key, value, co);
    newpend->retcode = retcode;
    newpend->ref = ref;

    if (!has_value && !has_oldvalue)
    {
        respond_to_client(co, retcode, hyperdex::NET_SUCCESS);
        g.dismiss();
        return;
    }

    if (has_value && !has_oldvalue)
    {
        // This is a put without a previous value.  If this is the result of it
        // being the first put for this key, then we need to tag it as fresh.
        // If it is the result of a PUT that is concurrent with a DEL (client's
        // perspective), then we need to block it.
        blocked = true;
        newpend->fresh = true;
    }

    if (has_value && has_oldvalue)
    {
        for (size_t i = 0; i < value.size(); ++i)
        {
            if (!value_mask.get(i))
            {
                newpend->backing2 = oldpend;
                newpend->value[i] = oldvalue[i];
            }
        }
    }

    if (!prev_and_next(kp.region, key, has_value, newpend->value, has_oldvalue, oldvalue, newpend))
    {
        return;
    }

    if (kp.region != newpend->this_old && kp.region != newpend->this_new)
    {
        respond_to_client(co, retcode, hyperdex::NET_NOTUS);
        g.dismiss();
        return;
    }

    if (blocked)
    {
        kh->blocked_updates.insert(std::make_pair(oldversion + 1, newpend));
        // This unblock messages will unblock the message we just tagged as
        // fresh (if any).
        unblock_messages(to.get_region(), key, kh);
    }
    else
    {
        kh->pending_updates.insert(std::make_pair(oldversion + 1, newpend));
        send_update(to.get_region(), oldversion + 1, key, newpend);
    }

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
    // Grab the lock that protects this keypair.
    keypair kp(to.get_region(), key);
    e::striped_lock<po6::threads::mutex>::hold hold(&m_locks, get_lock_num(kp));

    // Get a reference to the keyholder for the keypair.
    e::intrusive_ptr<keyholder> kh = get_keyholder(kp);

    // Check that the reference is valid.
    if (!kh)
    {
        return;
    }

    // Check that a chain's put matches the dimensions of the space.
    if (has_value && m_config.dimensions(to.get_space()) != value.size() + 1)
    {
        return;
    }

    // Figure out what to do with the updateI
    bool defer = false;
    const uint64_t oldversion = version - 1;
    bool has_oldvalue = false;
    std::vector<e::slice> oldvalue;
    std::map<uint64_t, e::intrusive_ptr<pending> >::iterator smallest_pending;
    std::map<uint64_t, e::intrusive_ptr<pending> >::iterator olditer;
    smallest_pending = kh->pending_updates.begin();
    hyperdisk::reference ref;

    // If nothing is pending.
    if (smallest_pending == kh->pending_updates.end())
    {
        uint64_t diskversion = 0;
        bool has_diskvalue = false;
        std::vector<e::slice> diskvalue;

        // Get old version from disk.
        if (!from_disk(to.get_region(), key, &has_diskvalue, &diskvalue, &diskversion, &ref))
        {
            LOG(INFO) << "dropping message because we could not read from the hyperdisk.";
            return;
        }

        if (diskversion >= version)
        {
            send_ack(to.get_region(), from, key, version);
            return;
        }
        else if (diskversion == oldversion)
        {
            has_oldvalue = has_diskvalue;
            oldvalue = diskvalue;
            // Fallthrough to set pending.
        }
        else if (fresh)
        {
            has_oldvalue = false;
        }
        else
        {
            defer = true;
        }
    }
    // If the version is committed.
    else if (smallest_pending->first > version)
    {
        send_ack(to.get_region(), from, key, version);
        return;
    }
    // If the update is pending already.
    else if (kh->pending_updates.find(version) != kh->pending_updates.end())
    {
        return;
    }
    // If the update is "fresh".
    else if (fresh)
    {
        has_oldvalue = false;
        // Fallthrough to set pending.
    }
    // If the oldversion is pending.
    else if ((olditer = kh->pending_updates.find(oldversion)) != kh->pending_updates.end())
    {
        has_oldvalue = olditer->second->has_value;
        oldvalue = olditer->second->value;
        // Fallthrough to set pending.
    }
    // Oldversion is committed
    else if (smallest_pending->first > oldversion)
    {
        // This is an interesting case because oldversion is considered
        // committed (all pending updates are greater), but there is not a
        // pending update for version.  This means that an update >= to this one
        // was tagged fresh.  That should only happen when the point-leader has
        // seen the update immediately prior to the fresh one.  This means that
        // the point-leader is misbehaving.
        LOG(INFO) << "dropping chain message which violates version ordering.";
        return;
    }
    else
    {
        defer = true;
    }

    // If the update needs to be deferred.
    if (defer)
    {
        e::intrusive_ptr<deferred> newdefer;
        newdefer = new deferred(has_value, backing, key, value, from, m_config.instancefor(from));
        newdefer->ref = ref;
        kh->deferred_updates.insert(std::make_pair(version, newdefer));
        return;
    }

    // Create a new pending object to set as pending.
    e::intrusive_ptr<pending> newpend;
    newpend = new pending(has_value, backing, key, value);
    newpend->fresh = fresh;
    newpend->ref = ref;

    if (!prev_and_next(kp.region, key, has_value, value, has_oldvalue, oldvalue, newpend))
    {
        LOG(INFO) << "dropping chain message with no value and no previous value.";
        return;
    }

    if (!(to.get_region() == newpend->this_old &&
          sent_forward_or_from_tail(from, to, newpend->this_old, newpend->prev)))
    {
        LOG(INFO) << "dropping chain message which didn't come from the right host.";
        return;
    }

    // We may ack this if we are not subspace 0.
    newpend->mayack = kp.region.subspace != 0;

    kh->pending_updates.insert(std::make_pair(version, newpend));
    send_update(to.get_region(), version, key, newpend);
    move_deferred_to_pending(to, key, kh);
}

size_t
hyperdaemon :: replication_manager :: get_lock_num(const keypair& kp)
{
    return CityHash64WithSeed(static_cast<const char*>(kp.key.data()),
                              kp.key.size(),
                              kp.region.hash());
}

e::intrusive_ptr<hyperdaemon::replication::keyholder>
hyperdaemon :: replication_manager :: get_keyholder(const keypair& kp)
{
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
hyperdaemon :: replication_manager :: erase_keyholder(const keypair& kp)
{
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
                                                  e::intrusive_ptr<replication::keyholder> kh,
                                                  uint64_t version)
{
    if (version <= kh->version_on_disk)
    {
        return true;
    }

    std::map<uint64_t, e::intrusive_ptr<pending> >::iterator to_put_to_disk;
    to_put_to_disk = kh->pending_updates.find(version);

    if (to_put_to_disk == kh->pending_updates.end())
    {
        return false;
    }

    e::intrusive_ptr<pending> update = to_put_to_disk->second;

    bool success = true;

    if (!update->has_value || (update->this_old != update->this_new && pending_in == update->this_old))
    {
        switch (m_data->del(pending_in, update->backing, update->key))
        {
            case hyperdisk::SUCCESS:
                success = true;
                break;
            case hyperdisk::MISSINGDISK:
                LOG(ERROR) << "MISSINGDISK returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::WRONGARITY:
                LOG(ERROR) << "WRONGARITY returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::NOTFOUND:
                LOG(ERROR) << "NOTFOUND returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::DATAFULL:
                LOG(ERROR) << "DATAFULL returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::SEARCHFULL:
                LOG(ERROR) << "SEARCHFULL returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::SYNCFAILED:
                LOG(ERROR) << "SYNCFAILED returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::DROPFAILED:
                LOG(ERROR) << "DROPFAILED returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::SPLITFAILED:
                LOG(ERROR) << "SPLITFAILED returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::DIDNOTHING:
                LOG(ERROR) << "DIDNOTHING returned when committing to disk.";
                success = false;
                break;
            default:
                LOG(ERROR) << "unknown error when committing to disk.";
                success = false;
                break;
        }
    }
    else if (update->has_value)
    {
        switch (m_data->put(pending_in, update->backing, update->key, update->value, version))
        {
            case hyperdisk::SUCCESS:
                success = true;
                break;
            case hyperdisk::MISSINGDISK:
                LOG(ERROR) << "MISSINGDISK returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::WRONGARITY:
                LOG(ERROR) << "WRONGARITY returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::NOTFOUND:
                LOG(ERROR) << "NOTFOUND returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::DATAFULL:
                LOG(ERROR) << "DATAFULL returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::SEARCHFULL:
                LOG(ERROR) << "SEARCHFULL returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::SYNCFAILED:
                LOG(ERROR) << "SYNCFAILED returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::DROPFAILED:
                LOG(ERROR) << "DROPFAILED returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::SPLITFAILED:
                LOG(ERROR) << "SPLITFAILED returned when committing to disk.";
                success = false;
                break;
            case hyperdisk::DIDNOTHING:
                LOG(ERROR) << "DIDNOTHING returned when committing to disk.";
                success = false;
                break;
            default:
                LOG(ERROR) << "unknown error when committing to disk.";
                success = false;
                break;
        }
    }

    kh->version_on_disk = std::max(kh->version_on_disk, version);
    return success;
}

bool
hyperdaemon :: replication_manager :: prev_and_next(const regionid& r,
                                                    const e::slice& key,
                                                    bool has_newvalue,
                                                    const std::vector<e::slice>& newvalue,
                                                    bool has_oldvalue,
                                                    const std::vector<e::slice>& oldvalue,
                                                    e::intrusive_ptr<replication::pending> pend)
{
    size_t subspaces = m_config.subspaces(r.get_space());
    uint16_t prev_subspace;
    uint16_t next_subspace;

    // Discover the dimensions of the space which correspond to the
    // subspace in which "to" is located, and the subspaces before and
    // after it.
    prev_subspace = r.subspace > 0 ? r.subspace - 1 : subspaces - 1;
    next_subspace = r.subspace < subspaces - 1 ? r.subspace + 1 : 0;

    hyperspacehashing::prefix::hasher prev_hasher = m_config.repl_hasher(hyperdex::subspaceid(r.space, prev_subspace));
    hyperspacehashing::prefix::hasher this_hasher = m_config.repl_hasher(r.get_subspace());
    hyperspacehashing::prefix::hasher next_hasher = m_config.repl_hasher(hyperdex::subspaceid(r.space, next_subspace));
    hyperspacehashing::prefix::coordinate prev_coord;
    hyperspacehashing::prefix::coordinate this_old_coord;
    hyperspacehashing::prefix::coordinate this_new_coord;
    hyperspacehashing::prefix::coordinate next_coord;

    if (has_oldvalue && has_newvalue)
    {
        prev_coord = prev_hasher.hash(key, newvalue);
        this_old_coord = this_hasher.hash(key, oldvalue);
        this_new_coord = this_hasher.hash(key, newvalue);
        next_coord = next_hasher.hash(key, oldvalue);
    }
    else if (has_oldvalue)
    {
        prev_coord = prev_hasher.hash(key, oldvalue);
        this_old_coord = this_hasher.hash(key, oldvalue);
        this_new_coord = this_old_coord;
        next_coord = next_hasher.hash(key, oldvalue);
    }
    else if (has_newvalue)
    {
        prev_coord = prev_hasher.hash(key, newvalue);
        this_old_coord = this_hasher.hash(key, newvalue);
        this_new_coord = this_old_coord;
        next_coord = next_hasher.hash(key, newvalue);
    }
    else
    {
        abort();
    }

    pend->prev = regionid(r.space, prev_subspace, prev_coord.prefix, prev_coord.point);
    pend->next = regionid(r.space, next_subspace, next_coord.prefix, next_coord.point);

    if (r.coord().contains(this_old_coord))
    {
        pend->this_old = r;
    }
    else
    {
        pend->this_old = regionid(r.get_subspace(), this_old_coord.prefix, this_old_coord.point);
    }

    if (r.coord().contains(this_new_coord))
    {
        pend->this_new = r;
    }
    else
    {
        pend->this_new = regionid(r.get_subspace(), this_new_coord.prefix, this_new_coord.point);
    }

    return true;
}

void
hyperdaemon :: replication_manager :: unblock_messages(const regionid& r,
                                                       const e::slice& key,
                                                       e::intrusive_ptr<keyholder> kh)
{
    // We cannot unblock so long as there are messages pending.
    if (!kh->pending_updates.empty())
    {
        return;
    }

    if (kh->blocked_updates.empty())
    {
        return;
    }

    assert(kh->blocked_updates.begin()->second->fresh);

    do
    {
        std::map<uint64_t, e::intrusive_ptr<pending> >::iterator pend;
        pend = kh->blocked_updates.begin();
        kh->pending_updates.insert(std::make_pair(pend->first, pend->second));
        send_update(r, pend->first, key, pend->second);
        kh->blocked_updates.erase(pend);
    }
    while (!kh->blocked_updates.empty() && !kh->blocked_updates.begin()->second->fresh);
}

// Invariant:  the lock for the keyholder must be held.
void
hyperdaemon :: replication_manager :: move_deferred_to_pending(const entityid& to,
                                                               const e::slice& key,
                                                               e::intrusive_ptr<keyholder> kh)
{
    while (!kh->deferred_updates.empty())
    {
        const uint64_t version = kh->deferred_updates.begin()->first;
        deferred& defrd(*kh->deferred_updates.begin()->second);

        // Figure out what to do with the updateI
        const uint64_t oldversion = version - 1;
        bool has_oldvalue = false;
        std::vector<e::slice> oldvalue;
        std::map<uint64_t, e::intrusive_ptr<pending> >::iterator smallest_pending;
        std::map<uint64_t, e::intrusive_ptr<pending> >::iterator olditer;
        smallest_pending = kh->pending_updates.begin();

        // If nothing is pending.
        if (smallest_pending == kh->pending_updates.end())
        {
            // Cron can call this without adding to pending.
            break;
        }
        // If the version is committed.
        else if (smallest_pending->first >= version)
        {
            kh->deferred_updates.erase(kh->deferred_updates.begin());
            continue;
        }
        // If the update is pending already.
        else if (kh->pending_updates.find(version) != kh->pending_updates.end())
        {
            kh->deferred_updates.erase(kh->deferred_updates.begin());
            continue;
        }
        // If the oldversion is pending.
        else if ((olditer = kh->pending_updates.find(oldversion)) != kh->pending_updates.end())
        {
            has_oldvalue = olditer->second->has_value;
            oldvalue = olditer->second->value;
            // Fallthrough to set pending.
        }
        // Oldversion is committed
        else if (smallest_pending->first > oldversion)
        {
            kh->deferred_updates.erase(kh->deferred_updates.begin());
            continue;
        }
        else
        {
            // We'd still defer this update.
            break;
        }

        // Create a new pending object to set as pending.
        e::intrusive_ptr<pending> newpend;
        newpend = new pending(defrd.has_value, defrd.backing, defrd.key, defrd.value);

        if (!prev_and_next(to.get_region(), key, defrd.has_value, defrd.value,
                           has_oldvalue, oldvalue, newpend))
        {
            kh->deferred_updates.erase(kh->deferred_updates.begin());
            continue;
        }

        if (!(m_config.instancefor(defrd.from_ent) == defrd.from_inst &&
              sent_forward_or_from_tail(defrd.from_ent, to, newpend->this_old, newpend->prev)))
        {
            kh->deferred_updates.erase(kh->deferred_updates.begin());
            continue;
        }

        // We may ack this if we are not subspace 0.
        newpend->mayack = to.subspace != 0;

        kh->pending_updates.insert(std::make_pair(version, newpend));
        send_update(to.get_region(), version, key, newpend);
        kh->deferred_updates.erase(kh->deferred_updates.begin());
    }
}

void
hyperdaemon :: replication_manager :: send_update(const hyperdex::regionid& pending_in,
                                                  uint64_t version, const e::slice& key,
                                                  e::intrusive_ptr<pending> update)
{
    // XXX Cleanup this logic to avoid all the useless packings.
    assert(pending_in == update->this_old || pending_in == update->this_new);

    size_t sz_revkey = m_comm->header_size()
                     + sizeof(uint64_t)
                     + sizeof(uint32_t)
                     + key.size();
    std::auto_ptr<e::buffer> revkey(e::buffer::create(sz_revkey));
    bool packed = !(revkey->pack_at(m_comm->header_size()) << version << key).error();
    assert(packed);

    size_t sz_msg = m_comm->header_size()
                  + sizeof(uint64_t)
                  + sizeof(uint8_t)
                  + sizeof(uint32_t)
                  + key.size()
                  + hyperdex::packspace(update->value);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz_msg));

    if (update->this_old == update->this_new)
    {
        network_msgtype type;

        if (update->has_value)
        {
            uint8_t fresh = update->fresh ? 1 : 0;
            packed = !(msg->pack_at(m_comm->header_size()) << version << fresh << key << update->value).error();
            assert(packed);
            type = hyperdex::CHAIN_PUT;
        }
        else
        {
            packed = !(msg->pack_at(m_comm->header_size()) << version << key).error();
            assert(packed);
            type = hyperdex::CHAIN_DEL;
        }

        if (update->next.subspace == 0)
        {
            std::auto_ptr<e::buffer> cp(revkey->copy());
            // If we there is someone else in the chain, send the PUT or DEL pending
            // message to them.  If not, then send a PENDING message to the tail of
            // the first subspace.
            m_comm->send_forward_else_tail(pending_in, type, msg,
                                           update->next, hyperdex::CHAIN_PENDING, cp);
        }
        else
        {
            std::auto_ptr<e::buffer> msg2(msg->copy());
            m_comm->send_forward_else_head(pending_in, type, msg, update->next, type, msg2);
        }

        if (update->this_old.subspace == 0 && update->mayack)
        {
            std::auto_ptr<e::buffer> cp(revkey->copy());
            std::auto_ptr<e::buffer> ca(revkey->copy());
            m_comm->send_backward_else_tail(pending_in, hyperdex::CHAIN_PENDING, cp,
                                            update->prev, hyperdex::CHAIN_ACK, ca);
        }
    }
    else if (pending_in == update->this_old)
    {
        assert(update->has_value);
        uint8_t fresh = update->fresh ? 1 : 0;
        std::auto_ptr<e::buffer> msg2(msg->copy());
        packed = !(msg->pack_at(m_comm->header_size()) << version << fresh << key << update->value).error();
        assert(packed);
        packed = !(msg2->pack_at(m_comm->header_size()) << version << key << update->value << update->next.mask).error();
        assert(packed);
        m_comm->send_forward_else_head(update->this_old, hyperdex::CHAIN_PUT, msg,
                                       update->this_new, hyperdex::CHAIN_SUBSPACE, msg2);
    }
    else if (pending_in == update->this_new)
    {
        assert(update->has_value);
        uint8_t fresh = update->fresh ? 1 : 0;
        packed = !(msg->pack_at(m_comm->header_size()) << version << key << update->value << update->next.mask).error();
        assert(packed);

        if (update->next.subspace == 0)
        {
            m_comm->send_forward_else_head(update->this_new, hyperdex::CHAIN_SUBSPACE, msg,
                                           update->next, hyperdex::CHAIN_PENDING, revkey);
        }
        else
        {
            std::auto_ptr<e::buffer> newmsg(e::buffer::create(sz_msg));
            packed = !(newmsg->pack_at(m_comm->header_size()) << version << fresh << key << update->value).error();
            assert(packed);
            m_comm->send_forward_else_head(update->this_new, hyperdex::CHAIN_SUBSPACE, msg,
                                           update->next, hyperdex::CHAIN_PUT, newmsg);
        }
    }
    else
    {
        LOG(ERROR) << "There is a case for \"send_update\" which is not handled.";
    }
}

void
hyperdaemon :: replication_manager :: send_ack(const regionid& pending_in,
                                               uint64_t version,
                                               const e::slice& key,
                                               e::intrusive_ptr<pending> update)
{
    size_t sz = m_comm->header_size()
              + sizeof(uint64_t)
              + sizeof(uint32_t)
              + key.size();
    std::auto_ptr<e::buffer> msg1(e::buffer::create(sz));
    bool packed = !(msg1->pack_at(m_comm->header_size()) << version << key).error();
    assert(packed);
    std::auto_ptr<e::buffer> msg2(msg1->copy());

    if (pending_in == update->this_old)
    {
        m_comm->send_backward_else_tail(pending_in, hyperdex::CHAIN_ACK, msg1,
                                        update->prev, hyperdex::CHAIN_ACK, msg2);
    }
    else if (pending_in == update->this_new)
    {
        m_comm->send_backward_else_tail(pending_in, hyperdex::CHAIN_ACK, msg1,
                                        update->this_old, hyperdex::CHAIN_ACK, msg2);
    }
}

void
hyperdaemon :: replication_manager :: send_ack(const regionid& from,
                                               const entityid& to,
                                               const e::slice& key,
                                               uint64_t version)
{
    size_t sz = m_comm->header_size()
              + sizeof(uint64_t)
              + sizeof(uint32_t)
              + key.size();
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    bool packed = !(msg->pack_at(m_comm->header_size()) << version << key).error();
    assert(packed);
    m_comm->send(from, to, hyperdex::CHAIN_ACK, msg);
}

void
hyperdaemon :: replication_manager :: respond_to_client(clientop co,
                                                        network_msgtype type,
                                                        network_returncode ret)
{
    uint16_t result = static_cast<uint16_t>(ret);
    size_t sz = m_comm->header_size() + sizeof(uint64_t) +sizeof(uint16_t);
    std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
    bool packed = !(msg->pack_at(m_comm->header_size()) << co.nonce << result).error();
    assert(packed);
    m_clientops.remove(co);
    m_comm->send(co.region, co.from, type, msg);
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
    // XXX Need to actually retransmit something.
#if 0
    std::set<keypair> kps;

    // Hold the lock just long enough to copy of all current key pairs.
    {
        po6::threads::mutex::hold hold(&m_keyholders_lock);
        std::tr1::unordered_map<keypair, e::intrusive_ptr<keyholder>, keypair::hash>::iterator khiter;

        for (khiter = m_keyholders.begin(); khiter != m_keyholders.end(); ++khiter)
        {
            kps.insert(khiter->first);
        }
    }

    for (std::set<keypair>::iterator kp = kps.begin(); kp != kps.end(); ++kp)
    {
        // Grab the lock that protects this keypair.
        e::striped_lock<po6::threads::mutex>::hold hold(&m_locks, get_lock_num(*kp));

        // Get a reference to the keyholder for the keypair.
        e::intrusive_ptr<keyholder> kh = NULL;

        {
            po6::threads::mutex::hold hold_khl(&m_keyholders_lock);
            std::tr1::unordered_map<keypair, e::intrusive_ptr<keyholder>, keypair::hash>::iterator i;

            if ((i = m_keyholders.find(*kp)) != m_keyholders.end())
            {
                kh = i->second;
            }
        }

        if (!kh)
        {
            continue;
        }

        if (kh->pending_updates.empty())
        {
            unblock_messages(kp->region, kp->key, kh);
            move_deferred_to_pending(kp->region, kp->key, kh);

            if (kh->pending_updates.empty() &&
                kh->blocked_updates.empty() &&
                kh->deferred_updates.empty())
            {
                erase_keyholder(*kp);
            }

            continue;
        }

        // We only touch the first pending update.  If there is an issue which
        // requires retransmission, we shouldn't hit hosts with a number of
        // excess messages.
        uint64_t version = kh->pending_updates.begin()->first;
        e::intrusive_ptr<pending> pend = kh->pending_updates.begin()->second;

        if (pend->retransmit >= 2 && pend->retransmit & (pend->retransmit - 1))
        {
            send_update(kp->region, version, kp->key, pend);
        }

        ++pend->retransmit;

        if (pend->retransmit == 64)
        {
            pend->retransmit = 32;
        }
    }
#endif
}

bool
hyperdaemon :: replication_manager :: sent_backward_or_from_head(const entityid& from,
                                                                 const entityid& to,
                                                                 const regionid& chain,
                                                                 const regionid& head)
{
    return (from.get_region() == to.get_region()
            && chain == from.get_region()
            && from.number == to.number + 1)
           ||
           (from == m_config.headof(head));
}

bool
hyperdaemon :: replication_manager :: sent_backward_or_from_tail(const entityid& from,
                                                                 const entityid& to,
                                                                 const regionid& chain,
                                                                 const regionid& tail)
{
    return (from.get_region() == to.get_region()
            && chain == from.get_region()
            && from.number == to.number + 1)
           ||
           (from == m_config.tailof(tail));
}

bool
hyperdaemon :: replication_manager :: sent_forward_or_from_tail(const entityid& from,
                                                                const entityid& to,
                                                                const regionid& chain,
                                                                const regionid& tail)
{
    return (from.get_region() == to.get_region()
            && chain == from.get_region()
            && from.number + 1 == to.number)
           ||
           (from == m_config.tailof(tail));
}
