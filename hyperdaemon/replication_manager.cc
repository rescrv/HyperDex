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
#include "hyperdex/hyperdex/coordinatorlink.h"
#include "hyperdex/hyperdex/network_constants.h"
#include "hyperdex/hyperdex/packing.h"

// HyperDaemon
#include "hyperdaemon/datalayer.h"
#include "hyperdaemon/logical.h"
#include "hyperdaemon/ongoing_state_transfers.h"
#include "hyperdaemon/replication_manager.h"
#include "hyperdaemon/runtimeconfig.h"

using hyperspacehashing::prefix::coordinate;
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
    // Grab the lock that protects this keypair.
    keypair kp(to.get_region(), key);
    e::striped_lock<po6::threads::mutex>::hold hold(&m_locks, get_lock_num(kp));

    // Get a reference to the keyholder for the keypair.
    e::intrusive_ptr<keyholder> kh = get_keyholder(kp);

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

    // Figure out how many subspaces (in total) there are.
    size_t subspaces = m_config.subspaces(to.get_space());
    assert(subspaces > 0);

    // Create a new pending object to set as pending.
    e::intrusive_ptr<pending> newpend;
    newpend = new pending(true, backing, key, value);
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

    kh->pending_updates.insert(std::make_pair(version, newpend));
    send_update(to, version, key, newpend);
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

    std::map<uint64_t, e::intrusive_ptr<pending> >::iterator to_ack;
    to_ack = kh->pending_updates.find(version);

    if (to_ack == kh->pending_updates.end())
    {
        LOG(INFO) << "dropping CHAIN_ACK for update we haven't seen";
        return;
    }

    e::intrusive_ptr<pending> pend = to_ack->second;

    if (from != pend->sent)
    {
        LOG(INFO) << "dropping CHAIN_ACK that came from the wrong host";
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
        send_ack(to, pend->recv, version, key);
    }
    else
    {
        if (pend->co.from.space == UINT32_MAX)
        {
            respond_to_client(to, pend->co.from, pend->co.nonce,
                              pend->retcode, hyperdex::NET_SUCCESS);
            pend->co = clientop();
        }

        unblock_messages(to, key, kh);
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
        respond_to_client(to, from, nonce, retcode, hyperdex::NET_NOTUS);
        return;
    }

    // If we have seen this (from, nonce) before and it is still active.
    if (!m_clientops.insert(co))
    {
        return;
    }

    // Automatically respond with "SERVERERROR" whenever we return without g.dismiss()
    e::guard g = e::makeobjguard(*this, &replication_manager::respond_to_client, to, from, nonce, retcode, hyperdex::NET_SERVERERROR);

    // Grab the lock that protects this keypair.
    keypair kp(to.get_region(), key);
    e::striped_lock<po6::threads::mutex>::hold hold(&m_locks, get_lock_num(kp));

    // Get a reference to the keyholder for the keypair.
    e::intrusive_ptr<keyholder> kh = get_keyholder(kp);

    // Check that a client's put matches the dimensions of the space.
    if (has_value && m_config.dimensions(to.get_space()) != value.size() + 1)
    {
        respond_to_client(to, from, nonce, retcode, hyperdex::NET_WRONGARITY);
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
    newpend->recv = to;

    if (!has_value && !has_oldvalue)
    {
        respond_to_client(to, from, nonce, retcode, hyperdex::NET_SUCCESS);
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

    if (!prev_and_next(kp.region, key, has_value, newpend->value, has_oldvalue, oldvalue, newpend))
    {
        respond_to_client(to, from, nonce, retcode, hyperdex::NET_NOTUS);
        g.dismiss();
        return;
    }

    if (blocked)
    {
        kh->blocked_updates.insert(std::make_pair(oldversion + 1, newpend));
        // This unblock messages will unblock the message we just tagged as
        // fresh (if any).
        unblock_messages(to, key, kh);
    }
    else
    {
        kh->pending_updates.insert(std::make_pair(oldversion + 1, newpend));
        send_update(to, oldversion + 1, key, newpend);
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
            send_ack(to, from, version, key);
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
        send_ack(to, from, version, key);
        return;
    }
    // If the update is pending already.
    else if (kh->pending_updates.find(version) != kh->pending_updates.end())
    {
        // XXX We should check more thoroughly
        kh->pending_updates.find(version)->second->recv = from;
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
    newpend->recv = from;

    if (!prev_and_next(kp.region, key, has_value, value, has_oldvalue, oldvalue, newpend))
    {
        LOG(INFO) << "dropping chain message with no value and no previous value.";
        return;
    }

    if (!(from.get_region() == to.get_region() && m_config.chain_adjacent(from, to))
            && !(from.space == to.space
                && from.subspace + 1 == to.subspace
                && m_config.is_tail(from)
                && m_config.is_head(to)))
    {
        LOG(INFO) << "dropping chain message which didn't come from the right host.";
        LOG(INFO) << from.get_region();
        LOG(INFO) << to.get_region();
        LOG(INFO) << m_config.chain_adjacent(from, to);
        LOG(INFO) << m_config.is_tail(from);
        LOG(INFO) << m_config.is_head(to);
        return;
    }

    kh->pending_updates.insert(std::make_pair(version, newpend));
    send_update(to, version, key, newpend);
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
    hyperdisk::returncode rc;

    if (!update->has_value
            || (pending_in.subspace == update->subspace_next && pending_in.subspace != 0))
    {
        switch (m_data->del(pending_in, update->backing, update->key))
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
    else if (update->has_value)
    {
        switch (m_data->put(pending_in, update->backing, update->key, update->value, version))
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
hyperdaemon :: replication_manager :: unblock_messages(const entityid& us,
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
        send_update(us, pend->first, key, pend->second);
        kh->blocked_updates.erase(pend);
    }
    while (!kh->blocked_updates.empty() && !kh->blocked_updates.begin()->second->fresh);
}

// Invariant:  the lock for the keyholder must be held.
void
hyperdaemon :: replication_manager :: move_deferred_to_pending(const entityid& us,
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

        if (!prev_and_next(us.get_region(), key, defrd.has_value, defrd.value,
                           has_oldvalue, oldvalue, newpend))
        {
            kh->deferred_updates.erase(kh->deferred_updates.begin());
            continue;
        }

        if (m_config.instancefor(defrd.from_ent) == defrd.from_inst)
        {
            kh->deferred_updates.erase(kh->deferred_updates.begin());
            continue;
        }

        if (!(defrd.from_ent.get_region() == us.get_region() && m_config.chain_adjacent(defrd.from_ent, us))
                && !(defrd.from_ent.get_subspace() == us.get_subspace()
                    && defrd.from_ent.subspace + 1 == us.subspace
                    && m_config.is_tail(defrd.from_ent)
                    && m_config.is_head(us)))
        {
            LOG(INFO) << "dropping deferred chain message which didn't come from the right host.";
            continue;
        }

        kh->pending_updates.insert(std::make_pair(version, newpend));
        send_update(us, version, key, newpend);
        kh->deferred_updates.erase(kh->deferred_updates.begin());
    }
}

void
hyperdaemon :: replication_manager :: send_update(const entityid& us,
                                                  uint64_t version,
                                                  const e::slice& key,
                                                  e::intrusive_ptr<pending> pend)
{
    size_t sz_msg = m_comm->header_size()
                  + sizeof(uint64_t)
                  + sizeof(uint8_t)
                  + sizeof(uint32_t)
                  + key.size()
                  + hyperdex::packspace(pend->value)
                  + sizeof(uint64_t);
    size_t sz_revkey = m_comm->header_size()
                     + sizeof(uint64_t)
                     + sizeof(uint32_t)
                     + key.size();

    entityid dst;

    if (m_config.is_tail(us))
    {
        // If we're the end of the line.
        if (pend->subspace_next == UINT16_MAX)
        {
            std::auto_ptr<e::buffer> revkey(e::buffer::create(sz_revkey));
            bool packed = !(revkey->pack_at(m_comm->header_size()) << version << key).error();
            assert(packed);

            if (m_comm->send(us, pend->recv, hyperdex::CHAIN_ACK, revkey))
            {
                pend->sent = pend->recv;
            }

            return;
        }
        // If we're doing a subspace transfer
        else if (pend->subspace_next == us.subspace)
        {
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz_msg));
            bool packed = !(msg->pack_at(m_comm->header_size()) << version << key << pend->value << pend->point_next_next).error();
            assert(packed);
            dst = entityid(us.space, us.subspace, 64, pend->point_next, 0);
            dst = m_config.sloppy_lookup(dst);

            if (m_comm->send(us, dst, hyperdex::CHAIN_SUBSPACE, msg))
            {
                pend->sent = dst;
            }

            return;
        }
        // Otherwise its a normal CHAIN_PUT/CHAIN_DEL; fall through.
        else if (pend->subspace_next == us.subspace + 1)
        {
            dst = entityid(us.space, pend->subspace_next, 64, pend->point_next, 0);
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
        if (pend->subspace_prev == us.subspace)
        {
            std::auto_ptr<e::buffer> msg(e::buffer::create(sz_msg));
            bool packed = !(msg->pack_at(m_comm->header_size()) << version << key << pend->value << pend->point_next).error();
            assert(packed);
            dst = m_config.chain_next(us);

            if (m_comm->send(us, dst, hyperdex::CHAIN_SUBSPACE, msg))
            {
                pend->sent = dst;
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

    if (pend->has_value)
    {
        uint8_t flags = pend->fresh ? 1 : 0;
        bool packed = !(msg->pack_at(m_comm->header_size()) << version << flags << key << pend->value).error();
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
        pend->sent = dst;
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
    m_clientops.remove(clientop(us.get_region(), client, nonce));
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
    // XXX NEED mutual exclusion with reconfigure
    for (keyholder_map_t::iterator khiter = m_keyholders.begin();
            khiter != m_keyholders.end(); khiter.next())
    {
        // Grab the lock that protects this object.
        e::striped_lock<po6::threads::mutex>::hold hold(&m_locks, get_lock_num(khiter.key()));

        // Grab some references.
        e::intrusive_ptr<keyholder> kh = khiter.value();
        entityid ent = m_config.entityfor(m_us, khiter.key().region);
        e::slice key(khiter.key().key.data(), khiter.key().key.size());

        if (kh->pending_updates.empty())
        {
            unblock_messages(ent, key, kh);
            move_deferred_to_pending(ent, key, kh);

            if (kh->pending_updates.empty() &&
                kh->blocked_updates.empty() &&
                kh->deferred_updates.empty())
            {
                erase_keyholder(khiter.key());
            }
        }

        // We want to recheck this condition after moving things around.
        if (kh->pending_updates.empty())
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
}
