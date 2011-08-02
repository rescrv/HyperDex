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
#include <hyperdex/hyperspace.h>
#include <hyperdex/network_constants.h>

// HyperDaemon
#include "datalayer.h"
#include "logical.h"
#include "replication_manager.h"

// XXX Make this configurable.
#define LOCK_STRIPING 1024
#define TRANSFERS_IN_FLIGHT 1000

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
                                                          logical* comm)
    : m_cl(cl)
    , m_data(data)
    , m_comm(comm)
    , m_config()
    , m_locks(LOCK_STRIPING)
    , m_keyholders_lock()
    , m_keyholders()
    , m_clientops(10)
    , m_shutdown(false)
    , m_periodic_thread(std::tr1::bind(&replication_manager::periodic, this))
    , m_transfers_in()
    , m_transfers_in_by_region()
    , m_transfers_out()
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
    // Figure out the inbound and outbound transfers which affect us.
    std::map<uint16_t, regionid> in_transfers = newconfig.transfers_to(us);
    std::map<uint16_t, regionid> out_transfers = newconfig.transfers_from(us);

    // Make sure that an inbound "transfer" object exists for each in-progress
    // transfer to "us".
    for (std::map<uint16_t, regionid>::iterator t = in_transfers.begin(); t != in_transfers.end(); ++t)
    {
        if (m_transfers_in.find(t->first) == m_transfers_in.end())
        {
            LOG(INFO) << "Initiating inbound transfer #" << t->first << ".";
            e::intrusive_ptr<transfer_in> xfer = new transfer_in(t->first, newconfig.tailof(t->second), t->second);
            m_transfers_in.insert(std::make_pair(t->first, xfer));
            m_transfers_in_by_region.insert(std::make_pair(t->second, xfer));
        }
    }

    // Make sure that an outbound "transfer" object exists for each in-progress
    // transfer from "us".
    for (std::map<uint16_t, regionid>::iterator t = out_transfers.begin(); t != out_transfers.end(); ++t)
    {
        if (m_transfers_out.find(t->first) == m_transfers_out.end()
            && m_transfers_in.find(t->first) == m_transfers_in.end())
        {
            LOG(INFO) << "Initiating outbound transfer #" << t->first << ".";
            e::intrusive_ptr<hyperdisk::disk::rolling_snapshot> snap = m_data->make_rolling_snapshot(t->second);
            e::intrusive_ptr<transfer_out> xfer = new transfer_out(newconfig.tailof(t->second), t->first, snap);
            m_transfers_out.insert(std::make_pair(t->first, xfer));
        }
    }

    // Remove those transfers which we no longer need.
    for (std::map<uint16_t, e::intrusive_ptr<transfer_in> >::iterator t = m_transfers_in.begin();
            t != m_transfers_in.end(); )
    {
        // If the transfer no longer exists.
        if (in_transfers.find(t->first) == in_transfers.end())
        {
            LOG(INFO) << "Stopping transfer #" << t->first << ".";
            std::map<uint16_t, e::intrusive_ptr<transfer_in> >::iterator to_erase;
            to_erase = t;
            ++t;
            m_transfers_in_by_region.erase(to_erase->second->replicate_from.get_region());
            m_transfers_in.erase(to_erase);
        }
        else
        {
            ++t;
        }
    }

    for (std::map<uint16_t, e::intrusive_ptr<transfer_out> >::iterator t = m_transfers_out.begin();
            t != m_transfers_out.end(); )
    {
        if (out_transfers.find(t->first) == out_transfers.end())
        {
            LOG(INFO) << "Stopping transfer #" << t->first << ".";
            std::map<uint16_t, e::intrusive_ptr<transfer_out> >::iterator to_erase;
            to_erase = t;
            ++t;
            m_transfers_out.erase(to_erase);
        }
        else
        {
            ++t;
        }
    }

    // XXX need to convert "mayack" messages to disk for every region for which
    // we are the row leader.

    // When reconfiguring, we need to drop all messages which we deferred (e.g.,
    // for arriving out-of-order) because they may no longer be from a valid
    // sender.
    //
    // We also drop all resources associated with regions for which we no longer
    // hold responsibility.
    //
    // XXX If we track the old/new entity->instance mappings for deferred
    // messages, we can selectively drop them.
    m_config = newconfig;
    std::set<regionid> regions;

    for (std::map<entityid, instance>::const_iterator e = m_config.entity_mapping().begin();
            e != m_config.entity_mapping().end(); ++e)
    {
        if (e->second == us)
        {
            regions.insert(e->first.get_region());
        }
    }

    std::tr1::unordered_map<keypair, e::intrusive_ptr<keyholder>, keypair::hash>::iterator khiter;
    khiter = m_keyholders.begin();

    while (khiter != m_keyholders.end())
    {
        khiter->second->deferred_updates.clear();

        if (regions.find(khiter->first.region) == regions.end())
        {
            std::tr1::unordered_map<keypair, e::intrusive_ptr<keyholder>, keypair::hash>::iterator to_erase;
            to_erase = khiter;
            ++khiter;
            m_keyholders.erase(to_erase);
        }
        else
        {
            ++khiter;
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
hyperdaemon :: replication_manager :: client_put(const entityid& from,
                                                 const entityid& to,
                                                 uint32_t nonce,
                                                 const e::buffer& key,
                                                 const std::vector<e::buffer>& newvalue)
{
    client_common(true, from, to, nonce, key, newvalue);
}

void
hyperdaemon :: replication_manager :: client_del(const entityid& from,
                                                 const entityid& to,
                                                 uint32_t nonce,
                                                 const e::buffer& key)
{
    client_common(false, from, to, nonce, key, std::vector<e::buffer>());
}

void
hyperdaemon :: replication_manager :: chain_put(const entityid& from,
                                                const entityid& to,
                                                uint64_t newversion,
                                                bool fresh,
                                                const e::buffer& key,
                                                const std::vector<e::buffer>& newvalue)
{
    chain_common(true, from, to, newversion, fresh, key, newvalue);
}

void
hyperdaemon :: replication_manager :: chain_del(const entityid& from,
                                                const entityid& to,
                                                uint64_t newversion,
                                                const e::buffer& key)
{
    chain_common(false, from, to, newversion, false, key, std::vector<e::buffer>());
}

void
hyperdaemon :: replication_manager :: chain_pending(const entityid& from,
                                                    const entityid& to,
                                                    uint64_t version,
                                                    const e::buffer& key)
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

    if (is_point_leader(to))
    {
        send_ack(kp.region, version, key, pend);
        put_to_disk(kp.region, key, kh, version);

        if (pend->co.from.space == UINT32_MAX)
        {
            clientop co = pend->co;
            pend->co = clientop();
            respond_to_client(co, pend->has_value ? hyperdex::RESP_PUT : hyperdex::RESP_DEL, hyperdex::NET_SUCCESS);
        }
    }
    else
    {
        e::buffer info;
        info.pack() << version << key;
        m_comm->send_backward(to.get_region(), hyperdex::CHAIN_PENDING, info);
    }
}

void
hyperdaemon :: replication_manager :: chain_subspace(const entityid& from,
                                                     const entityid& to,
                                                     uint64_t version,
                                                     const e::buffer& key,
                                                     const std::vector<e::buffer>& value,
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
    if (expected_dimensions(kp.region) != value.size() + 1)
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

    size_t subspaces;
    uint16_t next_subspace;

    if (!m_config.subspaces(to.get_space(), &subspaces))
    {
        return;
    }

    next_subspace = to.subspace < subspaces - 1 ? to.subspace + 1 : 0;

    // Create a new pending object to set as pending.
    e::intrusive_ptr<pending> newpend;
    newpend = new pending(true, value);
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
                                                const e::buffer& key)
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

    // If there exists a "transfers in" for this region, then add this acked key
    // to the set of completion triggers for the region.  This must happen
    // before the put to disk of the ack.  By doing so it blocks the transfer
    // from overwriting the same key.
    std::map<regionid, e::intrusive_ptr<transfer_in> >::iterator titer;
    titer = m_transfers_in_by_region.find(kp.region);

    if (titer != m_transfers_in_by_region.end())
    {
        e::intrusive_ptr<transfer_in> t = titer->second;
        // Grab a lock to ensure we can safely update it.
        po6::threads::mutex::hold hold_xfer(&t->lock);

        // Add this key/version to the set of triggers.
        t->triggers.insert(std::make_pair(key, version));
    }

    pend->acked = true;
    put_to_disk(kp.region, key, kh, version);

    while (!kh->pending_updates.empty() && kh->pending_updates.begin()->second->acked)
    {
        kh->pending_updates.erase(kh->pending_updates.begin());
    }

    send_ack(to.get_region(), version, key, pend);
    unblock_messages(to.get_region(), key, kh);

    if (kh->pending_updates.empty())
    {
        erase_keyholder(kp);
    }
}

void
hyperdaemon :: replication_manager :: region_transfer(const entityid& from,
                                                      const entityid& to)
{
    // Find the transfer_out object.
    std::map<uint16_t, e::intrusive_ptr<transfer_out> >::iterator titer;
    titer = m_transfers_out.find(from.subspace);

    if (titer == m_transfers_out.end())
    {
        return;
    }

    e::intrusive_ptr<transfer_out> t = titer->second;

    if (from != t->transfer_entity || to != t->replicate_from)
    {
        return;
    }

    // Grab a lock to ensure we can safely update it.
    po6::threads::mutex::hold hold(&t->lock);

    if (t->snap->valid())
    {
        uint8_t op = t->snap->has_value() ? 1 : 0;
        e::buffer msg;
        msg.pack() << t->xfer_num << op << t->snap->version()
                   << t->snap->key() << t->snap->value();
        ++t->xfer_num;
        t->snap->next();

        if (!m_comm->send(to, from, hyperdex::XFER_DATA, msg))
        {
            // XXX
            LOG(ERROR) << "FAIL TRANSFER " << from.subspace;
        }
    }
    else
    {
        e::buffer msg;

        if (!m_comm->send(to, from, hyperdex::XFER_DONE, msg))
        {
            // XXX
            LOG(ERROR) << "FAIL TRANSFER " << from.subspace;
        }
    }
}

void
hyperdaemon :: replication_manager :: region_transfer(const entityid& from,
                                                      uint16_t xfer_id,
                                                      uint64_t xfer_num,
                                                      bool has_value,
                                                      uint64_t version,
                                                      const e::buffer& key,
                                                      const std::vector<e::buffer>& value)
{
    // Find the transfer_in object.
    std::map<uint16_t, e::intrusive_ptr<transfer_in> >::iterator titer;
    titer = m_transfers_in.find(xfer_id);

    if (titer == m_transfers_in.end())
    {
        return;
    }

    e::intrusive_ptr<transfer_in> t = titer->second;

    // Grab a lock to ensure that we order the puts to disk correctly.
    keypair kp(t->replicate_from.get_region(), key);
    e::striped_lock<po6::threads::mutex>::hold hold_k(&m_locks, get_lock_num(kp));

    // Grab a lock to ensure we can safely update the transfer object.
    po6::threads::mutex::hold hold_t(&t->lock);

    if (from != t->replicate_from)
    {
        return;
    }

    if (t->triggered)
    {
        return;
    }

    // This is a probabilistic test of the remote end's failure.  If we have
    // queued more than 1000 messages, then somewhere one got dropped as the
    // other end inserts them into the queue in FIFO order, and they are
    // delivered to threads in FIFO order.  There is always a chance that what
    // really happened was one thread pulled off the missing message, and then
    // was blocked arbitrarily long while other threads worked through several
    // messages.  The large constant ensures that this is exceedingly unlikely
    // (but admittedly still possible).
    if (t->ops.size() > TRANSFERS_IN_FLIGHT)
    {
        // XXX FAIL THIS TRANSFER
        LOG(ERROR) << "FAIL TRANSFER " << xfer_id;
        return;
    }

    // Insert the new operation.
    e::intrusive_ptr<transfer_in::op> o = new transfer_in::op(has_value, version, key, value);
    t->ops.insert(std::make_pair(xfer_num, o));

    while (!t->ops.empty() && t->ops.begin()->first == t->xferred_so_far + 1)
    {
        transfer_in::op& one(*t->ops.begin()->second);

        if (t->triggers.find(std::make_pair(one.key, one.version)) != t->triggers.end())
        {
            t->triggered = true;
            LOG(INFO) << "COMPLETE TRANSFER " << xfer_id;
            return;
        }

        if (t->triggers.lower_bound(std::make_pair(one.key, 0)) ==
                t->triggers.upper_bound(std::make_pair(one.key, std::numeric_limits<uint64_t>::max() - 1)))
        {
            hyperdisk::returncode res;

            if (one.has_value)
            {
                switch ((res = m_data->put(t->replicate_from.get_region(), one.key, one.value, one.version)))
                {
                    case hyperdisk::SUCCESS:
                        break;
                    case hyperdisk::WRONGARITY:
                        LOG(ERROR) << "Transfer caused WRONGARITY.";
                        break;
                    case hyperdisk::MISSINGDISK:
                        LOG(ERROR) << "Transfer returned MISSINGDISK.";
                        break;
                    case hyperdisk::NOTFOUND:
                    case hyperdisk::HASHFULL:
                    case hyperdisk::DATAFULL:
                    case hyperdisk::SEARCHFULL:
                    case hyperdisk::SYNCFAILED:
                    case hyperdisk::DROPFAILED:
                    default:
                        LOG(ERROR) << "m_data returned unexpected error code.";
                        break;
                }
            }
            else
            {
                switch ((res = m_data->del(t->replicate_from.get_region(), one.key)))
                {
                    case hyperdisk::SUCCESS:
                        break;
                    case hyperdisk::MISSINGDISK:
                        LOG(ERROR) << "Transfer returned MISSINGDISK.";
                        break;
                    case hyperdisk::WRONGARITY:
                    case hyperdisk::NOTFOUND:
                    case hyperdisk::HASHFULL:
                    case hyperdisk::DATAFULL:
                    case hyperdisk::SEARCHFULL:
                    case hyperdisk::SYNCFAILED:
                    case hyperdisk::DROPFAILED:
                    default:
                        LOG(ERROR) << "m_data returned unexpected error code.";
                        break;
                }
            }

            if (res != hyperdisk::SUCCESS)
            {
                // XXX FAIL THIS TRANSFER
                LOG(ERROR) << "FAIL TRANSFER " << xfer_id;
                return;
            }
        }

        ++t->xferred_so_far;
        t->ops.erase(t->ops.begin());
    }

    t->started = true;
    e::buffer msg;

    if (!m_comm->send(t->transfer_entity, t->replicate_from, hyperdex::XFER_MORE, msg))
    {
        // XXX FAIL
        LOG(ERROR) << "FAIL TRANSFER " << xfer_id;
    }
}

void
hyperdaemon :: replication_manager :: region_transfer_done(const entityid& from, const entityid& to)
{
    // Find the transfer_in object.
    std::map<uint16_t, e::intrusive_ptr<transfer_in> >::iterator titer;
    titer = m_transfers_in.find(to.subspace);

    if (titer == m_transfers_in.end())
    {
        return;
    }

    e::intrusive_ptr<transfer_in> t = titer->second;

    // Grab a lock to ensure we can safely update it.
    po6::threads::mutex::hold hold(&t->lock);

    if (from != t->replicate_from)
    {
        return;
    }

    t->started = true;

    if (!t->go_live)
    {
        t->go_live = true;
        LOG(INFO) << "REGION GO LIVE " << to.subspace;
    }
}

// XXX It'd be a good idea to have the same thread which does periodic
// retransmission also handle cleaning of deferred updates which are too old.
// At any time it's acceptable to drop a deferred update, so this won't hurt
// correctness, and does make it easy to reclaim keyholders (if
// pending/blocked/deferred are empty then drop it).

void
hyperdaemon :: replication_manager :: client_common(bool has_value,
                                                    const entityid& from,
                                                    const entityid& to,
                                                    uint32_t nonce,
                                                    const e::buffer& key,
                                                    const std::vector<e::buffer>& value)
{
    // Make sure this message is from a client.
    if (from.space != UINT32_MAX)
    {
        LOG(INFO) << "dropping client-only message from " << from << " (it is not a client).";
        return;
    }

    clientop co(to.get_region(), from, nonce);

    // Make sure this message is to the point-leader.
    if (!is_point_leader(to))
    {
        respond_to_client(co, has_value ? hyperdex::RESP_PUT : hyperdex::RESP_DEL, hyperdex::NET_NOTUS);
        return;
    }

    // If we have seen this (from, nonce) before and it is still active.
    if (!m_clientops.insert(co))
    {
        return;
    }

    // Automatically respond with "SERVERERROR" whenever we return without g.dismiss()
    e::guard g = e::makeobjguard(*this, &replication_manager::respond_to_client, co, has_value ? hyperdex::RESP_PUT : hyperdex::RESP_DEL, hyperdex::NET_SERVERERROR);

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
    if (has_value && expected_dimensions(kp.region) != value.size() + 1)
    {
        respond_to_client(co, hyperdex::RESP_PUT, hyperdex::NET_WRONGARITY);
        g.dismiss();
        return;
    }

    // Find the pending or committed version with the largest number.
    bool blocked = false;
    uint64_t oldversion = 0;
    bool has_oldvalue = false;
    std::vector<e::buffer> oldvalue;

    if (kh->blocked_updates.empty())
    {
        if (kh->pending_updates.empty())
        {
            // Get old version from disk.
            if (!from_disk(to.get_region(), key, &has_oldvalue, &oldvalue, &oldversion))
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
        has_oldvalue = biggest->second->has_value;
        oldvalue = biggest->second->value;
        blocked = true;
    }

    e::intrusive_ptr<pending> newpend;
    newpend = new pending(has_value, value, co);

    if (!has_value && !has_oldvalue)
    {
        respond_to_client(co, hyperdex::RESP_PUT, hyperdex::NET_SUCCESS);
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

    if (!prev_and_next(kp.region, key, has_value, value, has_oldvalue, oldvalue, newpend))
    {
        return;
    }

    if (kp.region != newpend->this_old && kp.region != newpend->this_new)
    {
        respond_to_client(co, hyperdex::RESP_PUT, hyperdex::NET_NOTUS);
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
                                                   const e::buffer& key,
                                                   const std::vector<e::buffer>& value)
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
    if (has_value && expected_dimensions(kp.region) != value.size() + 1)
    {
        return;
    }

    // Figure out what to do with the updateI
    bool defer = false;
    const uint64_t oldversion = version - 1;
    bool has_oldvalue = false;
    std::vector<e::buffer> oldvalue;
    std::map<uint64_t, e::intrusive_ptr<pending> >::iterator smallest_pending;
    std::map<uint64_t, e::intrusive_ptr<pending> >::iterator olditer;
    smallest_pending = kh->pending_updates.begin();

    // If nothing is pending.
    if (smallest_pending == kh->pending_updates.end())
    {
        uint64_t diskversion = 0;
        bool has_diskvalue = false;
        std::vector<e::buffer> diskvalue;

        // Get old version from disk.
        if (!from_disk(to.get_region(), key, &has_diskvalue, &diskvalue, &diskversion))
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
        newdefer = new deferred(has_value, value, from, m_config.lookup(from));
        kh->deferred_updates.insert(std::make_pair(version, newdefer));
        return;
    }

    // Create a new pending object to set as pending.
    e::intrusive_ptr<pending> newpend;
    newpend = new pending(has_value, value);

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
    uint64_t region_hash = kp.region.space + kp.region.subspace
                           + kp.region.prefix + kp.region.mask;
    uint64_t key_hash = CityHash64(kp.key);
    return region_hash + key_hash;
}

e::intrusive_ptr<hyperdaemon::replication::keyholder>
hyperdaemon :: replication_manager :: get_keyholder(const keypair& kp)
{
    typedef std::tr1::unordered_map<keypair, e::intrusive_ptr<keyholder>, keypair::hash>::iterator kh_iter_t;
    po6::threads::mutex::hold hold(&m_keyholders_lock);
    kh_iter_t i;

    if ((i = m_keyholders.find(kp)) != m_keyholders.end())
    {
        return i->second;
    }
    else
    {
        e::intrusive_ptr<keyholder> kh;
        kh = new keyholder();
        m_keyholders.insert(std::make_pair(kp, kh));
        return kh;
    }
}

void
hyperdaemon :: replication_manager :: erase_keyholder(const keypair& kp)
{
    po6::threads::mutex::hold hold(&m_keyholders_lock);
    m_keyholders.erase(kp);
}

bool
hyperdaemon :: replication_manager :: from_disk(const regionid& r,
                                             const e::buffer& key,
                                             bool* has_value,
                                             std::vector<e::buffer>* value,
                                             uint64_t* version)
{
    switch (m_data->get(r, key, value, version))
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
        case hyperdisk::HASHFULL:
        case hyperdisk::DATAFULL:
        case hyperdisk::SEARCHFULL:
        case hyperdisk::SYNCFAILED:
        case hyperdisk::DROPFAILED:
        default:
            LOG(WARNING) << "Data layer returned unexpected result when reading old value.";
            return false;
    }
}

bool
hyperdaemon :: replication_manager :: put_to_disk(const regionid& pending_in,
                                                  const e::buffer& key,
                                                  e::intrusive_ptr<replication::keyholder> kh,
                                                  uint64_t version)
{
    std::map<uint64_t, e::intrusive_ptr<pending> >::iterator to_put_to_disk;
    to_put_to_disk = kh->pending_updates.find(version);

    if (to_put_to_disk == kh->pending_updates.end())
    {
        return false;
    }

    e::intrusive_ptr<pending> update = to_put_to_disk->second;

    // We don't have to mark others as being true as the way in which we set
    // ondisk guarantees that all lesser versions are ondisk as well.
    if (update->ondisk)
    {
        return true;
    }

    bool success = true;

    if (!update->has_value || (update->this_old != update->this_new && pending_in == update->this_old))
    {
        switch (m_data->del(pending_in, key))
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
            case hyperdisk::HASHFULL:
                LOG(ERROR) << "HASHFULL returned when committing to disk.";
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
            default:
                LOG(ERROR) << "unknown error when committing to disk.";
                success = false;
                break;
        }
    }
    else if (update->has_value)
    {
        switch (m_data->put(pending_in, key, update->value, version))
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
            case hyperdisk::HASHFULL:
                LOG(ERROR) << "HASHFULL returned when committing to disk.";
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
            default:
                LOG(ERROR) << "unknown error when committing to disk.";
                success = false;
                break;
        }
    }

    std::map<uint64_t, e::intrusive_ptr<pending> >::iterator penditer;

    for (penditer = kh->pending_updates.begin();
         success && penditer != kh->pending_updates.end() && penditer->first <= version;
         ++penditer)
    {
        penditer->second->ondisk = true;
    }

    return success;
}

size_t
hyperdaemon :: replication_manager :: expected_dimensions(const regionid& ri) const
{
    size_t dims = 0;
    return m_config.dimensionality(ri.get_space(), &dims) ? dims : -1;
}

bool
hyperdaemon :: replication_manager :: prev_and_next(const regionid& r,
                                                    const e::buffer& key,
                                                    bool has_newvalue,
                                                    const std::vector<e::buffer>& newvalue,
                                                    bool has_oldvalue,
                                                    const std::vector<e::buffer>& oldvalue,
                                                    e::intrusive_ptr<replication::pending> pend)
{
    using hyperdex::hyperspace::replication_point;
    size_t subspaces;
    uint16_t prev_subspace;
    uint16_t next_subspace;
    std::vector<bool> prev_dims;
    std::vector<bool> this_dims;
    std::vector<bool> next_dims;

    if (!m_config.subspaces(r.get_space(), &subspaces))
    {
        return false;
    }

    // Discover the dimensions of the space which correspond to the
    // subspace in which "to" is located, and the subspaces before and
    // after it.
    prev_subspace = r.subspace > 0 ? r.subspace - 1 : subspaces - 1;
    next_subspace = r.subspace < subspaces - 1 ? r.subspace + 1 : 0;

    if (!m_config.dimensions(r.get_subspace(), &this_dims)
        || !m_config.dimensions(hyperdex::subspaceid(r.space, prev_subspace), &prev_dims)
        || !m_config.dimensions(hyperdex::subspaceid(r.space, next_subspace), &next_dims))
    {
        return false;
    }

    uint64_t newkey_hash;
    uint64_t oldkey_hash;
    std::vector<uint64_t> newvalue_hashes;
    std::vector<uint64_t> oldvalue_hashes;

    if (has_newvalue)
    {
        hyperdex::hyperspace::point_hashes(key, newvalue, &newkey_hash, &newvalue_hashes);
    }

    if (has_oldvalue)
    {
        hyperdex::hyperspace::point_hashes(key, oldvalue, &oldkey_hash, &oldvalue_hashes);
    }

    if (has_oldvalue && has_newvalue)
    {
        pend->prev = regionid(r.space, prev_subspace, 64, replication_point(newkey_hash, newvalue_hashes, prev_dims));
        pend->this_old = regionid(r.get_subspace(), 64, replication_point(oldkey_hash, oldvalue_hashes, this_dims));
        pend->this_new = regionid(r.get_subspace(), 64, replication_point(newkey_hash, newvalue_hashes, this_dims));
        pend->next = regionid(r.space, next_subspace, 64, replication_point(oldkey_hash, oldvalue_hashes, next_dims));
    }
    else if (has_oldvalue)
    {
        pend->prev = regionid(r.space, prev_subspace, 64, replication_point(oldkey_hash, oldvalue_hashes, prev_dims));
        pend->this_old = regionid(r.get_subspace(), 64, replication_point(oldkey_hash, oldvalue_hashes, this_dims));
        pend->this_new = pend->this_old;
        pend->next = regionid(r.space, next_subspace, 64, replication_point(oldkey_hash, oldvalue_hashes, next_dims));
    }
    else if (has_newvalue)
    {
        pend->prev = regionid(r.space, prev_subspace, 64, replication_point(newkey_hash, newvalue_hashes, prev_dims));
        pend->this_old = regionid(r.get_subspace(), 64, replication_point(newkey_hash, newvalue_hashes, this_dims));
        pend->this_new = pend->this_old;
        pend->next = regionid(r.space, next_subspace, 64, replication_point(newkey_hash, newvalue_hashes, next_dims));
    }
    else
    {
        return false;
    }

    // A 'pending' object will never jump to another region, so if this_old or
    // this_new intersects with the region now, it will forever.  To simplify
    // logic elsewhere, we just set them to 'r' if they intersect so that
    // elsewhere we can use equality tests of this_old and this_new.
    if (overlap(r, pend->this_old))
    {
        pend->this_old = r;
    }

    if (overlap(r, pend->this_new))
    {
        pend->this_new = r;
    }

    return true;
}

void
hyperdaemon :: replication_manager :: unblock_messages(const regionid& r,
                                                       const e::buffer& key,
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
                                                               const e::buffer& key,
                                                               e::intrusive_ptr<keyholder> kh)
{
    while (!kh->deferred_updates.empty())
    {
        const uint64_t version = kh->deferred_updates.begin()->first;
        deferred& defrd(*kh->deferred_updates.begin()->second);

        // Figure out what to do with the updateI
        const uint64_t oldversion = version - 1;
        bool has_oldvalue = false;
        std::vector<e::buffer> oldvalue;
        std::map<uint64_t, e::intrusive_ptr<pending> >::iterator smallest_pending;
        std::map<uint64_t, e::intrusive_ptr<pending> >::iterator olditer;
        smallest_pending = kh->pending_updates.begin();

        // If nothing is pending.
        if (smallest_pending == kh->pending_updates.end())
        {
            // We know that "move_deferred_to_pending" will only be called when
            // something has just been added to pending.  Thus, this case should
            // not happen.
            LOG(ERROR) << "There is a programming error in \"move_deferred_to_pending\".";
            kh->deferred_updates.clear();
            return;
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
        newpend = new pending(defrd.has_value, defrd.value);

        if (!prev_and_next(to.get_region(), key, defrd.has_value, defrd.value,
                           has_oldvalue, oldvalue, newpend))
        {
            kh->deferred_updates.erase(kh->deferred_updates.begin());
            continue;
        }

        if (!(defrd.from_ent.get_region() == newpend->this_old &&
              sent_forward_or_from_tail(defrd.from_ent, to, newpend->this_old, newpend->prev)))
        {
            kh->deferred_updates.erase(kh->deferred_updates.begin());
            continue;
        }

        // We may ack this if we are not subspace 0.
        newpend->mayack = to.subspace != 0;

        kh->pending_updates.insert(std::make_pair(version, newpend));
        send_update(to.get_region(), version, key, newpend);
    }
}

void
hyperdaemon :: replication_manager :: send_update(const hyperdex::regionid& pending_in,
                                                  uint64_t version, const e::buffer& key,
                                                  e::intrusive_ptr<pending> update)
{
    assert(pending_in == update->this_old || pending_in == update->this_new);

    if (update->this_old == update->this_new)
    {
        uint8_t fresh = update->fresh ? 1 : 0;
        e::buffer msg;
        e::packer pack(&msg);
        pack << version << fresh << key;
        network_msgtype type;

        if (update->has_value)
        {
            pack << update->value;
            type = hyperdex::CHAIN_PUT;
        }
        else
        {
            type = hyperdex::CHAIN_DEL;
        }

        e::buffer info;
        info.pack() << version << key;

        if (update->next.subspace == 0)
        {
            // If we there is someone else in the chain, send the PUT or DEL pending
            // message to them.  If not, then send a PENDING message to the tail of
            // the first subspace.
            m_comm->send_forward_else_tail(pending_in, type, msg, update->next, hyperdex::CHAIN_PENDING, info);
        }
        else
        {
            m_comm->send_forward_else_head(pending_in, type, msg, update->next, type, msg);
        }

        if (update->this_old.subspace == 0 && update->mayack)
        {
            m_comm->send_backward(pending_in, hyperdex::CHAIN_PENDING, info);
        }
    }
    else if (pending_in == update->this_old)
    {
        assert(update->has_value);
        uint8_t fresh = update->fresh ? 1 : 0;
        e::buffer oldmsg;
        oldmsg.pack() << version << fresh << key << update->value;
        e::buffer newmsg;
        newmsg.pack() << version << key << update->value << update->next.mask;
        m_comm->send_forward_else_head(update->this_old, hyperdex::CHAIN_PUT, oldmsg,
                                       update->this_new, hyperdex::CHAIN_SUBSPACE, newmsg);
    }
    else if (pending_in == update->this_new)
    {
        assert(update->has_value);
        uint8_t fresh = update->fresh ? 1 : 0;
        e::buffer oldmsg;
        oldmsg.pack() << version << key << update->value << update->next.mask;
        e::buffer newmsg;
        newmsg.pack() << version << fresh << key << update->value;
        m_comm->send_forward_else_head(update->this_new, hyperdex::CHAIN_SUBSPACE, oldmsg,
                                       update->next, hyperdex::CHAIN_PUT, newmsg);
    }
    else
    {
        LOG(INFO) << "There is a case for \"send_update\" which is not handled.";
    }
}

void
hyperdaemon :: replication_manager :: send_ack(const regionid& pending_in,
                                               uint64_t version,
                                               const e::buffer& key,
                                               e::intrusive_ptr<pending> update)
{
    e::buffer msg;
    msg.pack() << version << key;

    if (pending_in == update->this_old)
    {
        m_comm->send_backward_else_tail(pending_in, hyperdex::CHAIN_ACK, msg,
                                        update->prev, hyperdex::CHAIN_ACK, msg);
    }
    else if (pending_in == update->this_new)
    {
        m_comm->send_backward_else_tail(pending_in, hyperdex::CHAIN_ACK, msg,
                                        update->this_old, hyperdex::CHAIN_ACK, msg);
    }
}

void
hyperdaemon :: replication_manager :: send_ack(const regionid& from,
                                               const entityid& to,
                                               const e::buffer& key,
                                               uint64_t version)
{
    e::buffer msg;
    msg.pack() << version << key;
    m_comm->send(from, to, hyperdex::CHAIN_ACK, msg);
}

void
hyperdaemon :: replication_manager :: respond_to_client(clientop co,
                                                        network_msgtype type,
                                                        network_returncode ret)
{
    e::buffer msg(5);
    uint16_t result = static_cast<uint16_t>(ret);
    msg.pack() << co.nonce << result;
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

        try
        {
            // Every second.
            if (i % 4 == 0)
            {
                start_transfers();
            }
        }
        catch (std::exception& e)
        {
            LOG(INFO) << "Uncaught exception when starting transfers: " << e.what();
        }

        try
        {
            // Every ten seconds.
            if (i % 40 == 0)
            {
                finish_transfers();
            }
        }
        catch (std::exception& e)
        {
            LOG(INFO) << "Uncaught exception when finishing transfers: " << e.what();
        }

        e::sleep_ms(250);
    }
}

void
hyperdaemon :: replication_manager :: retransmit()
{
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
        e::intrusive_ptr<keyholder> kh = get_keyholder(*kp);

        if (!kh)
        {
            continue;
        }

        if (kh->pending_updates.empty())
        {
            unblock_messages(kp->region, kp->key, kh);
        }

        if (kh->pending_updates.empty())
        {
            erase_keyholder(*kp);
            continue;
        }

        uint64_t version = kh->pending_updates.begin()->first;
        e::intrusive_ptr<pending> pend = kh->pending_updates.begin()->second;

        send_update(kp->region, version, kp->key, pend);

        if (kp->region.subspace == 0)
        {
            e::buffer info;
            info.pack() << version << kp->key;
            m_comm->send_backward(kp->region, hyperdex::CHAIN_PENDING, info);
        }
    }
}

void
hyperdaemon :: replication_manager :: start_transfers()
{
    for (std::map<uint16_t, e::intrusive_ptr<transfer_in> >::iterator t = m_transfers_in.begin();
            t != m_transfers_in.end(); ++t)
    {
        if (!t->second->started)
        {
            e::buffer msg;

            for (int i = 0; i < TRANSFERS_IN_FLIGHT; ++i)
            {
                m_comm->send(t->second->transfer_entity, t->second->replicate_from, hyperdex::XFER_MORE, msg);
            }
        }
    }
}

void
hyperdaemon :: replication_manager :: finish_transfers()
{
    for (std::map<uint16_t, e::intrusive_ptr<transfer_in> >::iterator t = m_transfers_in.begin();
            t != m_transfers_in.end(); ++t)
    {
        if (t->second->go_live)
        {
            e::buffer msg;
            m_comm->send(t->second->transfer_entity, t->second->replicate_from, hyperdex::XFER_MORE, msg);
        }
    }
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
