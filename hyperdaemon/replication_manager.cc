// Copyright (c) 2011-2012, Cornell University
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
#include <cstdio>

// STL
#include <algorithm>
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
#include "hyperdex/hyperdex/coordinatorlink.h"
#include "hyperdex/hyperdex/microop.h"
#include "hyperdex/hyperdex/network_constants.h"
#include "hyperdex/hyperdex/packing.h"

// HyperDaemon
#include "hyperdaemon/datalayer.h"
#include "hyperdaemon/datatypes.h"
#include "hyperdaemon/logical.h"
#include "hyperdaemon/ongoing_state_transfers.h"
#include "hyperdaemon/replication_manager.h"
#include "hyperdaemon/replication_manager_deferred.h"
#include "hyperdaemon/replication_manager_keyholder.h"
#include "hyperdaemon/replication_manager_pending.h"
#include "hyperdaemon/runtimeconfig.h"
#include "hyperspacehashing/hashes_internal.h"

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
    , m_keyholders_lock()
    , m_keyholders(REPLICATION_HASHTABLE_SIZE)
    , m_us()
    , m_quiesce(false)
    , m_quiesce_state_id_lock()
    , m_quiesce_state_id("")
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
    // Mark as quiescing if config says so.
    if (newconfig.quiesce())
    {
        po6::threads::mutex::hold hold(&m_quiesce_state_id_lock);

        // OK to see multiple quiesce requests - we will take new id each time, but we 
        // cannot go back to normal operation without shutdown.
        m_quiesce_state_id = newconfig.quiesce_state_id();
        m_quiesce = true;
    }

    // Install a new configuration.
    m_config = newconfig;
    m_us = us;
    po6::threads::mutex::hold hold(&m_keyholders_lock);

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

static bool
unpack_attributes(const std::vector<std::pair<uint16_t, e::slice> >& value,
                  const std::vector<hyperdex::attribute>& dims,
                  e::bitfield *bf,
                  std::vector<e::slice> *realvalue)
{
    using namespace hyperdaemon;

    for (size_t i = 0; i < value.size(); ++i)
    {
        if (value[i].first == 0 || value[i].first >= dims.size())
        {
            return false;
        }

        if (!validate_datatype(dims[value[i].first].type, value[i].second))
        {
            return false;
        }

        (*realvalue)[value[i].first - 1] = value[i].second;
        (*bf).set(value[i].first - 1);
    }
    return true;
}

void
hyperdaemon :: replication_manager :: client_put(const hyperdex::entityid& from,
                                                 const hyperdex::entityid& to,
                                                 uint64_t nonce,
                                                 std::auto_ptr<e::buffer> backing,
                                                 const e::slice& key,
                                                 const std::vector<std::pair<uint16_t, e::slice> >& value)
{
    // Fail as read only if we are quiescing. 
    if (m_quiesce)
    {
        respond_to_client(to, from, nonce,
                          hyperdex::RESP_PUT,
                          hyperdex::NET_READONLY);
        return;
    }

    std::vector<hyperdex::attribute> dims = m_config.dimension_names(to.get_space());
    assert(dims.size() > 0);
    e::bitfield bf(dims.size() - 1);
    std::vector<e::slice> realvalue(dims.size() - 1);
    e::bitfield condbf(dims.size() - 1);
    std::vector<e::slice> condvalue(dims.size() - 1);

    if (!validate_datatype(dims[0].type, key))
    {
        respond_to_client(to, from, nonce,
                          hyperdex::RESP_PUT,
                          hyperdex::NET_BADDIMSPEC);
        return;
    }

    if (!unpack_attributes(value, dims, &bf, &realvalue))
    {
        respond_to_client(to, from, nonce,
                          hyperdex::RESP_PUT,
                          hyperdex::NET_BADDIMSPEC);
        return;
    }

    client_common(hyperdex::RESP_PUT, true, from, to, nonce, backing, key, condbf, condvalue, bf, realvalue);
}

void
hyperdaemon :: replication_manager :: client_condput(const hyperdex::entityid& from,
                                                     const hyperdex::entityid& to,
                                                     uint64_t nonce,
                                                     std::auto_ptr<e::buffer> backing,
                                                     const e::slice& key,
                                                     const std::vector<std::pair<uint16_t, e::slice> >& condfields,
                                                     const std::vector<std::pair<uint16_t, e::slice> >& value)
{
    // Fail as read only if we are quiescing.
    if (m_quiesce)
    {
        respond_to_client(to, from, nonce,
                          hyperdex::RESP_PUT,
                          hyperdex::NET_READONLY);
        return;
    }

    std::vector<hyperdex::attribute> dims = m_config.dimension_names(to.get_space());
    assert(dims.size() > 0);
    e::bitfield condbf(dims.size() - 1);
    std::vector<e::slice> condvalue(dims.size() - 1);
    e::bitfield bf(dims.size() - 1);
    std::vector<e::slice> realvalue(dims.size() - 1);

    if (!validate_datatype(dims[0].type, key))
    {
        respond_to_client(to, from, nonce,
                          hyperdex::RESP_CONDPUT,
                          hyperdex::NET_BADDIMSPEC);
        return;
    }

    if (!unpack_attributes(condfields, dims, &condbf, &condvalue) ||
        !unpack_attributes(value, dims, &bf, &realvalue))
    {
        respond_to_client(to, from, nonce,
                          hyperdex::RESP_CONDPUT,
                          hyperdex::NET_BADDIMSPEC);
        return;
    }

    client_common(hyperdex::RESP_CONDPUT, true, from, to, nonce, backing, key, condbf, condvalue, bf, realvalue);
}

void
hyperdaemon :: replication_manager :: client_del(const entityid& from,
                                                 const entityid& to,
                                                 uint64_t nonce,
                                                 std::auto_ptr<e::buffer> backing,
                                                 const e::slice& key)
{
    // Fail as read only if we are quiescing.
    if (m_quiesce)
    {
        respond_to_client(to, from, nonce,
                          hyperdex::RESP_DEL,
                          hyperdex::NET_READONLY);
        return;
    }

    size_t dims = m_config.dimensions(to.get_space());
    assert(dims > 0);
    e::bitfield b(dims - 1);
    std::vector<e::slice> v(dims - 1);
    e::bitfield condbf(dims - 1);
    std::vector<e::slice> condvalue(dims - 1);

    client_common(hyperdex::RESP_DEL, false, from, to, nonce, backing, key, condbf, condvalue, b, v);
}

void
hyperdaemon :: replication_manager :: client_atomic(const hyperdex::entityid& from,
                                                    const hyperdex::entityid& to,
                                                    uint64_t nonce,
                                                    std::auto_ptr<e::buffer> /*backing*/,
                                                    const e::slice& key,
                                                    std::vector<hyperdex::microop>* ops)
{
    // Fail as read only if we are quiescing.
    if (m_quiesce)
    {
        respond_to_client(to, from, nonce,
                          hyperdex::RESP_ATOMIC,
                          hyperdex::NET_READONLY);
        return;
    }

    std::vector<hyperdex::attribute> dims = m_config.dimension_names(to.get_space());
    assert(!dims.empty());

    if (!validate_datatype(dims[0].type, key))
    {
        respond_to_client(to, from, nonce,
                          hyperdex::RESP_ATOMIC,
                          hyperdex::NET_BADDIMSPEC);
        return;
    }

    // Make sure this message is from a client.
    if (from.space != UINT32_MAX)
    {
        LOG(INFO) << "dropping client-only message from " << from << " (it is not a client).";
        return;
    }

    clientop co(to.get_region(), from, nonce);

    // Make sure this message is to the point-leader.
    if (!m_config.is_point_leader(to))
    {
        respond_to_client(to, from, nonce, hyperdex::RESP_ATOMIC, hyperdex::NET_NOTUS);
        return;
    }

    // Automatically respond with "SERVERERROR" whenever we return without g.dismiss()
    e::guard g = e::makeobjguard(*this, &replication_manager::respond_to_client, to, from, nonce, hyperdex::RESP_ATOMIC, hyperdex::NET_SERVERERROR);

    // Grab the lock that protects this key.
    HOLD_LOCK_FOR_KEY(to, key);
    // Get the keyholder for this key.
    e::intrusive_ptr<keyholder> kh = get_keyholder(to.get_region(), key);

    // Find the pending or committed version with the largest number.
    uint64_t old_version = 0;
    bool has_old_value = false;
    std::vector<e::slice> old_value;
    hyperdisk::reference ref;

    if (kh->has_blocked_ops())
    {
        old_version = kh->most_recent_blocked_version();
        has_old_value = kh->most_recent_blocked_op()->has_value;
        old_value = kh->most_recent_blocked_op()->value;
    }
    else if (kh->has_committable_ops())
    {
        old_version = kh->most_recent_committable_version();
        has_old_value = kh->most_recent_committable_op()->has_value;
        old_value = kh->most_recent_committable_op()->value;
    }
    else if (!from_disk(to.get_region(), key, &has_old_value, &old_value, &old_version, &ref))
    {
        return;
    }

    // We allow "atomic" if and only if it already exists.
    if (!has_old_value)
    {
        // an atomic increment on an object that does not exist should fail
        respond_to_client(to, from, nonce, hyperdex::RESP_ATOMIC, hyperdex::NET_NOTFOUND);
        g.dismiss();
        return;
    }

    // If the atomic op does nothing, just act as if it was successful.
    if (ops->empty())
    {
        respond_to_client(to, from, nonce,
                          hyperdex::RESP_ATOMIC,
                          hyperdex::NET_SUCCESS);
        g.dismiss();
        return;
    }

    // We make an unvalidated assumption that the ops array is sorted.  We will
    // validate this at a later point.
    if (ops->front().attr <= 0 || ops->back().attr >= dims.size())
    {
        respond_to_client(to, from, nonce,
                          hyperdex::RESP_ATOMIC,
                          hyperdex::NET_BADDIMSPEC);
        g.dismiss();
        return;
    }

    // Calculate the new size of the object
    size_t new_size = key.size();

    for (size_t i = 0; i < old_value.size(); ++i)
    {
        // Keep the current size
        new_size += old_value[i].size();
    }

    for (size_t i = 0; i < ops->size(); ++i)
    {
        // Increment by the amount required by the microop
        new_size += sizeof_microop((*ops)[i]);
    }

    // Allocate the new buffer
    std::tr1::shared_ptr<e::buffer> new_backing(e::buffer::create(new_size));
    std::vector<e::slice> new_value(dims.size() - 1);

    // Copy the old data, mutating it as the micro ops require
    uint8_t* data = new_backing->data();

    // Copy the key.  Micro ops never apply.
    e::slice new_key = e::slice(new_backing->data(), key.size());
    memmove(data, key.data(), key.size());
    data += key.size();

    // Divide the micro ops up by attribute
    hyperdex::microop* op = &ops->front();
    const hyperdex::microop* const stop = &ops->front() + ops->size();
    // the next attribute to copy, indexed based on the total number of
    // dimensions.  It starts at 1, because the key is 0, and 1 is the first
    // secondary attribute.
    uint16_t next_to_copy = 1;

    while (op < stop)
    {
        hyperdex::microop* end = op;

        // If the ops are out of order, or out of bounds
        if (op->attr < next_to_copy || op->attr >= dims.size())
        {
            // Fail it for bad micro ops
            respond_to_client(to, from, nonce, hyperdex::RESP_ATOMIC, hyperdex::NET_BADMICROS);
            g.dismiss();
            return;
        }

        // Advance so that op/end point to the micro ops that need to be applied
        while (end < stop && op->attr == end->attr)
        {
            // If the datatype doesn't match the structure
            if (end->type != dims[end->attr].type ||
                    end->action == hyperdex::OP_FAIL)
            {
                // Fail it for bad micro ops
                respond_to_client(to, from, nonce, hyperdex::RESP_ATOMIC, hyperdex::NET_BADMICROS);
                g.dismiss();
                return;
            }

            ++end;
        }

        // Copy the attributes that are unaffected by micro ops.
        while (next_to_copy < op->attr)
        {
            assert(next_to_copy > 0);
            size_t idx = next_to_copy - 1;
            memmove(data, old_value[idx].data(), old_value[idx].size());
            new_value[idx] = e::slice(data, old_value[idx].size());
            data += old_value[idx].size();
            ++next_to_copy;
        }

        // - "op" now points to the first unapplied micro op
        // - "end" points to one micro op past the last op we want to apply
        // - we've guaranteed that the micro ops thus far are in sorted order
        // - we've guaranteed that the micro ops thus far match the declared
        //   types
        // - we've copied all attributes so far, even those not mentioned by
        //   micro ops.

        hyperdex::network_returncode error;
        // This call may modify [op, end) ops.
        uint8_t* newdata = apply_microops(dims[op->attr].type,
                                          old_value[op->attr - 1],
                                          op, end - op,
                                          data, &error);

        // If applying the micro ops failed
        if (!newdata)
        {
            // Fail it for bad micro ops
            respond_to_client(to, from, nonce, hyperdex::RESP_ATOMIC, error);
            g.dismiss();
            return;
        }

        // see message above "yes I did"
        new_value[next_to_copy - 1] = e::slice(data, newdata - data);
        data = newdata;

        // Why ++ and assert rather than straight assign?  This will help us to
        // catch any problems in the interaction between micro ops and
        // attributes which we just copy.
        assert(next_to_copy == op->attr);
        ++next_to_copy;

        op = end;
    }

    // Copy the attributes that are unaffected by micro ops.
    while (next_to_copy < dims.size())
    {
        assert(next_to_copy > 0);
        size_t idx = next_to_copy - 1;
        memmove(data, old_value[idx].data(), old_value[idx].size());
        new_value[idx] = e::slice(data, old_value[idx].size());
        data += old_value[idx].size();
        ++next_to_copy;
    }

    e::intrusive_ptr<pending> new_pend;
    new_pend = new pending(true, new_backing, key, new_value, co);
    new_pend->retcode = hyperdex::RESP_ATOMIC;
    new_pend->ref = ref;
    new_pend->key = new_key;

    if (!prev_and_next(to.get_region(), new_pend->key, true, new_pend->value, has_old_value, old_value, new_pend))
    {
        respond_to_client(to, from, nonce, hyperdex::RESP_ATOMIC, hyperdex::NET_NOTUS);
        g.dismiss();
        return;
    }

    assert(!kh->has_deferred_ops());
    kh->append_blocked(old_version + 1, new_pend);
    move_operations_between_queues(to, key, kh);
    g.dismiss();
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
    newpend->recv_e = from;
    newpend->recv_i = m_config.instancefor(from);
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
                                                std::auto_ptr<e::buffer> backing,
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

    if (pend->sent_e == entityid())
    {
        LOG(INFO) << "dropping CHAIN_ACK for update we haven't sent";
        return;
    }

    if (from != pend->sent_e)
    {
        LOG(INFO) << "dropping CHAIN_ACK that came from the wrong host";
        return;
    }

    std::tr1::shared_ptr<e::buffer> shared_backing(backing.release());
    m_ost->add_trigger(to.get_region(), shared_backing, key, version);
    pend->acked = true;
    put_to_disk(to.get_region(), kh, version);

    while (kh->has_committable_ops()
            && kh->oldest_committable_op()->acked)
    {
        kh->remove_oldest_committable_op();
    }

    move_operations_between_queues(to, key, kh);

    if (m_config.is_point_leader(to))
    {
        if (pend->co.from.space == UINT32_MAX)
        {
            respond_to_client(to, pend->co.from, pend->co.nonce,
                              pend->retcode, hyperdex::NET_SUCCESS);
            pend->co = clientop();
        }
    }
    else
    {
        send_ack(to, pend->recv_e, version, key);
    }

    if (kh->empty())
    {
        erase_keyholder(to.get_region(), key);
    }
}

void
hyperdaemon :: replication_manager :: client_common(const hyperdex::network_msgtype opcode,
                                                    const bool has_value,
                                                    const entityid& from,
                                                    const entityid& to,
                                                    uint64_t nonce,
                                                    std::auto_ptr<e::buffer> backing,
                                                    const e::slice& key,
                                                    const e::bitfield& condvalue_mask,
                                                    const std::vector<e::slice>& condvalue,
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
    hyperdex::network_msgtype retcode = opcode;

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
    uint64_t old_version = 0;
    bool has_old_value = false;
    std::vector<e::slice> old_value;
    hyperdisk::reference ref;

    if (kh->has_blocked_ops())
    {
        old_version = kh->most_recent_blocked_version();
        has_old_value = kh->most_recent_blocked_op()->has_value;
        old_value = kh->most_recent_blocked_op()->value;
    }
    else if (kh->has_committable_ops())
    {
        old_version = kh->most_recent_committable_version();
        has_old_value = kh->most_recent_committable_op()->has_value;
        old_value = kh->most_recent_committable_op()->value;
    }
    else if (!from_disk(to.get_region(), key, &has_old_value, &old_value, &old_version, &ref))
    {
        return;
    }

    e::intrusive_ptr<pending> new_pend;
    std::tr1::shared_ptr<e::buffer> sharedbacking(backing.release());
    new_pend = new pending(has_value, sharedbacking, key, value, co);
    new_pend->retcode = retcode;
    new_pend->ref = ref;

    if (!has_value && !has_old_value)
    {
        respond_to_client(to, from, nonce, retcode, hyperdex::NET_NOTFOUND);
        g.dismiss();
        return;
    }

    if (has_value && !has_old_value)
    {
        if (opcode == hyperdex::RESP_CONDPUT)
        {
            // a conditional put or atomic inc/dec on an object that does not exist should fail
            respond_to_client(to, from, nonce, retcode, hyperdex::NET_NOTFOUND);
            g.dismiss();
            return;
        }

        new_pend->fresh = true;
    }

    std::tr1::shared_ptr<e::buffer> old_backing = new_pend->backing;

    if (has_value && has_old_value)
    {
        size_t need_moar = 0;

        for (size_t i = 0; i < value.size(); ++i)
        {
            if (!value_mask.get(i))
            {
                need_moar += old_value[i].size();
            }
        }

        if (need_moar)
        {
#define REBASE(X) \
            ((X) - new_pend->backing->data() + new_backing->data())
            std::tr1::shared_ptr<e::buffer> new_backing(e::buffer::create(new_pend->backing->size() + need_moar));
            new_backing->resize(new_pend->backing->size() + need_moar);
            memmove(new_backing->data(), new_pend->backing->data(), new_pend->backing->size());
            e::slice oldkey = new_pend->key;
            new_pend->key = e::slice(REBASE(new_pend->key.data()), new_pend->key.size());
            assert(oldkey == new_pend->key);
            size_t curdata = new_pend->backing->size();

            for (size_t i = 0; i < value.size(); ++i)
            {
                if (value_mask.get(i))
                {
                    e::slice oldslice = new_pend->value[i];
                    new_pend->value[i] = e::slice(REBASE(new_pend->value[i].data()), new_pend->value[i].size());
                    assert(oldslice == new_pend->value[i]);
                }
                else
                {
                    e::slice oldslice = old_value[i];
                    memmove(new_backing->data() + curdata, old_value[i].data(), old_value[i].size());
                    new_pend->value[i] = e::slice(new_backing->data() + curdata, old_value[i].size());
                    curdata += old_value[i].size();
                    assert(oldslice == new_pend->value[i]);
                }
            }

            assert(curdata == new_pend->backing->size() + need_moar);
            new_pend->backing = new_backing;
        }

        if (opcode == hyperdex::RESP_CONDPUT)
        {
          for (size_t i = 0; i < condvalue.size(); ++i)
          {
              if (condvalue_mask.get(i) && (old_value[i] != condvalue[i]))
              {
                // cond value mismatch, put operation should fail
                respond_to_client(to, from, nonce, retcode, hyperdex::NET_CMPFAIL);
                g.dismiss();
                return;
              }
          }
        }
    }

    if (!prev_and_next(to.get_region(), new_pend->key, has_value, new_pend->value, has_old_value, old_value, new_pend))
    {
        respond_to_client(to, from, nonce, retcode, hyperdex::NET_NOTUS);
        g.dismiss();
        return;
    }

    assert(!kh->has_deferred_ops());
    kh->append_blocked(old_version + 1, new_pend);
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
        newop->recv_e = from;
        newop->recv_i = m_config.instancefor(from);
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
    newpend->recv_e = from;
    newpend->recv_i = m_config.instancefor(from);

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
hyperdaemon :: replication_manager :: check_for_deferred_operations(const hyperdex::regionid& r,
                                                                    uint64_t version,
                                                                    std::tr1::shared_ptr<e::buffer> /*backing*/,
                                                                    const e::slice& key,
                                                                    bool has_value,
                                                                    const std::vector<e::slice>& value)
{
    // Get the keyholder for this key.
    e::intrusive_ptr<keyholder> kh = get_keyholder(r, key);

    entityid us = m_config.entityfor(m_us, r);

    // If this is empty, it means we've not been integrated into the chain, and
    // the race condition we check for cannot exist.
    if (us == entityid())
    {
        return;
    }

    if (kh->has_deferred_ops() && version + 1 == kh->oldest_deferred_version())
    {
        // Create a new pending object to set as pending.
        e::intrusive_ptr<deferred> op = kh->oldest_deferred_op();
        e::intrusive_ptr<pending> newop;
        newop = new pending(op->has_value, op->backing, op->key, op->value);
        newop->fresh = false;
        newop->ref = op->ref;
        newop->recv_e = op->from_ent;
        newop->recv_i = m_config.instancefor(op->from_ent);

        if (!prev_and_next(us.get_region(), key, newop->has_value, newop->value, has_value, value, newop))
        {
            LOG(ERROR) << "errror checking for deferred operations";
            return;
        }

        if (!(newop->recv_e.get_region() == r && m_config.chain_adjacent(newop->recv_e, us))
                && !(newop->recv_e.space == r.space
                    && newop->recv_e.subspace + 1 == r.subspace
                    && m_config.is_tail(newop->recv_e)
                    && m_config.is_head(us)))
        {
            LOG(INFO) << "dropping deferred CHAIN_* which didn't come from the right host";
            return;
        }

        kh->append_blocked(kh->oldest_deferred_version(), newop);
        kh->remove_oldest_deferred_op();
    }

    move_operations_between_queues(us, key, kh);
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
        newop->recv_e = op->from_ent;
        newop->recv_i = m_config.instancefor(op->from_ent);

        if (!prev_and_next(us.get_region(), key, newop->has_value, newop->value, oldop->has_value, oldop->value, newop))
        {
            LOG(INFO) << "dropping deferred CHAIN_* which does not match this host";
            return;
        }

        if (!(newop->recv_e.get_region() == us.get_region() && m_config.chain_adjacent(newop->recv_e, us))
                && !(newop->recv_e.space == us.space
                    && newop->recv_e.subspace + 1 == us.subspace
                    && m_config.is_tail(newop->recv_e)
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
    if (op->sent_e != entityid())
    {
        return;
    }

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
                op->sent_e = us;
                op->sent_i = m_us;
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
                op->sent_e = dst;
                op->sent_i = m_config.instancefor(dst);
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
                op->sent_e = dst;
                op->sent_i = m_config.instancefor(dst);
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
        op->sent_e = dst;
        op->sent_i = m_config.instancefor(dst);
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
            int processed = retransmit();
            
            // While quiescing, if all replication-related state is cleaned up, 
            // we are truly queisced.
            if (m_quiesce && processed <= 0)
            {
                // Lock needed to access quiesce state.
                po6::threads::mutex::hold hold(&m_quiesce_state_id_lock);
                
                // Let the coordinator know replication is quiesced.
                // XXX error handling?
                m_cl->quiesced(m_quiesce_state_id);
                
                // We are quiesced, there will be no more retransmits,
                // stop the periodic thread.
                LOG(INFO) << "Replication manager quiesced, periodic thread stopping.";
                break;
            }
        }
        catch (std::exception& e)
        {
            LOG(INFO) << "Uncaught exception when retransmitting: " << e.what();
        }

        e::sleep_ms(250);
    }
}

int
hyperdaemon :: replication_manager :: retransmit()
{
    int processed = 0;
    
    for (keyholder_map_t::iterator khiter = m_keyholders.begin();
            khiter != m_keyholders.end(); khiter.next())
    {
        processed++;
        
        po6::threads::mutex::hold holdkh(&m_keyholders_lock);
        // Grab the lock that protects this object.
        e::slice key(khiter.key().key.data(), khiter.key().key.size());
        e::striped_lock<po6::threads::mutex>::hold hold(&m_locks, get_lock_num(khiter.key().region, key));

        // Grab some references.
        e::intrusive_ptr<keyholder> kh = khiter.value();

        if (kh->empty())
        {
            // If the keyholder is empty, we need to check to make sure that the
            // *same* keyholder is accessible via the hash table using
            // khiter.key().  If it is not, then we know that we are seeing
            // stale data when iterating.  If it is, the hold on m_locks above
            // will guarantee that we don't erase the keyholder some other
            // thread creates.
            e::intrusive_ptr<keyholder> check;

            if (m_keyholders.lookup(khiter.key(), &check) && check == kh)
            {
                m_keyholders.remove(khiter.key());
            }

            continue;
        }

        if (!kh->has_committable_ops())
        {
            continue;
        }

        // We only touch the first pending update.  If there is an issue which
        // requires retransmission, we shouldn't hit hosts with a number of
        // excess messages.
        e::intrusive_ptr<pending> pend = kh->oldest_committable_op();

        if (pend->sent_e == entityid() ||
            pend->sent_i != m_config.instancefor(pend->sent_e))
        {
            pend->sent_e = entityid();
            pend->sent_i = instance();
            entityid ent = m_config.entityfor(m_us, khiter.key().region);
            send_message(ent, kh->oldest_committable_version(), key, pend);
        }
    }
   
    return processed;
}
