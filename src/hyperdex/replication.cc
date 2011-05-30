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

// STL
#include <tr1/functional>

// Google Log
#include <glog/logging.h>

// e
#include <e/guard.h>

// HyperDex
#include <hyperdex/buffer.h>
#include <hyperdex/city.h>
#include <hyperdex/hyperspace.h>
#include <hyperdex/replication.h>
#include <hyperdex/stream_no.h>

static void
nop(uint64_t) {}

// Note:  Computing the four possible locations to send the data makes a
// heavy assumption that subspaces do not change.  If subspaces can
// change on the fly, this needs to change.

// Calculate the possible next entities for a point.  This ignores
// discussion of replicas.  The below cases assume we are replica 0
// or n (depending upon direction of message travel).  If we are
// not, then the options are trivially same region with entity num
// +- 1.
//
// If we are subspace i, the cases are:
//  - op == DEL:
//    - We have an old value.  The messages go to subspace i - 1 and subspace i
//      + 1 and must go to the points mapped by the old value.
//    - We don't have an old value.  Tell the client "NOTFOUND".
//  - op == PUT:
//    - We have an old value.  The message may either go to the other point
//      within our subspace, or to the new point in subspace i - 1, or to
//      the old point in subspace i + 1.
//    - We don't have an old value.  The message may go to the new point in
//      the previous subspace or the new point in the next subspace.
//
// This leaves us with four points to track this all:
//
//  - prevsubspace:  This is point in the previous subspace.
//  - thisold:  This is the old point for this subspace.
//  - thisnew:  This is the new point for this subspace.
//  - nextsubspace:  This is the point in the next subspace.
//
// With just these four points, we can decide what to do when it comes time
// to retransmit a pending operation:
//      if this region (to) contains thisnew:
//          send to newsubspace
//      else if this region contains thisold:
//          send to thisnew
// A similar behavior occurs for sending the ACK message.
//      if this region (to) contains thisnew:
//          send to thisold
//      else if this region contains thisold:
//          send to prevsubspace
//
// We compute all four values here and then put them in the queue
// with the pending operation so that when it comes time to
// retransmit we can do so by finding the entity responsible for the
// dest point, and the entity within the region which matches to our
// instance.  Thus, the sending does not happen here, but happens
// elsewhere.

hyperdex :: replication :: replication(datalayer* data, logical* comm)
    : m_data(data)
    , m_comm(comm)
    , m_config()
    , m_lock()
    , m_keyholders_lock()
    , m_keyholders()
    , m_clientops_lock()
    , m_clientops()
{
}

void
hyperdex :: replication :: reconfigure(const configuration& config)
{
    po6::threads::mutex::hold hold(&m_lock);
    m_config = config;
    m_comm->remap(config.entity_mapping());
    // XXX Drop all pieces which we no longer are responsible for.
}

void
hyperdex :: replication :: drop(const regionid&)
{
    po6::threads::mutex::hold hold(&m_lock);
    // XXX maybe do it here.
}

void
hyperdex :: replication :: client_put(const entityid& from,
                                      const regionid& to,
                                      uint32_t nonce,
                                      const e::buffer& key,
                                      const std::vector<e::buffer>& newvalue)
{
    client_common(PUT, from, to, nonce, key, newvalue);
}

void
hyperdex :: replication :: client_del(const entityid& from,
                                      const regionid& to,
                                      uint32_t nonce,
                                      const e::buffer& key)
{
    client_common(DEL, from, to, nonce, key, std::vector<e::buffer>());
}

void
hyperdex :: replication :: chain_put(const entityid& from,
                                     const entityid& to,
                                     uint64_t newversion,
                                     bool fresh,
                                     const e::buffer& key,
                                     const std::vector<e::buffer>& newvalue)
{
    chain_common(PUT, from, to, newversion, fresh, key, newvalue);
}

void
hyperdex :: replication :: chain_del(const entityid& from,
                                     const entityid& to,
                                     uint64_t newversion,
                                     const e::buffer& key)
{
    chain_common(DEL, from, to, newversion, false, key, std::vector<e::buffer>());
}

void
hyperdex :: replication :: client_common(op_t op,
                                         const entityid& from,
                                         const regionid& to,
                                         uint32_t nonce,
                                         const e::buffer& key,
                                         const std::vector<e::buffer>& value)
{
    clientop co(to, from, nonce);
    keypair kp(to, key);
    // Automatically respond with "ERROR" whenever we return without
    // g.dismiss()
    e::guard g = e::makeobjguard(*this, &replication::respond_negatively_to_client, co, ERROR);
    m_clientops.insert(co);

    // Grab the lock that protects this keypair.
    po6::threads::mutex::hold hold(get_lock(kp));

    // Get a reference to the keyholder.
    e::intrusive_ptr<keyholder> kh = get_keyholder(kp);

    if (!kh)
    {
        return;
    }

    // If we have seen this (from, nonce) before and it is still active.
    if (have_seen_clientop(co))
    {
        // Silently ignore this message.
        g.dismiss();
        return;
    }

    // Check that a client's put matches the dimensions of the space.
    if (op == PUT && kh->expected_dimensions() != value.size() + 1)
    {
        // Tell the client they sent an invalid message.
        respond_negatively_to_client(co, INVALID);
        g.dismiss();
        return;
    }

    std::tr1::function<result_t (std::vector<e::buffer>*, uint64_t*)> read_from_disk;
    read_from_disk = std::tr1::bind(&datalayer::get, m_data, to, key, std::tr1::placeholders::_1, std::tr1::placeholders::_2);
    std::tr1::function<void (uint64_t)> notify;
    notify = std::tr1::bind(&hyperdex::replication::respond_positively_to_client,
                            this, co, std::tr1::placeholders::_1);

    if (op == PUT)
    {
        if (!kh->add_pending(read_from_disk, key, value, notify))
        {
            // Do not dismiss the guard.  This will automatically
            // trigger sending an error to the user.
            return;
        }
    }
    else
    {
        if (!kh->add_pending(read_from_disk, key, notify))
        {
            // Do not dismiss the guard.  This will automatically
            // trigger sending an error to the user.
            return;
        }
    }

    g.dismiss();
}

void
hyperdex :: replication :: chain_common(op_t op,
                                        const entityid& from,
                                        const entityid& to,
                                        uint64_t version,
                                        bool fresh,
                                        const e::buffer& key,
                                        const std::vector<e::buffer>& value)
{
    keypair kp(to.get_region(), key);

    // Grab the lock that protects this keypair.
    po6::threads::mutex::hold hold(get_lock(kp));

    // Get a reference to the keyholder.
    e::intrusive_ptr<keyholder> kh = get_keyholder(kp);

    if (!kh)
    {
        return;
    }

    // Check that a chains's put matches the dimensions of the space.
    if (op == PUT && kh->expected_dimensions() != value.size() + 1)
    {
        return;
    }

    std::tr1::function<result_t (std::vector<e::buffer>*, uint64_t*)> read_from_disk;
    read_from_disk = std::tr1::bind(&datalayer::get, m_data, to.get_region(), key, std::tr1::placeholders::_1, std::tr1::placeholders::_2);

    // XXX check here if we are a point leader, and if we are, then
    // initiate ACKs.
    if (op == PUT)
    {
        kh->add_pending(read_from_disk, from, to, version, fresh, key, value);
    }
    else
    {
        kh->add_pending(read_from_disk, from, to, version, key);
    }
}

po6::threads::mutex*
hyperdex :: replication :: get_lock(const keypair& /*kp*/)
{
    return &m_lock;
}

e::intrusive_ptr<hyperdex::replication::keyholder>
hyperdex :: replication :: get_keyholder(const keypair& kp)
{
    po6::threads::mutex::hold hold(&m_keyholders_lock);
    std::map<keypair, e::intrusive_ptr<keyholder> >::iterator i;

    if ((i = m_keyholders.find(kp)) != m_keyholders.end())
    {
        return i->second;
    }
    else
    {
        e::intrusive_ptr<keyholder> kh(new keyholder(m_config, kp.region.get_subspace(), m_comm));
        m_keyholders.insert(std::make_pair(kp, kh));
        return kh;
    }
}

bool
hyperdex :: replication :: have_seen_clientop(const clientop& co)
{
    po6::threads::mutex::hold hold(&m_clientops_lock);
    return m_clientops.find(co) != m_clientops.end();
}

void
hyperdex :: replication :: respond_positively_to_client(clientop co,
                                                        uint64_t /*version*/)
{
    e::buffer msg;
    uint8_t result = static_cast<uint8_t>(SUCCESS);
    msg.pack() << co.nonce << result;
    m_comm->send(co.region, co.from, stream_no::RESULT, msg);
}

void
hyperdex :: replication :: respond_negatively_to_client(clientop co,
                                                        result_t r)
{
    e::buffer msg;
    uint8_t result = static_cast<uint8_t>(r);
    msg.pack() << co.nonce << result;
    m_comm->send(co.region, co.from, stream_no::RESULT, msg);
}

bool
hyperdex :: replication :: clientop :: operator < (const clientop& rhs) const
{
    const clientop& lhs(*this);

    if (lhs.region < rhs.region)
    {
        return true;
    }
    else if (lhs.region == rhs.region)
    {
        if (lhs.from < rhs.from)
        {
            return true;
        }
        else if (lhs.from == rhs.from)
        {
            return lhs.nonce < rhs.nonce;
        }
        else
        {
            return false;
        }
    }
    else
    {
        return false;
    }
}

bool
hyperdex :: replication :: keypair :: operator < (const keypair& rhs) const
{
    const keypair& lhs(*this);

    if (lhs.region < rhs.region)
    {
        return true;
    }
    else if (lhs.region > rhs.region)
    {
        return false;
    }

    return lhs.key < rhs.key;
}

hyperdex :: replication :: pending :: pending(bool f,
                                              const std::vector<e::buffer>& v,
                                              std::tr1::function<void (uint64_t)> n,
                                              const regionid& prev,
                                              const regionid& thisold,
                                              const regionid& thisnew,
                                              const regionid& next)
    : enabled(false)
    , fresh(f)
    , op(PUT)
    , value(v)
    , notify(n)
    , m_ref(0)
    , m_prev(prev)
    , m_thisold(thisold)
    , m_thisnew(thisnew)
    , m_next(next)
{
}

hyperdex :: replication :: pending :: pending(std::tr1::function<void (uint64_t)> n,
                                              const regionid& prev,
                                              const regionid& thisold,
                                              const regionid& thisnew,
                                              const regionid& next)
    : enabled(false)
    , fresh(false)
    , op(DEL)
    , value()
    , notify(n)
    , m_ref(0)
    , m_prev(prev)
    , m_thisold(thisold)
    , m_thisnew(thisnew)
    , m_next(next)
{
}

void
hyperdex :: replication :: pending :: forward_msg(e::buffer* msg,
                                                  uint64_t version,
                                                  const e::buffer& key)
{
    uint8_t f = fresh ? 1 : 0;

    if (op == PUT)
    {
        msg->pack() << version << f << key << value;
    }
    else
    {
        msg->pack() << version << f << key;
    }
}

void
hyperdex :: replication :: pending :: ack_msg(e::buffer* msg,
                                              uint64_t version,
                                              const e::buffer& key)
{
    msg->pack() << version << key;
}

hyperdex :: replication :: keyholder :: keyholder(const configuration& config,
                                                  const regionid& r,
                                                  logical* comm)
    : m_ref(0)
    , m_comm(comm)
    , m_region(r)
    , m_prev_subspace(0)
    , m_next_subspace(0)
    , m_prev_dims()
    , m_this_dims()
    , m_next_dims()
    , m_pending()
{
    // Count the number of subspaces for this space.
    size_t subspaces;

    if (!config.subspaces(r.get_space(), &subspaces))
    {
        throw std::runtime_error("Creating keyholder for non-existant subspace");
    }

    // Discover the dimensions of the space which correspond to the
    // subspace in which "to" is located, and the subspaces before and
    // after it.
    const_cast<uint16_t&>(m_prev_subspace) = r.subspace > 0 ? r.subspace - 1 : subspaces - 1;
    const_cast<uint16_t&>(m_next_subspace) = r.subspace < subspaces - 1 ? r.subspace + 1 : 0;

    if (!config.dimensions(r.get_subspace(), const_cast<std::vector<bool>*>(&m_this_dims))
        || !config.dimensions(subspaceid(r.space, m_prev_subspace), const_cast<std::vector<bool>*>(&m_prev_dims))
        || !config.dimensions(subspaceid(r.space, m_next_subspace), const_cast<std::vector<bool>*>(&m_next_dims)))
    {
        throw std::runtime_error("Could not determine dimensions of neighboring subspaces.");
    }
}

bool
hyperdex :: replication :: keyholder :: add_pending(std::tr1::function<result_t (std::vector<e::buffer>*, uint64_t*)> read_from_disk,
                                                    const e::buffer& key,
                                                    const std::vector<e::buffer>& value,
                                                    std::tr1::function<void (uint64_t)> notify)
{
    uint64_t oldversion = 0;
    bool have_oldvalue = false;
    std::vector<e::buffer> oldvalue;

    if (m_pending.empty())
    {
        // Get old version from disk.
        if (!from_disk(read_from_disk, &have_oldvalue, &oldvalue, &oldversion))
        {
            return false;
        }
    }
    else
    {
        // Get old version from memory.
        std::map<uint64_t, e::intrusive_ptr<pending> >::reverse_iterator biggest;
        biggest = m_pending.rbegin();
        oldversion = biggest->first;
        have_oldvalue = biggest->second->op == PUT;
        oldvalue = biggest->second->value;
    }

    // Figure out the four regions we could send to.
    regionid prev;
    regionid thisold;
    regionid thisnew;
    regionid next;

    if (have_oldvalue)
    {
        four_regions(key, oldvalue, value, &prev, &thisold, &thisnew, &next);
    }
    else
    {
        four_regions(key, value, &prev, &thisold, &thisnew, &next);
    }

    e::intrusive_ptr<pending> newpend;
    newpend = new pending(!have_oldvalue, value, notify, prev, thisold, thisnew, next);
    newpend->enabled = true;
    m_pending.insert(std::make_pair(oldversion + 1, newpend));
    e::buffer msg;
    newpend->forward_msg(&msg, oldversion + 1, key);

    if (overlap(m_region, thisnew))
    {
        return m_comm->send_forward(m_region, next, stream_no::PUT_PENDING, msg);
    }
    else if (overlap(m_region, thisold))
    {
        return m_comm->send_forward(m_region, thisnew, stream_no::PUT_PENDING, msg);
    }
    else
    {
        LOG(INFO) << "Message doesn't hash to this region.";
        return false;
    }
}

bool
hyperdex :: replication :: keyholder :: add_pending(std::tr1::function<result_t (std::vector<e::buffer>*, uint64_t*)> read_from_disk,
                                                    const e::buffer& key,
                                                    std::tr1::function<void (uint64_t)> notify)
{
    uint64_t oldversion = 0;
    bool have_oldvalue = false;
    std::vector<e::buffer> oldvalue;

    if (m_pending.empty())
    {
        // Get old version from disk.
        if (!from_disk(read_from_disk, &have_oldvalue, &oldvalue, &oldversion))
        {
            return false;
        }
    }
    else
    {
        // Get old version from memory.
        std::map<uint64_t, e::intrusive_ptr<pending> >::reverse_iterator biggest;
        biggest = m_pending.rbegin();
        oldversion = biggest->first;
        have_oldvalue = biggest->second->op == PUT;
        oldvalue = biggest->second->value;
    }

    // Figure out the four regions we could send to.
    regionid prev;
    regionid thisold;
    regionid thisnew;
    regionid next;

    if (have_oldvalue)
    {
        four_regions(key, oldvalue, &prev, &thisold, &thisnew, &next);
    }
    else
    {
        notify(0);
        return true;
    }

    e::intrusive_ptr<pending> newpend;
    newpend = new pending(notify, prev, thisold, thisnew, next);
    newpend->enabled = true;
    m_pending.insert(std::make_pair(oldversion + 1, newpend));
    e::buffer msg;
    newpend->forward_msg(&msg, oldversion + 1, key);

    if (overlap(m_region, thisnew))
    {
        return m_comm->send_forward(m_region, next, stream_no::DEL_PENDING, msg);
    }
    else if (overlap(m_region, thisold))
    {
        return m_comm->send_forward(m_region, thisnew, stream_no::DEL_PENDING, msg);
    }
    else
    {
        LOG(INFO) << "Message doesn't hash to this region.";
        return false;
    }
}

void
hyperdex :: replication :: keyholder :: add_pending(std::tr1::function<result_t (std::vector<e::buffer>*, uint64_t*)> read_from_disk,
                                                    const entityid& from,
                                                    const entityid& to,
                                                    uint64_t version,
                                                    bool fresh,
                                                    const e::buffer& key,
                                                    const std::vector<e::buffer>& value)
{
    uint64_t oldversion = 0;
    bool have_oldvalue = false;
    std::vector<e::buffer> oldvalue;
    bool sendack = false;

    if (m_pending.empty())
    {
        if (!from_disk(read_from_disk, &have_oldvalue, &oldvalue, &oldversion))
        {
            return;
        }

        // If we don't have the old value, and the message wasn't tagged
        // "fresh" (as in, OK to apply without an old value), then we
        // just drop the message.
        if (!have_oldvalue && !fresh)
        {
            return;
        }

        // Make sure we apply updates in order
        if (oldversion != version - 1)
        {
            return;
        }
    }
    else
    {
        // If we've already put this in as a pending update.
        if (m_pending.find(version) != m_pending.end())
        {
            return;
        }

        // Find the update at version -1.
        std::map<uint64_t, e::intrusive_ptr<pending> >::iterator old;
        old = m_pending.find(version - 1);

        // If we've not got the previous update to it
        if (old == m_pending.end())
        {
            if (!from_disk(read_from_disk, &have_oldvalue, &oldvalue, &oldversion))
            {
                return;
            }

            if (oldversion >= version)
            {
                sendack = true;
                return;
            }
            else if (oldversion != version - 1)
            {
                return;
            }
        }
        else
        {
            oldversion = old->first;
            have_oldvalue = old->second->op == PUT;
            oldvalue = old->second->value;
        }
    }

    // Figure out the four regions we could send to.
    regionid prev;
    regionid thisold;
    regionid thisnew;
    regionid next;

    if (have_oldvalue)
    {
        four_regions(key, oldvalue, value, &prev, &thisold, &thisnew, &next);
    }
    else
    {
        four_regions(key, value, &prev, &thisold, &thisnew, &next);
    }

    e::intrusive_ptr<pending> newpend;
    newpend = new pending(!have_oldvalue, value, nop, prev, thisold, thisnew, next);
    newpend->enabled = true;

    // Send an ack message if we've already committed this update.
    if (sendack)
    {
        e::buffer msg;
        newpend->ack_msg(&msg, version, key);
        m_comm->send(to, from, stream_no::ACK, msg);
        return;
    }

    newpend->enabled = true;
    m_pending.insert(std::make_pair(version, newpend));
    e::buffer msg;
    newpend->forward_msg(&msg, version, key);

    if (overlap(m_region, thisnew))
    {
        m_comm->send_forward(m_region, next, stream_no::PUT_PENDING, msg);
    }
    else if (overlap(m_region, thisold))
    {
        m_comm->send_forward(m_region, thisnew, stream_no::PUT_PENDING, msg);
    }
    else
    {
        LOG(INFO) << "Message doesn't hash to this region.";
    }
}

void
hyperdex :: replication :: keyholder :: add_pending(std::tr1::function<result_t (std::vector<e::buffer>*, uint64_t*)> read_from_disk,
                                                    const entityid& from,
                                                    const entityid& to,
                                                    uint64_t version,
                                                    const e::buffer& key)
{
    uint64_t oldversion = 0;
    bool have_oldvalue = false;
    std::vector<e::buffer> oldvalue;
    bool sendack = false;

    if (m_pending.empty())
    {
        if (!from_disk(read_from_disk, &have_oldvalue, &oldvalue, &oldversion))
        {
            return;
        }

        // Make sure we apply updates in order
        if (oldversion != version - 1)
        {
            return;
        }
    }
    else
    {
        // If we've already put this in as a pending update.
        if (m_pending.find(version) != m_pending.end())
        {
            return;
        }

        // Find the update at version -1.
        std::map<uint64_t, e::intrusive_ptr<pending> >::iterator old;
        old = m_pending.find(version - 1);

        // If we've not got the previous update to it
        if (old == m_pending.end())
        {
            if (!from_disk(read_from_disk, &have_oldvalue, &oldvalue, &oldversion))
            {
                return;
            }

            if (oldversion >= version)
            {
                sendack = true;
                return;
            }
            else if (oldversion != version - 1)
            {
                return;
            }
        }
        else
        {
            oldversion = old->first;
            have_oldvalue = old->second->op == PUT;
            oldvalue = old->second->value;
        }
    }

    // Figure out the four regions we could send to.
    regionid prev;
    regionid thisold;
    regionid thisnew;
    regionid next;

    if (have_oldvalue)
    {
        four_regions(key, oldvalue, &prev, &thisold, &thisnew, &next);
    }
    else
    {
        return;
    }

    e::intrusive_ptr<pending> newpend;
    newpend = new pending(nop, prev, thisold, thisnew, next);
    newpend->enabled = true;

    // Send an ack message if we've already committed this update.
    if (sendack)
    {
        e::buffer msg;
        newpend->ack_msg(&msg, version, key);
        m_comm->send(to, from, stream_no::ACK, msg);
        return;
    }

    newpend->enabled = true;
    m_pending.insert(std::make_pair(version, newpend));
    e::buffer msg;
    newpend->forward_msg(&msg, version, key);

    if (overlap(m_region, thisnew))
    {
        m_comm->send_forward(m_region, next, stream_no::DEL_PENDING, msg);
    }
    else if (overlap(m_region, thisold))
    {
        m_comm->send_forward(m_region, thisnew, stream_no::DEL_PENDING, msg);
    }
    else
    {
        LOG(INFO) << "Message doesn't hash to this region.";
    }
}

bool
hyperdex :: replication :: keyholder :: from_disk(std::tr1::function<result_t (std::vector<e::buffer>*, uint64_t*)> read_from_disk,
                                                  bool* have_value,
                                                  std::vector<e::buffer>* value,
                                                  uint64_t* version)
{
    switch (read_from_disk(value, version))
    {
        case SUCCESS:
            *have_value = true;
            return true;
        case NOTFOUND:
            *version = 0;
            *have_value = false;
            return true;
        case INVALID:
            LOG(INFO) << "Data layer returned INVALID when queried for old value.";
            return false;
        case ERROR:
            LOG(INFO) << "Data layer returned ERROR when queried for old value.";
            return false;
        default:
            LOG(WARNING) << "Data layer returned unknown result when queried for old value.";
            return false;
    }
}

void
hyperdex :: replication :: keyholder :: four_regions(const e::buffer& key,
                                                     const std::vector<e::buffer>& oldvalue,
                                                     const std::vector<e::buffer>& value,
                                                     regionid* prev,
                                                     regionid* thisold,
                                                     regionid* thisnew,
                                                     regionid* next)
{
    assert(oldvalue.size() == value.size());
    std::vector<uint64_t> oldhashes;
    std::vector<uint64_t> newhashes;
    uint64_t keyhash = CityHash64(key);
    oldhashes.push_back(keyhash);
    newhashes.push_back(keyhash);

    for (size_t i = 0; i < value.size(); ++i)
    {
        oldhashes.push_back(CityHash64(oldvalue[i]));
        newhashes.push_back(CityHash64(value[i]));
    }

    *prev = regionid(m_region.get_subspace(), 64, makepoint(newhashes, m_prev_dims));
    *thisold = regionid(m_region.get_subspace(), 64, makepoint(oldhashes, m_this_dims));
    *thisnew = regionid(m_region.get_subspace(), 64, makepoint(newhashes, m_this_dims));
    *next = regionid(m_region.get_subspace(), 64, makepoint(oldhashes, m_next_dims));
}


void
hyperdex :: replication :: keyholder :: four_regions(const e::buffer& key,
                                                     const std::vector<e::buffer>& value,
                                                     regionid* prev,
                                                     regionid* thisold,
                                                     regionid* thisnew,
                                                     regionid* next)
{
    std::vector<uint64_t> hashes;
    uint64_t keyhash = CityHash64(key);
    hashes.push_back(keyhash);

    for (size_t i = 0; i < value.size(); ++i)
    {
        hashes.push_back(CityHash64(value[i]));
    }

    *prev = regionid(m_region.get_subspace(), 64, makepoint(hashes, m_prev_dims));
    *thisold = regionid(m_region.get_subspace(), 64, makepoint(hashes, m_this_dims));
    *thisnew = regionid(m_region.get_subspace(), 64, makepoint(hashes, m_this_dims));
    *next = regionid(m_region.get_subspace(), 64, makepoint(hashes, m_next_dims));
}
