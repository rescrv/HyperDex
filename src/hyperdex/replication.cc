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

// Google Log
#include <glog/logging.h>

// e
#include <e/guard.h>

// HyperDex
#include <hyperdex/buffer.h>
#include <hyperdex/city.h>
#include <hyperdex/hyperspace.h>
#include <hyperdex/replication.h>

hyperdex :: replication :: replication(datalayer* data, logical* comm)
    : m_data(data)
    , m_comm(comm)
    , m_lock()
    , m_config()
    , m_clientops()
    , m_pending()
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
hyperdex :: replication :: client_common(op_t op,
                                         const entityid& from,
                                         const regionid& to,
                                         uint32_t nonce,
                                         const e::buffer& key,
                                         const std::vector<e::buffer>& newvalue)
{
    clientop co(to, from, nonce);
    // Automatically respond with "ERROR" whenever we return without
    // g.dismiss()
    e::guard g = e::makeobjguard(*this, &replication::respond_negatively_to_client, co, ERROR);

    // Grab the lock.
    po6::threads::mutex::hold hold(&m_lock);

    // If we have seen this (from, nonce) before and it is still active.
    if (m_clientops.find(co) != m_clientops.end())
    {
        // Silently ignore this message.
        g.dismiss();
        return;
    }

    // Count the number of subspaces for this space.
    size_t subspaces;

    if (!m_config.subspaces(to.get_space(), &subspaces))
    {
        LOG(INFO) << "Unable to discover the number of subspaces in " << to.get_space();
        return;
    }

    // Discover the dimensions of the space which correspond to the
    // subspace in which "to" is located, and the subspaces before and
    // after it.
    std::vector<bool> prev_dims;
    std::vector<bool> this_dims;
    std::vector<bool> next_dims;

    uint16_t prev_subspace = to.subspace > 0 ? to.subspace - 1 : subspaces - 1;
    uint16_t next_subspace = to.subspace < subspaces - 1 ? to.subspace + 1 : 0;

    if (!m_config.dimensions(to.get_subspace(), &this_dims)
        || !m_config.dimensions(subspaceid(to.space, prev_subspace), &prev_dims)
        || !m_config.dimensions(subspaceid(to.space, next_subspace), &next_dims))
    {
        LOG(INFO) << "Could determine dimensions subsets for " << to;
        return;
    }

    // Figure out if the client's PUT represents a point.
    if (op == PUT && this_dims.size() != newvalue.size() + 1)
    {
        return;
    }

    // Read the older version from disk.
    uint64_t oldversion = 0;
    bool have_oldvalue = false;
    std::vector<e::buffer> oldvalue;

    if (!check_oldversion(to, key, &have_oldvalue, &oldversion, &oldvalue))
    {
        respond_negatively_to_client(co, INVALID);
        g.dismiss();
        return;
    }

    // Prepare a pending operation.
    e::intrusive_ptr<pending> pend;
    std::tr1::function<void (void)>
        notify(std::tr1::bind(&hyperdex::replication::respond_positively_to_client,
                              this, co, oldversion + 1));

    if (!have_oldvalue && op == DEL)
    {
        // We don't bother to store this as pending as there is no one
        // to tell about it.
        respond_positively_to_client(co, 0);
        g.dismiss();
        return;
    }
    else if (have_oldvalue && op == DEL)
    {
        pend = new pending(to, DEL, oldversion + 1, key, oldvalue, notify, prev_subspace, next_subspace, prev_dims, this_dims, next_dims);
    }
    else if (!have_oldvalue && op == PUT)
    {
        pend = new pending(to, PUT, oldversion + 1, key, newvalue, notify, prev_subspace, next_subspace, prev_dims, this_dims, next_dims);
    }
    else if (have_oldvalue && op == PUT)
    {
        pend = new pending(to, oldversion + 1, key, oldvalue, newvalue, notify, prev_subspace, next_subspace, prev_dims, this_dims, next_dims);
    }

    // Store the operation as pending.
    // XXX Do not enable if something else is pending.  Furthermore, do
    // enable when the other pending operation finishes.
    pend->enable();
    pend->tryforward(m_comm);
    m_clientops.insert(co);
    m_pending.insert(std::make_pair(to, pend));
    g.dismiss();
}

bool
hyperdex :: replication :: check_oldversion(const regionid& to, const e::buffer& key,
                                            bool* have_value, uint64_t* version,
                                            std::vector<e::buffer>* value)
{
    // Read the current value from the list of previous pending values
    // or disk.
    *version = 0;
    *have_value = false;

    std::multimap<regionid, e::intrusive_ptr<pending> >::iterator lower;
    std::multimap<regionid, e::intrusive_ptr<pending> >::iterator upper;
    lower = m_pending.lower_bound(to);
    upper = m_pending.upper_bound(to);

    for (; lower != upper; ++lower)
    {
        pending& pend(*(lower->second));

        if (pend.key == key && pend.version > *version)
        {
            *version = pend.version;

            if (pend.op == PUT)
            {
                *have_value = true;
                *value = pend.value;
            }
            else
            {
                *have_value = false;
                *value = std::vector<e::buffer>();
            }
        }
    }

    if (*version != 0)
    {
        return true;
    }

    switch (m_data->get(to, key, value, version))
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

hyperdex :: replication :: pending :: pending(const regionid& r,
                                              op_t o,
                                              uint64_t ver,
                                              const e::buffer& k,
                                              const std::vector<e::buffer>& val,
                                              std::tr1::function<void (void)> n,
                                              uint16_t prev_subspace,
                                              uint16_t next_subspace,
                                              const std::vector<bool>& prev_dims,
                                              const std::vector<bool>& this_dims,
                                              const std::vector<bool>& next_dims)
    : region(r)
    , op(o)
    , version(ver)
    , key(k)
    , value(val)
    , notify(n)
    , m_ref(0)
    , m_enabled(false)
    , m_fresh(op == PUT)
    , m_prev(r)
    , m_thisold(r)
    , m_thisnew(r)
    , m_next(r)
{
    std::vector<uint64_t> hashes;
    hashes.push_back(CityHash64(key));

    for (size_t i = 0; i < value.size(); ++i)
    {
        hashes.push_back(CityHash64(value[i]));
    }

    m_prev.subspace = prev_subspace;
    m_prev.prefix = 64;
    m_prev.mask = makepoint(hashes, prev_dims);

    m_thisold.prefix = 64;
    m_thisold.mask = makepoint(hashes, this_dims);
    m_thisnew = m_thisold;

    m_next.subspace = next_subspace;
    m_next.prefix = 64;
    m_next.mask = makepoint(hashes, next_dims);
}

hyperdex :: replication :: pending :: pending(const regionid& r,
                                              uint64_t ver,
                                              const e::buffer& k,
                                              const std::vector<e::buffer>& oldval,
                                              const std::vector<e::buffer>& newval,
                                              std::tr1::function<void (void)> n,
                                              uint16_t prev_subspace,
                                              uint16_t next_subspace,
                                              const std::vector<bool>& prev_dims,
                                              const std::vector<bool>& this_dims,
                                              const std::vector<bool>& next_dims)
    : region(r)
    , op(PUT)
    , version(ver)
    , key(k)
    , value(newval)
    , notify(n)
    , m_ref(0)
    , m_enabled(false)
    , m_fresh(false)
    , m_prev(r)
    , m_thisold(r)
    , m_thisnew(r)
    , m_next(r)
{
    std::vector<uint64_t> oldhashes;
    std::vector<uint64_t> newhashes;
    uint64_t keyhash = CityHash64(key);
    oldhashes.push_back(keyhash);
    newhashes.push_back(keyhash);

    for (size_t i = 0; i < value.size(); ++i)
    {
        oldhashes.push_back(CityHash64(oldval[i]));
        newhashes.push_back(CityHash64(newval[i]));
    }

    m_prev.subspace = prev_subspace;
    m_prev.prefix = 64;
    m_prev.mask = makepoint(newhashes, prev_dims);

    m_thisold.prefix = 64;
    m_thisold.mask = makepoint(oldhashes, this_dims);

    m_thisnew.prefix = 64;
    m_thisnew.mask = makepoint(newhashes, this_dims);

    m_next.subspace = next_subspace;
    m_next.prefix = 64;
    m_next.mask = makepoint(oldhashes, next_dims);
}

void
hyperdex :: replication :: pending :: tryforward(logical* comm)
{
    if (!m_enabled)
    {
        return;
    }

    // Pack the message.
    e::buffer msg;

    if (op == PUT)
    {
        msg.pack() << version << key << value;
    }
    else if (op == DEL)
    {
        msg.pack() << version << key;
    }
}
