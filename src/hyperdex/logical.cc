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
#include <stdexcept>

// Google Log
#include <glog/logging.h>

// HyperDex
#include <hyperdex/logical.h>
#include <hyperdex/hyperspace.h>

using e::buffer;
using po6::net::location;
using po6::threads::rwlock;

hyperdex :: logical :: logical(ev::loop_ref lr,
                               const po6::net::ipaddr& ip)
    : m_us()
    , m_mapping_lock()
    , m_client_lock()
    , m_mapping()
    , m_client_nums()
    , m_client_locs()
    , m_client_counter(0)
    , m_physical(lr, ip)
{
    // XXX Get the addresses of the underlying layer, and message the master to
    // get the versions.
    m_us.inbound = m_physical.inbound();
    m_us.inbound_version = 0;
    m_us.outbound = m_physical.outbound();
    m_us.outbound_version = 0;
}

hyperdex :: logical :: ~logical()
{
}

typedef std::map<hyperdex::entityid, hyperdex::instance>::iterator mapiter;

void
hyperdex :: logical :: remap(std::map<entityid, instance> mapping)
{
    rwlock::wrhold wr(&m_mapping_lock);
    LOG(INFO) << "Installing new mapping.";
    m_mapping.swap(mapping);
}

bool
hyperdex :: logical :: send(const hyperdex::entityid& from, const hyperdex::entityid& to,
                            const uint8_t msg_type,
                            const e::buffer& msg)
{
    uint16_t fromver = 0;
    uint16_t tover = 0;
    po6::net::location dst;

    // If we are sending to a client
    if (to.space == UINT32_MAX)
    {
        {
            rwlock::rdhold rd(&m_client_lock);
            std::map<uint64_t, po6::net::location>::iterator cli;
            cli = m_client_locs.find(to.mask);

            if (cli == m_client_locs.end())
            {
                return false;
            }

            dst = cli->second;
        }
        {
            rwlock::rdhold rd(&m_mapping_lock);
            mapiter f = m_mapping.find(from);

            if (f == m_mapping.end() || f->second != m_us)
            {
                return false;
            }

            fromver = f->second.outbound_version;
        }
    }
    else
    {
        rwlock::rdhold rd(&m_mapping_lock);
        mapiter f = m_mapping.find(from);
        mapiter t = m_mapping.find(to);

        if (f == m_mapping.end() || t == m_mapping.end() || f->second != m_us)
        {
            return false;
        }

        fromver = f->second.outbound_version;
        tover = t->second.inbound_version;
        dst = t->second.inbound;
    }

    buffer finalmsg(msg.size() + 32);
    finalmsg.pack() << msg_type
                    << fromver
                    << tover
                    << from
                    << to
                    << msg;
    m_physical.send(dst, finalmsg);
    return true;
}

bool
hyperdex :: logical :: send(const hyperdex::regionid& from,
                            const hyperdex::entityid& to,
                            const uint8_t msg_type, const e::buffer& msg)
{
    entityid realfrom;

    // We don't need to look up the block at exactly the same time.  If
    // a change occurs between now and when the inner send acquires the
    // read lock, it is likely that the message will be thrown out at
    // the above layer.
    {
        po6::threads::rwlock::rdhold hold(&m_mapping_lock);
        mapiter f = m_mapping.lower_bound(entityid(from, 0));
        mapiter t = m_mapping.upper_bound(entityid(from, UINT8_MAX));

        for (; f != t; ++f)
        {
            if (f->second == m_us)
            {
                realfrom = f->first;
                break;
            }
        }

        if (f == t)
        {
            return false;
        }
    }

    return send(realfrom, to, msg_type, msg);
}

bool
hyperdex :: logical :: send_forward(const hyperdex::regionid& from,
                                    const hyperdex::regionid& to,
                                    const uint8_t msg_type,
                                    const e::buffer& msg)
{
    entityid realfrom;
    entityid realto;

    // Same reasoning about locking applies.
    {
        po6::threads::rwlock::rdhold hold(&m_mapping_lock);
        mapiter f = m_mapping.lower_bound(entityid(from, 0));
        mapiter t = m_mapping.upper_bound(entityid(from, UINT8_MAX));

        for (; f != t; ++f)
        {
            if (f->second == m_us)
            {
                realfrom = f->first;
                break;
            }
        }

        if (f == t)
        {
            return false;
        }

        ++f;

        if (f == t)
        {
            mapiter i = m_mapping.begin();

            for (; i != m_mapping.end(); ++i)
            {
                if (overlap(i->first.get_region(), to))
                {
                    realto = i->first;
                    break;
                }
            }

            if (i == m_mapping.end())
            {
                return false;
            }
        }
        else
        {
            realto = f->first;
        }
    }

    return send(realfrom, realto, msg_type, msg);
}

bool
hyperdex :: logical :: send_backward(const hyperdex::regionid& from,
                                     const hyperdex::regionid& to,
                                     const uint8_t msg_type,
                                     const e::buffer& msg)
{
    entityid realfrom;
    entityid realto;

    // Same reasoning about locking applies.
    {
        po6::threads::rwlock::rdhold hold(&m_mapping_lock);
        mapiter f = m_mapping.lower_bound(entityid(from, 0));
        mapiter t = m_mapping.upper_bound(entityid(from, UINT8_MAX));

        for (; f != t; ++f)
        {
            if (f->second == m_us)
            {
                realfrom = f->first;
                break;
            }
        }

        if (f == t)
        {
            return false;
        }

        if (realfrom.number == 0)
        {
            mapiter i = m_mapping.begin();

            for (; i != m_mapping.end(); ++i)
            {
                if (overlap(i->first.get_region(), to))
                {
                    realto = i->first;
                    break;
                }
            }

            if (i == m_mapping.end())
            {
                return false;
            }
        }
        else
        {
            realto = entityid(realfrom.get_region(), realfrom.number - 1);
        }

        ++f;

        if (f == t)
        {
            typedef std::map<hyperdex::entityid, hyperdex::instance>::reverse_iterator reverse_mapiter;
            reverse_mapiter i = m_mapping.rbegin();

            for (; i != m_mapping.rend(); ++i)
            {
                if (overlap(i->first.get_region(), to))
                {
                    realto = i->first;
                    break;
                }
            }

            if (i == m_mapping.rend())
            {
                return false;
            }
        }
        else
        {
            realto = f->first;
        }
    }

    return send(realfrom, realto, msg_type, msg);
}

bool
hyperdex :: logical :: recv(hyperdex::entityid* from, hyperdex::entityid* to,
                            uint8_t* msg_type,
                            e::buffer* msg)
{
    location loc;
    bool fromvalid = false;
    bool tovalid = false;
    instance frominst;
    instance toinst;
    uint16_t fromver = 0;
    uint16_t tover = 0;

    // Read messages from the network until we get one which meets the following
    // constraints:
    //  - We have a mapping for the source entityid.
    //  - We have a mapping for the destination entityid.
    //  - We have a mapping for the source entityid which corresponds to the
    //    underlying network location and version number of this message.
    //  - The destination entityid maps to our network location.
    //  - The message is to the correct version of our port bindings.
    do
    {
        buffer packed;

        if (!m_physical.recv(&loc, &packed))
        {
            return false;
        }

        if (packed.size() < sizeof(uint8_t) + 2 * sizeof(uint16_t) +
                            2 * entityid::SERIALIZEDSIZE)
        {
            continue;
        }

        // This should not throw thanks to the size check above.
        msg->clear();
        packed.unpack() >> *msg_type >> fromver >> tover >> *from >> *to >> *msg;

        // If the message is from someone claiming to be a client.
        if (from->space == UINT32_MAX)
        {
            // If the entity is (UINT32_MAX, X, Y, 0, Z), then grab a wrlock on
            // entities and give this entity a new id.  The client should pick
            // up on this, and start using this tag for our convenience.  If the
            // entity is not filled with zeroes, then the client assumes we've
            // talked before, and we can go for a read lock.  A bad client does
            // not hamper our correctness, but can cause us to grab a write lock
            // much more often than we need to.

            if (from->mask == 0)
            {
                rwlock::wrhold wr(&m_client_lock);
                std::map<po6::net::location, uint64_t>::iterator cni;
                cni = m_client_nums.find(loc);

                if (cni == m_client_nums.end())
                {
                    ++ m_client_counter;
                    uint64_t id = m_client_counter;
                    m_client_nums.insert(std::make_pair(loc, id));
                    m_client_locs.insert(std::make_pair(id, loc));
                    from->mask = id;
                }
                else
                {
                    // XXX This case here is where the client just hurt the
                    // performance of the other clients.
                    from->mask = cni->second;
                }

                fromvalid = true;
                frominst.outbound = loc;
                frominst.outbound_version = fromver;
            }
            else
            {
                rwlock::rdhold rd(&m_client_lock);

                if (m_client_locs.find(from->mask) == m_client_locs.end())
                {
                    continue;
                }
                else
                {
                    fromvalid = true;
                    frominst.outbound = loc;
                    frominst.outbound_version = fromver;
                }
            }

            rwlock::rdhold rd(&m_mapping_lock);
            mapiter t = m_mapping.find(*to);
            tovalid = t != m_mapping.end();

            if (tovalid)
            {
                toinst = t->second;
            }
        }
        else
        {
            // Grab a read lock on the entity mapping.
            rwlock::rdhold rd(&m_mapping_lock);

            // Find the from/to mappings.
            mapiter f;
            mapiter t;
            f = m_mapping.find(*from);
            t = m_mapping.find(*to);
            fromvalid = f != m_mapping.end();
            tovalid = t != m_mapping.end();

            if (fromvalid)
            {
                frominst = f->second;
            }

            if (tovalid)
            {
                toinst = t->second;
            }
        }
    }
    while (!fromvalid // Try again because we don't know the source.
            || !tovalid // Try again because we don't know the destination.
            || frominst.outbound != loc // Try again because the sender isn't who it should be.
            || frominst.outbound_version != fromver // Try again because an older sender is sending the message.
            || toinst != m_us // Try again because we don't believe ourselves to be the dest entity.
            || m_us.inbound_version != tover); // Try again because it is to an older version of us.

    return true;
}

void
hyperdex :: logical :: shutdown()
{
    m_physical.shutdown();
}
