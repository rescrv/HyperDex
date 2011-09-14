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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// C
#include <stdint.h>

// STL
#include <stdexcept>

// Google Log
#include <glog/logging.h>

// HyperDaemon
#include "logical.h"

using hyperdex::configuration;
using hyperdex::coordinatorlink;
using hyperdex::entityid;
using hyperdex::instance;
using hyperdex::network_msgtype;
using hyperdex::regionid;

hyperdaemon :: logical :: logical(coordinatorlink* cl, const po6::net::ipaddr& ip,
                                  in_port_t incoming, in_port_t outgoing)
    : m_cl(cl)
    , m_us()
    , m_config()
    , m_mapping()
    , m_client_nums()
    , m_client_locs()
    , m_client_counter(0)
    , m_physical(ip, incoming, outgoing, true)
{
    m_us.inbound = m_physical.inbound();
    m_us.inbound_version = 0;
    m_us.outbound = m_physical.outbound();
    m_us.outbound_version = 0;
}

hyperdaemon :: logical :: ~logical() throw ()
{
}

typedef std::map<hyperdex::entityid, hyperdex::instance>::iterator mapiter;

void
hyperdaemon :: logical :: prepare(const configuration&, const hyperdex::instance&)
{
    // Do nothing.
}

void
hyperdaemon :: logical :: reconfigure(const configuration& newconfig,
                                      const hyperdex::instance& newinst)
{
    m_config = newconfig;
    m_mapping = newconfig.entity_mapping();
    m_us = newinst;
}

void
hyperdaemon :: logical :: cleanup(const configuration&, const hyperdex::instance&)
{
    // Do nothing.
}

bool
hyperdaemon :: logical :: send(const hyperdex::entityid& from, const hyperdex::entityid& to,
                               const network_msgtype msg_type,
                               const e::buffer& msg)
{
    return send_you_hold_lock(from, to, msg_type, msg);
}

bool
hyperdaemon :: logical :: send(const hyperdex::regionid& from,
                               const hyperdex::entityid& to,
                               const network_msgtype msg_type, const e::buffer& msg)
{
    entityid realfrom;

    if (!our_position(from, &realfrom))
    {
        return false;
    }

    return send_you_hold_lock(realfrom, to, msg_type, msg);
}

bool
hyperdaemon :: logical :: send_forward_else_head(const hyperdex::regionid& chain,
                                                 network_msgtype msg1_type,
                                                 const e::buffer& msg1,
                                                 const hyperdex::regionid& otherwise,
                                                 network_msgtype msg2_type,
                                                 const e::buffer& msg2)
{
    entityid from;

    if (!our_position(chain, &from))
    {
        return false;
    }

    entityid chain_next = entityid(from.get_region(), from.number + 1);

    if (m_mapping.find(chain_next) != m_mapping.end())
    {
        return send_you_hold_lock(from, chain_next, msg1_type, msg1);
    }
    else
    {
        entityid head = m_config.headof(otherwise);
        return send_you_hold_lock(from, head, msg2_type, msg2);
    }
}

bool
hyperdaemon :: logical :: send_forward_else_tail(const hyperdex::regionid& chain,
                                                 network_msgtype msg1_type,
                                                 const e::buffer& msg1,
                                                 const hyperdex::regionid& otherwise,
                                                 network_msgtype msg2_type,
                                                 const e::buffer& msg2)
{
    entityid from;

    if (!our_position(chain, &from))
    {
        return false;
    }

    entityid chain_next = entityid(from.get_region(), from.number + 1);

    if (m_mapping.find(chain_next) != m_mapping.end())
    {
        return send_you_hold_lock(from, chain_next, msg1_type, msg1);
    }
    else
    {
        entityid tail = m_config.tailof(otherwise);
        return send_you_hold_lock(from, tail, msg2_type, msg2);
    }
}

bool
hyperdaemon :: logical :: send_backward(const hyperdex::regionid& chain,
                                        network_msgtype msg_type,
                                        const e::buffer& msg)
{
    entityid from;

    if (!our_position(chain, &from))
    {
        return false;
    }

    if (from.number > 0)
    {
        entityid chain_prev = entityid(from.get_region(), from.number - 1);
        return send_you_hold_lock(from, chain_prev, msg_type, msg);
    }
    else
    {
        return false;
    }
}

bool
hyperdaemon :: logical :: send_backward_else_tail(const hyperdex::regionid& chain,
                                                  network_msgtype msg1_type,
                                                  const e::buffer& msg1,
                                                  const hyperdex::regionid& otherwise,
                                                  network_msgtype msg2_type,
                                                  const e::buffer& msg2)
{
    entityid from;

    if (!our_position(chain, &from))
    {
        return false;
    }

    if (from.number > 0)
    {
        entityid chain_prev = entityid(from.get_region(), from.number - 1);
        return send_you_hold_lock(from, chain_prev, msg1_type, msg1);
    }
    else
    {
        entityid tail = m_config.tailof(otherwise);
        return send_you_hold_lock(from, tail, msg2_type, msg2);
    }
}

bool
hyperdaemon :: logical :: recv(hyperdex::entityid* from, hyperdex::entityid* to,
                               network_msgtype* msg_type,
                               e::buffer* msg)
{
    po6::net::location loc;
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
        e::buffer packed;

        switch(m_physical.recv(&loc, &packed))
        {
            case physical::SHUTDOWN:
                return false;
            case physical::SUCCESS:
                break;
            case physical::DISCONNECT:
                handle_disconnect(loc);
                continue;
            case physical::CONNECTFAIL:
                handle_connectfail(loc);
                continue;
            case physical::QUEUED:
                LOG(ERROR) << "physical::recv unexpectedly returned QUEUED.";
                continue;
            case physical::LOGICERROR:
                LOG(ERROR) << "physical::recv unexpectedly returned LOGICERROR.";
                continue;
            default:
                LOG(ERROR) << "physical::recv unexpectedly returned unknown state.";
                continue;
        }

        if (packed.size() < sizeof(uint8_t) + 2 * sizeof(uint16_t) +
                            2 * entityid::SERIALIZEDSIZE)
        {
            continue;
        }

        // This should not throw thanks to the size check above.
        msg->clear();
        uint8_t mt;
        e::unpacker up(packed.unpack());
        up >> mt >> fromver >> tover >> *from >> *to;
        up.leftovers(msg);
        *msg_type = static_cast<network_msgtype>(mt);

        // If the message is from someone claiming to be a client.
        if (from->space == UINT32_MAX)
        {
            po6::net::location expected_loc;

            if (m_client_locs.lookup(from->mask, &expected_loc))
            {
                if (expected_loc != loc)
                {
                    continue;
                }
            }
            else
            {
                if (from->mask != 0)
                {
                    continue;
                }

                uint64_t client_num;

                if (!m_client_nums.lookup(loc, &client_num))
                {
                    client_num = __sync_add_and_fetch(&m_client_counter, 1);
                    m_client_locs.insert(client_num, loc);
                    m_client_nums.insert(loc, client_num);
                }

                from->mask = client_num;
            }

            fromvalid = true;
            frominst.outbound = loc;
            frominst.outbound_version = fromver;

            mapiter t = m_mapping.find(*to);
            tovalid = t != m_mapping.end();

            if (tovalid)
            {
                toinst = t->second;
            }
        }
        else
        {
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

#ifdef HD_LOG_ALL_MESSAGES
    LOG(INFO) << "RECV " << *from << "->" << *to << " " << *msg_type << " " << msg->hex();
#endif
    return true;
}

void
hyperdaemon :: logical :: handle_connectfail(const po6::net::location& loc)
{
    if (m_client_nums.contains(loc))
    {
        uint64_t client_num = 0;
        m_client_nums.lookup(loc, &client_num);
        m_client_nums.remove(loc);
        m_client_locs.remove(client_num);
    }
    else
    {
        LOG(INFO) << "XXX Tell the master we observed a connection failure to " << loc;
    }
}

void
hyperdaemon :: logical :: handle_disconnect(const po6::net::location& loc)
{
    if (m_client_nums.contains(loc))
    {
        uint64_t client_num = 0;
        m_client_nums.lookup(loc, &client_num);
        m_client_nums.remove(loc);
        m_client_locs.remove(client_num);
    }
    else
    {
        LOG(INFO) << "XXX Tell the master we observed a disconnect from " << loc;
    }
}

bool
hyperdaemon :: logical :: send_you_hold_lock(const hyperdex::entityid& from,
                                             const hyperdex::entityid& to,
                                             const network_msgtype msg_type,
                                             const e::buffer& msg)
{
#ifdef HD_LOG_ALL_MESSAGES
    LOG(INFO) << "SEND " << from << "->" << to << " " << msg_type << " " << msg.hex();
#endif
    uint16_t fromver = 0;
    uint16_t tover = 0;
    po6::net::location dst;

    // If we are sending to a client
    if (to.space == UINT32_MAX)
    {
        if (!m_client_locs.lookup(to.mask, &dst))
        {
            return false;
        }

        mapiter f = m_mapping.find(from);

        if (f == m_mapping.end() || f->second != m_us)
        {
            return false;
        }

        fromver = f->second.outbound_version;
    }
    else
    {
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

    uint8_t mt = static_cast<uint8_t>(msg_type);
    e::buffer finalmsg(msg.size() + 32);
    finalmsg.pack() << mt
                    << fromver
                    << tover
                    << from
                    << to;
    finalmsg += msg;

    if (dst == m_us.inbound)
    {
        m_physical.deliver(m_us.outbound, finalmsg);
    }
    else
    {
        switch (m_physical.send(dst, &finalmsg))
        {
            case physical::SUCCESS:
            case physical::QUEUED:
                break;
            case physical::CONNECTFAIL:
                handle_connectfail(dst);
                return false;
            case physical::DISCONNECT:
                handle_disconnect(dst);
                return false;
            case physical::SHUTDOWN:
                LOG(ERROR) << "physical::recv unexpectedly returned SHUTDOWN.";
                return false;
            case physical::LOGICERROR:
                LOG(ERROR) << "physical::recv unexpectedly returned LOGICERROR.";
                return false;
            default:
                LOG(ERROR) << "physical::recv unexpectedly returned unknown state.";
                return false;
        }
    }

    return true;
}

bool
hyperdaemon :: logical :: our_position(const regionid& r, entityid* e)
{
    mapiter f = m_mapping.lower_bound(entityid(r, 0));
    mapiter t = m_mapping.upper_bound(entityid(r, UINT8_MAX));

    for (; f != t; ++f)
    {
        if (f->second == m_us)
        {
            *e = f->first;
            break;
        }
    }

    return f != t;
}
