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
#include <list>
#include <stdexcept>

// Google Log
#include <glog/logging.h>

// BusyBee
#include <busybee_constants.h>

// HyperDex
#include "hyperdex/hyperdex/coordinatorlink.h"

// HyperDaemon
#include "hyperdaemon/logical.h"

using hyperdex::configuration;
using hyperdex::coordinatorlink;
using hyperdex::entityid;
using hyperdex::instance;
using hyperdex::network_msgtype;
using hyperdex::regionid;

//////////////////////////////// Early Messages ////////////////////////////////

class hyperdaemon::logical::early_message
{
    public:
        early_message();
        early_message(uint64_t v,
                      const po6::net::location& l,
                      std::auto_ptr<e::buffer> m);
        ~early_message() throw ();

    public:
        uint64_t config_version;
        po6::net::location loc;
        std::auto_ptr<e::buffer> msg;
};

hyperdaemon :: logical :: early_message :: early_message()
    : config_version(0)
    , loc()
    , msg()
{
}

hyperdaemon :: logical :: early_message :: early_message(uint64_t v,
                                                         const po6::net::location& l,
                                                         std::auto_ptr<e::buffer> m)
    : config_version(v)
    , loc(l)
    , msg(m)
{
}

hyperdaemon :: logical :: early_message :: ~early_message() throw ()
{
}

///////////////////////////////// Public Class /////////////////////////////////

hyperdaemon :: logical :: logical(coordinatorlink* cl, const po6::net::ipaddr& ip,
                                  in_port_t incoming, in_port_t outgoing, size_t num_threads)
    : m_cl(cl)
    , m_us()
    , m_config()
    , m_early_messages()
    , m_client_nums()
    , m_client_locs()
    , m_client_counter(0)
    , m_busybee(ip, incoming, outgoing, num_threads)
{
    assert(m_busybee.inbound().address == m_busybee.outbound().address);
    m_us.address = m_busybee.inbound().address;
    m_us.inbound_port = m_busybee.inbound().port;
    m_us.outbound_port = m_busybee.outbound().port;
    m_us.inbound_version = 0;
    m_us.outbound_version = 0;
}

hyperdaemon :: logical :: ~logical() throw ()
{
}

typedef std::map<hyperdex::entityid, hyperdex::instance>::iterator mapiter;

size_t
hyperdaemon :: logical :: header_size() const
{
    return sizeof(uint8_t) + BUSYBEE_HEADER_SIZE + sizeof(uint64_t) + 2 * sizeof(uint16_t) + 2 * entityid::SERIALIZEDSIZE;
}

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
    m_us = newinst;

    e::lockfree_fifo<early_message> ems;
    early_message em;

    while (m_early_messages.pop(&em))
    {
        if (em.config_version <= m_config.version())
        {
            m_busybee.deliver(em.loc, em.msg);
        }
        else
        {
            ems.push(em);
        }
    }

    while (ems.pop(&em))
    {
        m_early_messages.push(em);
    }
}

void
hyperdaemon :: logical :: cleanup(const configuration&, const hyperdex::instance&)
{
    // Do nothing.
}

bool
hyperdaemon :: logical :: send(const hyperdex::entityid& from,
                               const hyperdex::entityid& to,
                               const network_msgtype msg_type,
                               std::auto_ptr<e::buffer> msg)
{
#ifdef HD_LOG_ALL_MESSAGES
    LOG(INFO) << "SEND " << from << "->" << to << " " << msg_type << " " << msg->hex();
#endif
    instance src;
    instance dst;

    // If we are sending as a transfer-recipient
    if (from.space == hyperdex::configuration::TRANSFERSPACE)
    {
        src = m_config.instancefortransfer(from.subspace);
    }
    else
    {
        src = m_config.instancefor(from);
    }

    // If we are sending to a client
    if (to.space == hyperdex::configuration::CLIENTSPACE)
    {
        po6::net::location loc;

        if (!m_client_locs.lookup(to.mask, &loc))
        {
            return false;
        }

        dst.address = loc.address;
        dst.inbound_port = loc.port;
        dst.inbound_version = 1;
    }
    else if (to.space == hyperdex::configuration::TRANSFERSPACE)
    {
        dst = m_config.instancefortransfer(to.subspace);
    }
    else
    {
        dst = m_config.instancefor(to);
    }

    if (src != m_us || dst == instance())
    {
        return false;
    }

    uint8_t mt = static_cast<uint8_t>(msg_type);
    assert(msg->capacity() >= header_size());
    msg->pack_at(BUSYBEE_HEADER_SIZE)
        << mt << m_config.version() << src.outbound_version << dst.inbound_version << from << to;

    if (dst == m_us)
    {
        m_busybee.deliver(po6::net::location(m_us.address, m_us.outbound_port), msg);
    }
    else
    {
        po6::net::location loc(dst.address, dst.inbound_port);

        switch (m_busybee.send(loc, msg))
        {
            case BUSYBEE_SUCCESS:
            case BUSYBEE_QUEUED:
                break;
            case BUSYBEE_SHUTDOWN:
                LOG(ERROR) << "busybee unexpectedly returned SHUTDOWN";
                return false;
            case BUSYBEE_POLLFAILED:
                PLOG(ERROR) << "busybee unexpectedly returned POLLFAILED";
                return false;
            case BUSYBEE_DISCONNECT:
                handle_disconnect(loc);
                return false;
            case BUSYBEE_CONNECTFAIL:
                handle_connectfail(loc);
                return false;
            case BUSYBEE_ADDFDFAIL:
                handle_connectfail(loc);
                return false;
            case BUSYBEE_BUFFERFULL:
                return false;
            case BUSYBEE_TIMEOUT:
                return false;
            case BUSYBEE_EXTERNAL:
                return false;
            default:
                LOG(ERROR) << "busybee wandered into bad state, and returned garbage";
                return false;
        }
    }

    return true;
}

bool
hyperdaemon :: logical :: recv(hyperdex::entityid* from,
                               hyperdex::entityid* to,
                               network_msgtype* msg_type,
                               std::auto_ptr<e::buffer>* msg)
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
    while (true)
    {
        switch(m_busybee.recv(&loc, msg))
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_SHUTDOWN:
                return false;
            case BUSYBEE_QUEUED:
                LOG(ERROR) << "busybee unexpectedly returned QUEUED";
                continue;
            case BUSYBEE_POLLFAILED:
                PLOG(ERROR) << "busybee unexpectedly returned POLLFAILED";
                continue;
            case BUSYBEE_DISCONNECT:
                handle_disconnect(loc);
                continue;
            case BUSYBEE_CONNECTFAIL:
                handle_connectfail(loc);
                continue;
            case BUSYBEE_ADDFDFAIL:
                PLOG(ERROR) << "busybee unexpectedly returned ADDFDFAIL";
                continue;
            case BUSYBEE_BUFFERFULL:
                LOG(ERROR) << "busybee unexpectedly returned BUFFERFULL";
                continue;
            case BUSYBEE_TIMEOUT:
                PLOG(ERROR) << "busybee unexpectedly returned TIMEOUT";
                continue;
            case BUSYBEE_EXTERNAL:
                LOG(ERROR) << "busybee unexpectedly returned EXTERNAL";
                continue;
            default:
                LOG(ERROR) << "busybee unexpectedly returned unknown state.";
                continue;
        }

        if ((*msg)->size() < header_size())
        {
            LOG(WARNING) << "dropping message that is too small to parse: " << (*msg)->hex();
            continue;
        }

        // This should not throw thanks to the size check above.
        uint8_t mt;
        uint64_t version;
        (*msg)->unpack_from(sizeof(uint32_t))
            >> mt >> version >> fromver >> tover >> *from >> *to;
        *msg_type = static_cast<network_msgtype>(mt);

        // Checkout the sender
        if (from->space == hyperdex::configuration::CLIENTSPACE)
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

            frominst.address = loc.address;
            frominst.outbound_port = loc.port;
            frominst.outbound_version = fromver;
            fromvalid = true;
        }
        else if (from->space == hyperdex::configuration::TRANSFERSPACE)
        {
            frominst = m_config.instancefortransfer(from->subspace);
            fromvalid = frominst != instance();
        }
        else
        {
            frominst = m_config.instancefor(*from);
            fromvalid = frominst != instance();
        }

        // Checkout the receiver
        if (to->space == hyperdex::configuration::TRANSFERSPACE)
        {
            toinst = m_config.instancefortransfer(to->subspace);
            tovalid = toinst != instance();
        }
        else
        {
            toinst = m_config.instancefor(*to);
            tovalid = toinst != instance();
        }

        if (fromvalid && // Try again because we don't know the source.
            tovalid && // Try again because we don't know the destination.
            frominst.address == loc.address && // Try again because the sender isn't who it should be.
            frominst.outbound_port == loc.port && // Try again because the sender isn't who it should be.
            frominst.outbound_version == fromver && // Try again because an older sender is sending the message.
            toinst == m_us && // Try again because we don't believe ourselves to be the dest entity.
            m_us.inbound_version == tover) // Try again because it is to an older version of us.
        {
            break;
        }

        // Shove the message back at the client so it fails with a reconfigure.
        if (from->space == hyperdex::configuration::CLIENTSPACE)
        {
            mt = static_cast<uint8_t>(hyperdex::CONFIGMISMATCH);
            (*msg)->pack_at(sizeof(uint32_t))
                << mt << m_config.version() << tover << fromver << *to << *from;
            m_busybee.send(loc, *msg);
        }
        // Otherwise, it's an early arrival.  We should postpone it, because it
        // could become valid in the future.
        else if (version > m_config.version())
        {
            early_message em(version, loc, *msg);
            assert(em.msg.get());
            m_early_messages.push(em);
            continue;
        }
    }

#ifdef HD_LOG_ALL_MESSAGES
    LOG(INFO) << "RECV " << *from << "->" << *to << " " << *msg_type << " " << (*msg)->hex();
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
        switch (m_cl->warn_location(loc))
        {
            case hyperdex::coordinatorlink::SUCCESS:
                break;
            case hyperdex::coordinatorlink::CONNECTFAIL:
                LOG(WARNING) << "Could not report connection failure to " << loc << ":  error(CONNECTFAIL)";
                break;
            default:
                abort();
        }
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
        switch (m_cl->fail_location(loc))
        {
            case hyperdex::coordinatorlink::SUCCESS:
                break;
            case hyperdex::coordinatorlink::CONNECTFAIL:
                LOG(WARNING) << "Could not report disconnect from " << loc << ":  error(CONNECTFAIL)";
                break;
            default:
                abort();
        }
    }
}
