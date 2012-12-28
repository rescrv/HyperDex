// Copyright (c) 2012, Cornell University
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

#ifdef HAVE_CONFIG_H
#include "config.h"
#endif

// Google Log
#include <glog/logging.h>

// HyperDex
#include "daemon/communication.h"
#include "daemon/daemon.h"

using hyperdex::communication;
using hyperdex::reconfigure_returncode;

//////////////////////////////// Early Messages ////////////////////////////////

class communication::early_message
{
    public:
        early_message();
        early_message(uint64_t version, uint64_t id,
                      std::auto_ptr<e::buffer> m);
        ~early_message() throw ();

    public:
        uint64_t config_version;
        uint64_t id;
        std::auto_ptr<e::buffer> msg;
};

communication :: early_message :: early_message()
    : config_version(0)
    , id()
    , msg()
{
}

communication :: early_message :: early_message(uint64_t v,
                                                uint64_t i,
                                                std::auto_ptr<e::buffer> m)
    : config_version(v)
    , id(i)
    , msg(m)
{
}

communication :: early_message :: ~early_message() throw ()
{
}

///////////////////////////////// Public Class /////////////////////////////////

communication :: communication(daemon* d)
    : m_daemon(d)
    , m_busybee_mapper(&m_daemon->m_config)
    , m_busybee()
    , m_early_messages()
{
}

communication :: ~communication() throw ()
{
}

bool
communication :: setup(const po6::net::location& bind_to,
                       unsigned threads)
{
    m_busybee.reset(new busybee_mta(&m_busybee_mapper, bind_to, m_daemon->m_us.get(), threads));
    return true;
}

void
communication :: teardown()
{
}

reconfigure_returncode
communication :: prepare(const configuration&,
                         const configuration&,
                         const server_id&)
{
    // XXX
    return RECONFIGURE_SUCCESS;
}

reconfigure_returncode
communication :: reconfigure(const configuration&,
                             const configuration&,
                             const server_id&)
{
    // XXX
    return RECONFIGURE_SUCCESS;
}

reconfigure_returncode
communication :: cleanup(const configuration&,
                         const configuration&,
                         const server_id&)
{
    // XXX
    return RECONFIGURE_SUCCESS;
}

bool
communication :: send(const virtual_server_id& from,
                      const server_id& to,
                      network_msgtype msg_type,
                      std::auto_ptr<e::buffer> msg)
{
    assert(msg->size() >= HYPERDEX_HEADER_SIZE_VS);

    if (m_daemon->m_us != m_daemon->m_config.get_server_id(from))
    {
        return false;
    }

    uint8_t mt = static_cast<uint8_t>(msg_type);
    msg->pack_at(BUSYBEE_HEADER_SIZE) << mt << from.get();

#ifdef HD_LOG_ALL_MESSAGES
    LOG(INFO) << "SEND " << from << "->" << to << " " << msg_type << " " << msg->hex();
#endif

    if (to == m_daemon->m_us)
    {
        m_busybee->deliver(to.get(), msg);
    }
    else
    {
        busybee_returncode rc = m_busybee->send(to.get(), msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_DISRUPTED:
                handle_disruption(to.get());
                return false;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_TIMEOUT:
            case BUSYBEE_EXTERNAL:
            default:
                LOG(ERROR) << "BusyBee unexpectedly returned " << rc;
                return false;
        }
    }

    return true;
}

bool
communication :: send(const virtual_server_id& from,
                      const virtual_server_id& vto,
                      network_msgtype msg_type,
                      std::auto_ptr<e::buffer> msg)
{
    assert(msg->size() >= HYPERDEX_HEADER_SIZE_VV);

    if (m_daemon->m_us != m_daemon->m_config.get_server_id(from))
    {
        return false;
    }

    uint8_t mt = static_cast<uint8_t>(msg_type);
    uint8_t flags = 1;
    msg->pack_at(BUSYBEE_HEADER_SIZE) << mt << flags << m_daemon->m_config.version() << vto.get() << from.get();
    server_id to = m_daemon->m_config.get_server_id(vto);

    if (to == server_id())
    {
        return false;
    }

#ifdef HD_LOG_ALL_MESSAGES
    LOG(INFO) << "SEND " << from << "->" << vto << " " << msg_type << " " << msg->hex();
#endif

    if (to == m_daemon->m_us)
    {
        m_busybee->deliver(to.get(), msg);
    }
    else
    {
        busybee_returncode rc = m_busybee->send(to.get(), msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_DISRUPTED:
                handle_disruption(to.get());
                return false;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_TIMEOUT:
            case BUSYBEE_EXTERNAL:
            default:
                LOG(ERROR) << "BusyBee unexpectedly returned " << rc;
                return false;
        }
    }

    return true;
}

bool
communication :: send(const virtual_server_id& vto,
                      network_msgtype msg_type,
                      std::auto_ptr<e::buffer> msg)
{
    assert(msg->size() >= HYPERDEX_HEADER_SIZE_SV);

    uint8_t mt = static_cast<uint8_t>(msg_type);
    uint8_t flags = 0;
    msg->pack_at(BUSYBEE_HEADER_SIZE) << mt << flags << m_daemon->m_config.version() << vto.get();
    server_id to = m_daemon->m_config.get_server_id(vto);

    if (to == server_id())
    {
        return false;
    }

#ifdef HD_LOG_ALL_MESSAGES
    LOG(INFO) << "SEND ->" << vto << " " << msg_type << " " << msg->hex();
#endif

    if (to == m_daemon->m_us)
    {
        m_busybee->deliver(to.get(), msg);
    }
    else
    {
        busybee_returncode rc = m_busybee->send(to.get(), msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_DISRUPTED:
                handle_disruption(to.get());
                return false;
            case BUSYBEE_SHUTDOWN:
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_TIMEOUT:
            case BUSYBEE_EXTERNAL:
            default:
                LOG(ERROR) << "BusyBee unexpectedly returned " << rc;
                return false;
        }
    }

    return true;
}

bool
communication :: recv(server_id* from,
                      virtual_server_id* vfrom,
                      virtual_server_id* vto,
                      network_msgtype* msg_type,
                      std::auto_ptr<e::buffer>* msg,
                      e::unpacker* up)
{
    // Read messages from the network until we get one that meets the following
    // constraints:
    //  - It is addressed to a virtual_server_id
    //  - The virtual_server_id destination maps to us
    //  - If it comes from a virtual_server_id, that id maps to the sender
    //  - The message version is less than or equal to our current config
    while (true)
    {
        uint64_t id;
        busybee_returncode rc = m_busybee->recv(&id, msg);

        switch (rc)
        {
            case BUSYBEE_SUCCESS:
                break;
            case BUSYBEE_SHUTDOWN:
                return false;
            case BUSYBEE_DISRUPTED:
                handle_disruption(id);
                continue;
            case BUSYBEE_POLLFAILED:
            case BUSYBEE_ADDFDFAIL:
            case BUSYBEE_TIMEOUT:
            case BUSYBEE_EXTERNAL:
            default:
                LOG(ERROR) << "busybee unexpectedly returned " << rc;
                continue;
        }

        uint8_t mt;
        uint8_t flags;
        uint64_t version;
        uint64_t vidf;
        uint64_t vidt;
        *up = (*msg)->unpack_from(BUSYBEE_HEADER_SIZE);
        *up = *up >> mt >> flags >> version >> vidt;
        *msg_type = static_cast<network_msgtype>(mt);
        *from = server_id(id);
        *vto = virtual_server_id(vidt);

        if ((flags & 0x1))
        {
            *up = *up >> vidf;
            *vfrom = virtual_server_id(vidf);
        }
        else
        {
            *vfrom = virtual_server_id();
        }

        if (up->error())
        {
            LOG(WARNING) << "dropping message that has a malformed header; here's some hex: " << (*msg)->hex();
            continue;
        }

        bool from_valid = true;
        bool to_valid = m_daemon->m_us == m_daemon->m_config.get_server_id(virtual_server_id(vidt));

        // If this is a virtual-virtual message
        if ((flags & 0x1))
        {
            from_valid = *from == m_daemon->m_config.get_server_id(virtual_server_id(vidf));
        }

        if (from_valid && to_valid)
        {
#ifdef HD_LOG_ALL_MESSAGES
            LOG(INFO) << "RECV " << *from << "->" << *vto << " " << *msg_type << " " << (*msg)->hex();
#endif
            return true;
        }

        // Shove the message back at the client so it fails with a reconfigure.
        if (!(flags & 0x1))
        {
            mt = static_cast<uint8_t>(CONFIGMISMATCH);
            (*msg)->pack_at(BUSYBEE_HEADER_SIZE) << mt;
            m_busybee->send(id, *msg);
        }
        else if (version > m_daemon->m_config.version())
        {
            early_message em(version, id, *msg);
            m_early_messages.push(em);
            continue;
        }
    }
}

void
communication :: handle_disruption(uint64_t)
{
    // XXX
}
