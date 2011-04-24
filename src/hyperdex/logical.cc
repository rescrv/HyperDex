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
#include <stdexcept>

// HyperDex
#include <hyperdex/logical.h>

hyperdex :: logical :: logical(ev::loop_ref lr,
                               const po6::net::ipaddr& ip)
    : m_us()
    , m_lock()
    , m_mapping()
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

void
hyperdex :: logical :: map(const hyperdex::entity& log,
                           const instance& where)
{
    m_lock.wrlock();
    m_mapping[log] = where;
    m_lock.unlock();
}

void
hyperdex :: logical :: unmap(const hyperdex::entity& log)
{
    m_lock.wrlock();
    m_mapping.erase(log);
    m_lock.unlock();
}

typedef std::map<hyperdex::entity, hyperdex::logical::instance>::iterator mapiter;

#define VERSION_A_OFFSET (sizeof(uint8_t))
#define VERSION_B_OFFSET (VERSION_A_OFFSET + sizeof(uint16_t))
#define ENTITY_A_OFFSET (VERSION_B_OFFSET + sizeof(uint16_t))
#define ENTITY_B_OFFSET (ENTITY_A_OFFSET + entity::SERIALIZEDSIZE)
#define HEADER_SIZE (ENTITY_B_OFFSET + entity::SERIALIZEDSIZE)

bool
hyperdex :: logical :: send(const hyperdex::entity& from, const hyperdex::entity& to,
                            const uint8_t msg_type,
                            const std::vector<char>& msg)
{
    m_lock.rdlock();
    mapiter f = m_mapping.find(from);
    mapiter t = m_mapping.find(to);

    if (f == m_mapping.end() || t == m_mapping.end() || f->second != m_us)
    {
        m_lock.unlock();
        return false;
    }

    std::vector<char> finalmsg(msg.size() + HEADER_SIZE);
    char* buf = &finalmsg.front();
    buf[0] = msg_type;
    uint16_t fromver = htons(f->second.outbound_version);
    memmove(buf + VERSION_A_OFFSET, &fromver, sizeof(uint16_t));
    uint16_t tover = htons(t->second.inbound_version);
    memmove(buf + VERSION_B_OFFSET, &tover, sizeof(uint16_t));
    from.serialize(buf + ENTITY_A_OFFSET);
    to.serialize(buf + ENTITY_B_OFFSET);
    memmove(buf + HEADER_SIZE, &msg.front(), msg.size());
    m_physical.send(t->second.inbound, finalmsg);
    m_lock.unlock();
    return true;
}

bool
hyperdex :: logical :: recv(hyperdex::entity* from, hyperdex::entity* to,
                            uint8_t* msg_type,
                            std::vector<char>* msg)
{
    m_lock.rdlock();
    po6::net::location loc;
    mapiter f;
    mapiter t;
    uint16_t fromver;
    uint16_t tover;

    // Read messages from the network until we get one which meets the following
    // constraints:
    //  - We have a mapping for the source entity.
    //  - We have a mapping for the destination entity.
    //  - We have a mapping for the source entity which corresponds to the
    //    underlying network location and version number of this message.
    //  - The destination entity maps to our network location.
    //  - The message is to the correct version of our port bindings.
    do
    {
        if (!m_physical.recv(&loc, msg))
        {
            return false;
        }

        try
        {
            *from = entity(&msg->front() + ENTITY_A_OFFSET);
            *to = entity(&msg->front() + ENTITY_B_OFFSET);
        }
        catch (std::invalid_argument& e)
        {
            continue;
        }

        f = m_mapping.find(*from);
        t = m_mapping.find(*to);
        memmove(&fromver, &msg->front() + VERSION_A_OFFSET, sizeof(uint16_t));
        memmove(&tover, &msg->front() + VERSION_B_OFFSET, sizeof(uint16_t));
        fromver = ntohs(fromver);
        tover = ntohs(tover);
    }
    while (f == m_mapping.end() || t == m_mapping.end() ||
           f->second.outbound != loc || f->second.outbound_version != fromver ||
           t->second != m_us || m_us.inbound_version != tover);

    *msg_type = static_cast<uint8_t>((*msg)[0]);
    memmove(&msg->front(), &msg->front() + HEADER_SIZE, msg->size() - HEADER_SIZE);
    msg->resize(msg->size() - HEADER_SIZE);
    m_lock.unlock();
    return true;
}
