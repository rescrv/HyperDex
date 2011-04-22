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

// HyperDex
#include <hyperdex/logical.h>

hyperdex :: logical :: logical(ev::loop_ref lr,
                               const po6::net::location& us,
                               const uint16_t version)
    : m_us(std::make_pair(us, version))
    , m_lock()
    , m_mapping()
    , m_physical(lr, us)
{
}

hyperdex :: logical :: ~logical()
{
}

void
hyperdex :: logical :: map(const hyperdex::entity& log,
                           const po6::net::location& physical,
                           const uint16_t version)
{
    m_lock.wrlock();
    m_mapping[log] = std::make_pair(physical, version);
    m_lock.unlock();
}

void
hyperdex :: logical :: unmap(const hyperdex::entity& log)
{
    m_lock.wrlock();
    m_mapping.erase(log);
    m_lock.unlock();
}

typedef std::map<hyperdex::entity, std::pair<po6::net::location, uint16_t> >::iterator mapiter;

#define HEADER_SIZE 26
#define LOCVER_A_OFFSET 2
#define LOCVER_B_OFFSET 4
#define ENTITY_A_OFFSET 6
#define ENTITY_B_OFFSET 16

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
    hyperdex::entity::entity_type a = from.serialize(buf + ENTITY_A_OFFSET);
    hyperdex::entity::entity_type b = to.serialize(buf + ENTITY_B_OFFSET);
    buf[0] = msg_type;
    buf[1] = static_cast<uint8_t>(a) | (static_cast<uint8_t>(b) << 4);
    uint16_t fromver = htons(f->second.second);
    uint16_t tover = htons(t->second.second);
    memmove(&finalmsg.front() + LOCVER_A_OFFSET, &fromver, sizeof(uint16_t));
    memmove(&finalmsg.front() + LOCVER_B_OFFSET, &tover, sizeof(uint16_t));
    memmove(&finalmsg.front() + HEADER_SIZE, &msg.front(), msg.size());
    m_physical.send(t->second.first, finalmsg);
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

    do
    {
        if (!m_physical.recv(&loc, msg))
        {
            return false;
        }

        hyperdex::entity::entity_type a = static_cast<hyperdex::entity::entity_type>((*msg)[1] & 0x0f);
        hyperdex::entity::entity_type b = static_cast<hyperdex::entity::entity_type>(((*msg)[1] & 0xf0) >> 4);
        *from = entity(a, &msg->front() + ENTITY_A_OFFSET);
        *to = entity(b, &msg->front() + ENTITY_B_OFFSET);
        f = m_mapping.find(*from);
        t = m_mapping.find(*to);

        memmove(&fromver, &msg->front() + LOCVER_A_OFFSET, sizeof(uint16_t));
        memmove(&tover, &msg->front() + LOCVER_B_OFFSET, sizeof(uint16_t));
        fromver = ntohs(fromver);
        tover = ntohs(tover);
    }
    while (f == m_mapping.end() || t == m_mapping.end() ||
           f->second != std::make_pair(loc, fromver) || t->second != m_us ||
           m_us.second != tover);

    *msg_type = static_cast<uint8_t>((*msg)[0]);
    memmove(&msg->front(), &msg->front() + HEADER_SIZE, msg->size() - HEADER_SIZE);
    msg->resize(msg->size() - HEADER_SIZE);
    m_lock.unlock();
    return true;
}
