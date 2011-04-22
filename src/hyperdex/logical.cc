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

hyperdex :: logical :: logical(ev::loop_ref lr, const po6::net::location& us)
    : m_us(us)
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
                           const po6::net::location& physical)
{
    m_lock.wrlock();
    m_mapping[log] = physical;
    m_lock.unlock();
}

void
hyperdex :: logical :: unmap(const hyperdex::entity& log)
{
    m_lock.wrlock();
    m_mapping.erase(log);
    m_lock.unlock();
}

bool
hyperdex :: logical :: send(const hyperdex::entity& from, const hyperdex::entity& to,
                            const uint8_t msg_type,
                            const std::vector<char>& msg)
{
    m_lock.rdlock();
    std::map<entity, po6::net::location>::iterator f = m_mapping.find(from);
    std::map<entity, po6::net::location>::iterator t = m_mapping.find(to);

    if (f == m_mapping.end() || t == m_mapping.end() || f->second != m_us)
    {
        m_lock.unlock();
        return false;
    }

    std::vector<char> finalmsg(msg.size() + 22);
    char* buf = &finalmsg.front();
    hyperdex::entity::entity_type a = from.serialize(buf + 2);
    hyperdex::entity::entity_type b = to.serialize(buf + 12);
    buf[0] = msg_type;
    buf[1] = static_cast<uint8_t>(a) | (static_cast<uint8_t>(b) << 4);
    memmove(&finalmsg.front() + 22, &msg.front(), msg.size());
    m_physical.send(t->second, finalmsg);
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
    std::map<entity, po6::net::location>::iterator f;
    std::map<entity, po6::net::location>::iterator t;

    do
    {
        if (!m_physical.recv(&loc, msg))
        {
            return false;
        }

        hyperdex::entity::entity_type a = static_cast<hyperdex::entity::entity_type>((*msg)[1] & 0x0f);
        hyperdex::entity::entity_type b = static_cast<hyperdex::entity::entity_type>(((*msg)[1] & 0xf0) >> 4);
        *from = entity(a, &msg->front() + 2);
        *to = entity(b, &msg->front() + 12);
        f = m_mapping.find(*from);
        t = m_mapping.find(*to);
    }
    while (f == m_mapping.end() || t == m_mapping.end() ||
           f->second != loc || t->second != m_us);

    *msg_type = static_cast<uint8_t>((*msg)[0]);
    memmove(&msg->front(), &msg->front() + 22, msg->size() - 22);
    msg->resize(msg->size() - 22);
    m_lock.unlock();
    return true;
}
