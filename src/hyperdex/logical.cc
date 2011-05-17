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

// STL
#include <stdexcept>

// HyperDex
#include <hyperdex/logical.h>

using e::buffer;
using po6::net::location;
using po6::threads::rwlock;

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

typedef std::map<hyperdex::entity, hyperdex::configuration::instance>::iterator mapiter;

bool
hyperdex :: logical :: send(const hyperdex::entity& from, const hyperdex::entity& to,
                            const uint8_t msg_type,
                            const e::buffer& msg)
{
    rwlock::rdhold rd(&m_lock);
    mapiter f = m_mapping.find(from);
    mapiter t = m_mapping.find(to);

    if (f == m_mapping.end() || t == m_mapping.end() || f->second != m_us)
    {
        return false;
    }

    buffer finalmsg(msg.size() + 32);
    buffer::packer(&finalmsg) << msg_type
                              << f->second.outbound_version
                              << t->second.inbound_version
                              << from
                              << to
                              << msg;
    m_physical.send(t->second.inbound, finalmsg);
    return true;
}

bool
hyperdex :: logical :: recv(hyperdex::entity* from, hyperdex::entity* to,
                            uint8_t* msg_type,
                            e::buffer* msg)
{
    rwlock::rdhold rd(&m_lock);
    location loc;
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
        buffer packed;
        buffer::unpacker up(packed);

        if (!m_physical.recv(&loc, &packed))
        {
            return false;
        }

        if (packed.size() < sizeof(uint8_t) + 2 * sizeof(uint16_t) +
                         2 * entity::SERIALIZEDSIZE)
        {
            continue;
        }

        // This should not throw thanks to the size check above.
        msg->clear();
        up >> *msg_type >> fromver >> tover >> *from >> *to >> *msg;
        f = m_mapping.find(*from);
        t = m_mapping.find(*to);
    }
    while (f == m_mapping.end() || t == m_mapping.end() ||
           f->second.outbound != loc || f->second.outbound_version != fromver ||
           t->second != m_us || m_us.inbound_version != tover);
    return true;
}

void
hyperdex :: logical :: shutdown()
{
    m_physical.shutdown();
}
