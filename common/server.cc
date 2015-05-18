// Copyright (c) 2013, Cornell University
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
#include "common/serialization.h"
#include "common/server.h"

using hyperdex::server;

const char*
server :: to_string(state_t state)
{
    switch (state)
    {
        case ASSIGNED:
            return "ASSIGNED";
        case NOT_AVAILABLE:
            return "NOT_AVAILABLE";
        case AVAILABLE:
            return "AVAILABLE";
        case SHUTDOWN:
            return "SHUTDOWN";
        case KILLED:
            return "KILLED";
        default:
            return "UNKNOWN";
    }
}

server :: server()
    : state(KILLED)
    , id()
    , bind_to()
{
}

server :: server(const server_id& sid)
    : state(ASSIGNED)
    , id(sid)
    , bind_to()
{
}

bool
hyperdex :: operator < (const server& lhs, const server& rhs)
{
    return lhs.id < rhs.id;
}

e::packer
hyperdex :: operator << (e::packer lhs, const server& rhs)
{
    uint8_t s = static_cast<uint8_t>(rhs.state);
    return lhs << s << rhs.id << rhs.bind_to;
}

e::unpacker
hyperdex :: operator >> (e::unpacker lhs, server& rhs)
{
    uint8_t s;
    lhs = lhs >> s >> rhs.id >> rhs.bind_to;
    rhs.state = static_cast<server::state_t>(s);
    return lhs;
}

size_t
hyperdex :: pack_size(const server& p)
{
    return sizeof(uint8_t)
         + sizeof(uint64_t)
         + pack_size(p.bind_to);
}
