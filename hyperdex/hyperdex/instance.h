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

#ifndef hyperdex_instance_h_
#define hyperdex_instance_h_

// po6
#include <po6/net/ipaddr.h>

// e
#include <e/tuple_compare.h>

namespace hyperdex
{

struct instance
{
    instance()
        : address()
        , inbound_port()
        , inbound_version()
        , outbound_port()
        , outbound_version()
    {
    }

    instance(const po6::net::ipaddr& a,
             in_port_t ip,
             uint16_t iv,
             in_port_t op,
             uint16_t ov)
        : address(a)
        , inbound_port(ip)
        , inbound_version(iv)
        , outbound_port(op)
        , outbound_version(ov)
    {
    }

    bool operator < (const instance& rhs) const
    {
        const instance& lhs(*this);
        return e::tuple_compare(lhs.address, lhs.inbound_port, lhs.inbound_version, lhs.outbound_port, lhs.outbound_version,
                                rhs.address, rhs.inbound_port, rhs.inbound_version, rhs.outbound_port, rhs.outbound_version) < 0;
    }

    bool operator == (const instance& rhs) const
    {
        const instance& lhs(*this);
        return e::tuple_compare(lhs.address, lhs.inbound_port, lhs.inbound_version, lhs.outbound_port, lhs.outbound_version,
                                rhs.address, rhs.inbound_port, rhs.inbound_version, rhs.outbound_port, rhs.outbound_version) == 0;
    }

    bool operator != (const instance& rhs) const
    {
        return !(*this == rhs);
    }

    po6::net::ipaddr address;
    in_port_t inbound_port;
    uint16_t inbound_version;
    in_port_t outbound_port;
    uint16_t outbound_version;
};

} // namespace hyperdex

#endif // hyperdex_instance_h_
