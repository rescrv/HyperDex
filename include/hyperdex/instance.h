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
#include <po6/net/location.h>

// e
#include <e/tuple_compare.h>

namespace hyperdex
{

struct instance
{
    instance()
        : inbound()
        , inbound_version()
        , outbound()
        , outbound_version()
    {
    }

    instance(const po6::net::location& i, uint16_t iv,
             const po6::net::location& o, uint16_t ov)
        : inbound(i)
        , inbound_version(iv)
        , outbound(o)
        , outbound_version(ov)
    {
    }

    bool operator < (const instance& rhs) const
    {
        const instance& lhs(*this);
        return e::tuple_compare(lhs.inbound, lhs.inbound_version,
                                rhs.inbound, rhs.inbound_version) < 0;
    }

    bool operator == (const instance& rhs) const
    {
        const instance& lhs(*this);
        return e::tuple_compare(lhs.inbound, lhs.inbound_version,
                                rhs.inbound, rhs.inbound_version) == 0;
    }

    bool operator != (const instance& rhs) const
    {
        const instance& lhs(*this);
        return !(lhs == rhs);
    }

    po6::net::location inbound;
    uint16_t inbound_version;
    po6::net::location outbound;
    uint16_t outbound_version;
};

} // namespace hyperdex

#endif // hyperdex_instance_h_
