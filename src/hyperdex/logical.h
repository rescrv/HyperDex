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

#ifndef hyperdex_logical_h_
#define hyperdex_logical_h_

// STL
#include <map>

// libev
#include <ev++.h>

// po6
#include <po6/net/location.h>
#include <po6/threads/rwlock.h>

// e
#include <e/buffer.h>

// HyperDex
#include <hyperdex/entity.h>
#include <hyperdex/physical.h>

namespace hyperdex
{

class logical
{
    public:
        logical(ev::loop_ref lr, const po6::net::ipaddr& us);
        ~logical();

    // Send and recv messages.
    public:
        bool send(const hyperdex::entity& from, const hyperdex::entity& to,
                  const uint8_t msg_type, const e::buffer& msg);
        bool recv(hyperdex::entity* from, hyperdex::entity* to,
                  uint8_t* msg_type, e::buffer* msg);
        void shutdown();

    private:
        struct instance
        {
            instance()
                : inbound(), inbound_version(), outbound(), outbound_version()
            {
            }

            bool operator == (const instance& rhs) const
            {
                const instance& lhs(*this);
                return lhs.inbound == rhs.inbound &&
                       lhs.inbound_version == rhs.inbound_version &&
                       lhs.outbound == rhs.outbound &&
                       lhs.outbound_version == rhs.outbound_version;
            }

            bool operator != (const instance& rhs) const
            {
                const instance& lhs(*this);
                return lhs.inbound != rhs.inbound &&
                       lhs.inbound_version != rhs.inbound_version &&
                       lhs.outbound != rhs.outbound &&
                       lhs.outbound_version != rhs.outbound_version;
            }

            po6::net::location inbound;
            uint16_t inbound_version;
            po6::net::location outbound;
            uint16_t outbound_version;
        };

    private:
        logical(const logical&);

    private:
        logical& operator = (const logical&);

    private:
        instance m_us;
        po6::threads::rwlock m_lock;
        std::map<entity, instance> m_mapping;
        hyperdex::physical m_physical;
};

} // namespace hyperdex

#endif // hyperdex_logical_h_
