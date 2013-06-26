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

#ifndef hyperdex_coordinator_server_state_h_
#define hyperdex_coordinator_server_state_h_

// po6
#include <po6/net/location.h>

// HyperDex
#include "namespace.h"
#include "common/ids.h"

BEGIN_HYPERDEX_NAMESPACE

class server_state
{
    public:
        server_state();
        server_state(const server_id& _id,
                     const po6::net::location& _bind_to);
        ~server_state() throw ();

    public:
        server_id id;
        po6::net::location bind_to;
        enum { AVAILABLE, NOT_AVAILABLE, SHUTDOWN } state;
        // the most recent config that this server has acked
        uint64_t acked;
        // the most recent config for which this server was available if
        // this->state != AVAILABLE (undefined if this->state == AVAILABLE)
        uint64_t version;
};

inline
server_state :: server_state()
    : id()
    , bind_to()
    , state(NOT_AVAILABLE)
    , acked(0)
    , version(0)
{
}

inline
server_state :: server_state(const server_id& _id,
                             const po6::net::location& _bind_to)
    : id(_id)
    , bind_to(_bind_to)
    , state(AVAILABLE)
    , acked(0)
    , version(0)
{
}

inline
server_state :: ~server_state() throw ()
{
}

inline bool
operator < (const server_state& lhs, const server_state& rhs)
{
    return lhs.id < rhs.id;
}

bool
operator < (const server_state& lhs, const server_id& rhs)
{
    return lhs.id < rhs;
}

END_HYPERDEX_NAMESPACE

#endif // hyperdex_coordinator_server_state_h_
