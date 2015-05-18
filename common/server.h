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

#ifndef hyperdex_common_server_h_
#define hyperdex_common_server_h_

// po6
#include <po6/net/location.h>

// HyperDex
#include "namespace.h"
#include "common/ids.h"

BEGIN_HYPERDEX_NAMESPACE

class server
{
    public:
        enum state_t
        {
            ASSIGNED = 1,
            NOT_AVAILABLE = 2,
            AVAILABLE = 3,
            SHUTDOWN = 4,
            KILLED = 5
        };
        static const char* to_string(state_t state);

    public:
        server();
        explicit server(const server_id&);

    public:
        state_t state;
        server_id id;
        po6::net::location bind_to;
};

bool
operator < (const server& lhs, const server& rhs);

e::packer
operator << (e::packer lhs, const server& rhs);
e::unpacker
operator >> (e::unpacker lhs, server& rhs);
size_t
pack_size(const server& p);

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_server_h_
