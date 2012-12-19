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

// STL
#include <algorithm>

// e
#include <e/unpacker.h>

// HyperDex
#include "common/serialization.h"
#include "coordinator/coordinator.h"
#include "coordinator/register.h"
#include "coordinator/util.h"

using namespace hyperdex;

extern "C"
{

void
hyperdex_coordinator_register(struct replicant_state_machine_context* ctx,
                              void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t id;
    po6::net::location bind_to;
    e::unpacker up(data, data_sz);
    up = up >> id >> bind_to;

    if (up.error())
    {
        return generate_response(ctx, c, hyperdex::COORD_MALFORMED);
    }

    std::pair<server_id, po6::net::location> target;
    target.first = server_id(id);

    if (std::lower_bound(c->servers.begin(),
                         c->servers.end(),
                         target) != c->servers.end())
    {
        return generate_response(ctx, c, hyperdex::COORD_DUPLICATE);
    }

    c->servers.push_back(std::make_pair(server_id(id), bind_to));
    std::sort(c->servers.begin(), c->servers.end());
    return generate_response(ctx, c, hyperdex::COORD_SUCCESS);
}

} // extern "C"
