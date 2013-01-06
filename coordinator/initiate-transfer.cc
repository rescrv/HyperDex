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

// HyperDex
#include "coordinator/coordinator.h"
#include "coordinator/initiate-transfer.h"
#include "coordinator/util.h"

using hyperdex::capture_id;
using hyperdex::coordinator;
using hyperdex::region;
using hyperdex::region_id;
using hyperdex::server_id;
using hyperdex::space;
using hyperdex::transfer_id;
using hyperdex::virtual_server_id;

extern "C"
{

void
hyperdex_coordinator_initiate_transfer(struct replicant_state_machine_context* ctx,
                                       void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    coordinator* c = static_cast<coordinator*>(obj);
    uint64_t _rid;
    uint64_t _sid;
    e::unpacker up(data, data_sz);
    up = up >> _rid >> _sid;
    region_id rid(_rid);
    server_id sid(_sid);

    if (up.error())
    {
        return generate_response(ctx, c, hyperdex::COORD_MALFORMED);
    }

    bool found = false;

    for (size_t i = 0; i < c->servers.size(); ++i)
    {
        if (c->servers[i].first == sid)
        {
            found = true;
            break;
        }
    }

    if (!found)
    {
        return generate_response(ctx, c, hyperdex::COORD_NOT_FOUND);
    }

    for (std::list<space>::iterator it = c->spaces.begin(); it != c->spaces.end(); ++it)
    {
        for (size_t i = 0; i < it->subspaces.size(); ++i)
        {
            for (size_t j = 0; j < it->subspaces[i].regions.size(); ++j)
            {
                if (it->subspaces[i].regions[j].id != rid)
                {
                    continue;
                }

                region& r(it->subspaces[i].regions[j]);

                if (r.tid != transfer_id())
                {
                    return generate_response(ctx, c, hyperdex::COORD_TRANSFER_IN_PROGRESS);
                }

                r.cid = capture_id(c->counter);
                ++c->counter;
                r.tid = transfer_id(c->counter);
                ++c->counter;
                r.tsi = sid;
                r.tvi = virtual_server_id(c->counter);
                ++c->counter;
                c->regenerate(ctx);
                return generate_response(ctx, c, hyperdex::COORD_SUCCESS);
            }
        }
    }

    return generate_response(ctx, c, hyperdex::COORD_NOT_FOUND);
}

} // extern "C"
