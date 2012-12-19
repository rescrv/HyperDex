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

// e
#include <e/endian.h>

// Replicant
#include <replicant_state_machine.h>

// HyperDex
#include "common/coordinator_returncode.h"
#include "common/hyperspace.h"
#include "common/region_id.h"
#include "common/server_id.h"
#include "common/subspace_id.h"
#include "coordinator/add-space.h"
#include "coordinator/coordinator.h"
#include "coordinator/util.h"

using hyperdex::coordinator;
using hyperdex::region_id;
using hyperdex::space;
using hyperdex::space_id;
using hyperdex::subspace_id;

extern "C"
{

void
hyperdex_coordinator_add_space(struct replicant_state_machine_context* ctx,
                               void* obj, const char* data, size_t data_sz)
{
    PROTECT_UNINITIALIZED;
    coordinator* c = static_cast<coordinator*>(obj);
    space s;
    e::unpacker up(data, data_sz);
    up = up >> s;

    if (up.error())
    {
        return generate_response(ctx, c, hyperdex::COORD_MALFORMED);
    }

    // Check that a space with this name doesn't already exist
    for (std::list<space>::iterator it = c->spaces.begin(); it != c->spaces.end(); ++it)
    {
        if (it->name == s.name)
        {
            return generate_response(ctx, c, hyperdex::COORD_DUPLICATE);
        }
    }

    // Assign unique ids to each newly created object and clear the replicas
    s.id = space_id(c->counter);
    ++c->counter;

    for (size_t i = 0; i < s.subspaces.size(); ++i)
    {
        s.subspaces[i].id = subspace_id(c->counter);
        ++c->counter;

        for (size_t j = 0; j < s.subspaces[i].regions.size(); ++j)
        {
            s.subspaces[i].regions[j].id = region_id(c->counter);
            ++c->counter;
            s.subspaces[i].regions[j].replicas.clear();
        }
    }

    // XXX we trust that regions completely fill all space.  we should validate
    // this ourselves rather than trusting input
    c->spaces.push_back(s);
    c->initial_layout(&c->spaces.back());
    c->regenerate();
    return generate_response(ctx, c, hyperdex::COORD_SUCCESS);
}

} // extern "C"
