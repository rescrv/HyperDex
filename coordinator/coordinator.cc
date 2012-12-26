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

// po6
#include <po6/net/location.h>

// HyperDex
#include "common/serialization.h"
#include "coordinator/coordinator.h"

using hyperdex::coordinator;

coordinator :: coordinator()
    : version(0)
    , counter(1)
    , latest_config()
    , servers()
    , spaces()
    , m_seed()
{
    memset(&m_seed, 0, sizeof(m_seed));
    srand48_r(0, &m_seed);
}

coordinator :: ~coordinator() throw ()
{
}

void
coordinator :: initial_layout(space* s)
{
    if (servers.empty()) return;

    for (size_t i = 0; i < s->subspaces.size(); ++i)
    {
        for (size_t j = 0; j < s->subspaces[i].regions.size(); ++j)
        {
            s->subspaces[i].regions[j].replicas.clear();

            for (size_t num_replicas = 0; num_replicas < 3; ++num_replicas)
            {
                server_id x;

                for (size_t retry = 0; retry < 10; ++retry)
                {
                    long int y;
                    lrand48_r(&m_seed, &y);
                    bool found = false;

                    for (size_t k = 0; k < s->subspaces[i].regions[j].replicas.size(); ++k)
                    {
                        if (servers[y % servers.size()].first == s->subspaces[i].regions[j].replicas[k].si)
                        {
                            found = true;
                        }
                    }

                    if (found)
                    {
                        continue;
                    }

                    x = servers[y % servers.size()].first;
                    s->subspaces[i].regions[j].replicas.push_back(replica());
                    s->subspaces[i].regions[j].replicas.back().si = x;
                    s->subspaces[i].regions[j].replicas.back().vsi = virtual_server_id(counter);
                    ++counter;
                    break;
                }
            }
        }
    }
}

void
coordinator :: regenerate(struct replicant_state_machine_context* ctx)
{
    uint64_t cond_state;

    if (replicant_state_machine_condition_broadcast(ctx, "config", &cond_state) < 0)
    {
        replicant_state_machine_log_error(ctx, "could not broadcast on \"config\" condition");
    }

    ++version;
    size_t sz = 3 * sizeof(uint64_t);

    for (size_t i = 0; i < servers.size(); ++i)
    {
        sz += sizeof(uint64_t) + pack_size(servers[i].second);
    }
              
    for (std::list<space>::iterator it = spaces.begin();
            it != spaces.end(); ++it)
    {
        sz += pack_size(*it);
    }

    std::auto_ptr<e::buffer> new_config(e::buffer::create(sz));
    e::buffer::packer pa = new_config->pack_at(0);
    pa = pa << version << uint64_t(servers.size()) << uint64_t(spaces.size());

    for (size_t i = 0; i < servers.size(); ++i)
    {
        pa = pa << servers[i].first.get() << servers[i].second;
    }

    for (std::list<space>::iterator it = spaces.begin();
            it != spaces.end(); ++it)
    {
        pa = pa << *it;
    }

    latest_config = new_config;
}
