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

// Google Log
#include <glog/logging.h>

// HyperDisk
#include <hyperdisk/disk.h>
#include <hyperdisk/returncode.h>

// HyperDaemon
#include "datalayer.h"
#include "logical.h"
#include "searches.h"

using hyperdex::coordinatorlink;
using hyperdex::entityid;
using hyperdex::regionid;
using hyperspacehashing::search;
using hyperspacehashing::mask::search_coordinate;

hyperdaemon :: searches :: searches(coordinatorlink* cl,
                                    datalayer* data,
                                    logical* comm)
    : m_cl(cl)
    , m_data(data)
    , m_comm(comm)
    , m_config()
    , m_searches(16)
{
}

hyperdaemon :: searches :: ~searches() throw ()
{
}

void
hyperdaemon :: searches :: prepare(const hyperdex::configuration&,
                                   const hyperdex::instance&)
{
}

void
hyperdaemon :: searches :: reconfigure(const hyperdex::configuration& newconfig,
                                       const hyperdex::instance&)
{
    m_config = newconfig;
}

void
hyperdaemon :: searches :: cleanup(const hyperdex::configuration&,
                                   const hyperdex::instance&)
{
}

void
hyperdaemon :: searches :: start(const hyperdex::entityid& client,
                                 uint64_t searchid,
                                 const hyperdex::regionid& region,
                                 uint64_t nonce,
                                 const hyperspacehashing::search& terms)
{
    std::pair<entityid, uint64_t> key(client, searchid);

    if (m_searches.contains(key))
    {
        return;
    }

    hyperspacehashing::mask::hasher hasher(m_config.disk_hasher(region.get_subspace()));
    hyperspacehashing::mask::search_coordinate coord(hasher.hash(terms));
    e::intrusive_ptr<hyperdisk::snapshot> snap = m_data->make_snapshot(region, terms);
    e::intrusive_ptr<search_state> state = new search_state(region, coord, snap);
    m_searches.insert(key, state);
    next(client, searchid, nonce);
}

void
hyperdaemon :: searches :: next(const hyperdex::entityid& client, uint64_t searchid, uint64_t nonce)
{
    e::intrusive_ptr<search_state> state;

    if (!m_searches.lookup(std::make_pair(client, searchid), &state))
    {
        return;
    }

    po6::threads::mutex::hold hold(&state->lock);

    while (state->snap->valid())
    {
        if (state->search_coord.matches(state->snap->coordinate()))
        {
            if (state->search_coord.matches(state->snap->key(), state->snap->value()))
            {
                e::buffer msg;
                msg.pack() << nonce << state->snap->key() << state->snap->value();
                m_comm->send(state->region, client, hyperdex::RESP_SEARCH_ITEM, msg);
                state->snap->next();
                return;
            }
        }

        state->snap->next();
    }

    e::buffer msg;
    msg.pack() << nonce;
    m_comm->send(state->region, client, hyperdex::RESP_SEARCH_DONE, msg);
    stop(client, searchid);
}

void
hyperdaemon :: searches :: stop(const hyperdex::entityid& client,
                                uint64_t searchid)
{
    m_searches.remove(std::make_pair(client, searchid));
}

uint64_t
hyperdaemon :: searches :: hash(const std::pair<entityid, uint32_t>& key)
{
    return key.first.hash() + key.second;
}


hyperdaemon :: searches :: search_state :: search_state(const regionid& r,
                                                        const search_coordinate& sc,
                                                        e::intrusive_ptr<hyperdisk::snapshot> s)
    : lock()
    , region(r)
    , search_coord(sc)
    , snap(s)
    , m_ref(0)
{
}

hyperdaemon :: searches :: search_state :: ~search_state() throw ()
{
}
