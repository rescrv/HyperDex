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

// HyperDex
#include <hyperdex/searches.h>

hyperdex :: searches :: searches(datalayer* data, logical* comm)
    : m_data(data)
    , m_comm(comm)
    , m_searches(16)
{
}

hyperdex :: searches :: ~searches() throw ()
{
}

void
hyperdex :: searches :: start(const entityid& client,
                              uint32_t nonce,
                              const regionid& r,
                              const search& s)
{
    std::pair<entityid, uint32_t> key(client, nonce);

    if (m_searches.contains(key))
    {
        return;
    }

    e::intrusive_ptr<region::snapshot> snap = m_data->make_snapshot(r);
    e::intrusive_ptr<search_state> state = new search_state(r, s, snap);
    m_searches.insert(key, state);
    next(client, nonce);
}

void
hyperdex :: searches :: next(const entityid& client,
                             uint32_t nonce)
{
    e::intrusive_ptr<search_state> state;

    if (!m_searches.lookup(std::make_pair(client, nonce), &state))
    {
        return;
    }

    po6::threads::mutex::hold hold(&state->lock);

    while (state->snap->valid())
    {
        if ((state->point & state->mask) == (state->snap->secondary_point() & state->mask))
        {
            e::buffer key(state->snap->key());
            std::vector<e::buffer> value(state->snap->value());

            if (state->terms.matches(key, value))
            {
                e::buffer msg;
                msg.pack() << nonce << state-> count << key << value;
                m_comm->send(state->region, client, stream_no::SEARCH_ITEM, msg);
                ++state->count;
                return;
            }
        }

        state->snap->next();
    }

    e::buffer msg;
    m_comm->send(state->region, client, stream_no::SEARCH_ITEM, msg);
    ++state->count;
    stop(client, nonce);
}

void
hyperdex :: searches :: stop(const entityid& client,
                             uint32_t nonce)
{
    m_searches.remove(std::make_pair(client, nonce));
}

uint64_t
hyperdex :: searches :: hash(const std::pair<entityid, uint32_t>& key)
{
    return key.first.hash() + key.second;
}


hyperdex :: searches :: search_state :: search_state(const regionid& r,
                                                     const search& t,
                                                     e::intrusive_ptr<region::snapshot> s)
    : lock()
    , region(r)
    , point(t.secondary_point())
    , mask(t.secondary_mask())
    , terms(t)
    , snap(s)
    , count(0)
    , m_ref(0)
{
}

hyperdex :: searches :: search_state :: ~search_state() throw ()
{
}
