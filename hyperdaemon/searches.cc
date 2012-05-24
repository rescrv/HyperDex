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
#include "hyperdisk/hyperdisk/disk.h"
#include "hyperdisk/hyperdisk/returncode.h"

// HyperDex
#include "hyperdex/hyperdex/packing.h"

// HyperDaemon
#include "hyperdaemon/datalayer.h"
#include "hyperdaemon/logical.h"
#include "hyperdaemon/searches.h"

using hyperdex::coordinatorlink;
using hyperdex::entityid;
using hyperdex::regionid;
using hyperspacehashing::search;
using hyperspacehashing::mask::coordinate;

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
hyperdaemon :: searches :: start(const hyperdex::entityid& us,
                                 const hyperdex::entityid& client,
                                 uint64_t search_num,
                                 uint64_t nonce,
                                 std::auto_ptr<e::buffer> msg,
                                 const hyperspacehashing::search& terms)
{
    search_id key(us.get_region(), client, search_num);

    if (m_searches.contains(key))
    {
        LOG(INFO) << "DROPPED";
        return;
    }

    if (m_config.dimensions(us.get_space()) != terms.size())
    {
        LOG(INFO) << "DROPPED";
        return;
    }

    bool done = false;

    while(!done)
    {
        hyperdisk::returncode ret = m_data->flush(us.get_region(), -1, false);

        if (ret == hyperdisk::SUCCESS)
        {
            done = true;
        }
        else if (ret == hyperdisk::DIDNOTHING)
        {
            done = true;
        }
        else if (ret == hyperdisk::DATAFULL || ret == hyperdisk::SEARCHFULL)
        {
            hyperdisk::returncode ioret;
            ioret = m_data->do_mandatory_io(us.get_region());

            if (ioret != hyperdisk::SUCCESS && ioret != hyperdisk::DIDNOTHING)
            {
                PLOG(ERROR) << "Disk I/O returned " << ioret;
            }
        }
        else
        {
            PLOG(ERROR) << "Disk flush returned " << ret;
        }
    }

    hyperspacehashing::mask::hasher hasher(m_config.disk_hasher(us.get_subspace()));
    hyperspacehashing::mask::coordinate coord(hasher.hash(terms));
    e::intrusive_ptr<hyperdisk::snapshot> snap = m_data->make_snapshot(us.get_region(), terms);
    e::intrusive_ptr<search_state> state = new search_state(us.get_region(), coord, msg, terms, snap);
    m_searches.insert(key, state);
    next(us, client, search_num, nonce);
}

void
hyperdaemon :: searches :: next(const hyperdex::entityid& us,
                                const hyperdex::entityid& client,
                                uint64_t search_num,
                                uint64_t nonce)
{
    search_id key(us.get_region(), client, search_num);
    e::intrusive_ptr<search_state> state;

    if (!m_searches.lookup(key, &state))
    {
        LOG(INFO) << "DROPPED";
        return;
    }

    po6::threads::mutex::hold hold(&state->lock);

    while (state->snap->valid())
    {
        if (state->search_coord.intersects(state->snap->coordinate()))
        {
            if (state->terms.matches(state->snap->key(), state->snap->value()))
            {
                size_t sz = m_comm->header_size() + sizeof(uint64_t)
                          + sizeof(uint32_t) + state->snap->key().size()
                          + hyperdex::packspace(state->snap->value());
                std::auto_ptr<e::buffer> msg(e::buffer::create(sz));
                bool fits = !(msg->pack_at(m_comm->header_size())
                                << nonce
                                << state->snap->key()
                                << state->snap->value()).error();
                assert(fits);
                m_comm->send(us, client, hyperdex::RESP_SEARCH_ITEM, msg);
                state->snap->next();
                return;
            }
        }

        state->snap->next();
    }

    std::auto_ptr<e::buffer> msg(e::buffer::create(m_comm->header_size() + sizeof(uint64_t)));
    bool fits = !(msg->pack_at(m_comm->header_size()) << nonce).error();
    assert(fits);
    m_comm->send(us, client, hyperdex::RESP_SEARCH_DONE, msg);
    stop(us, client, search_num);
}

void
hyperdaemon :: searches :: stop(const hyperdex::entityid& us,
                                const hyperdex::entityid& client,
                                uint64_t search_num)
{
    m_searches.remove(search_id(us.get_region(), client, search_num));
}

uint64_t
hyperdaemon :: searches :: hash(const search_id& si)
{
    return si.region.hash() + si.client.hash() + si.search_number;
}


hyperdaemon :: searches :: search_state :: search_state(const regionid& r,
                                                        const coordinate& sc,
                                                        std::auto_ptr<e::buffer> msg,
                                                        const hyperspacehashing::search& t,
                                                        e::intrusive_ptr<hyperdisk::snapshot> s)
    : lock()
    , region(r)
    , search_coord(sc)
    , backing(msg)
    , terms(t)
    , snap(s)
    , m_ref(0)
{
}

hyperdaemon :: searches :: search_state :: ~search_state() throw ()
{
}
