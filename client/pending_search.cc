// Copyright (c) 2011-2013, Cornell University
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
#include "client/client.h"
#include "client/constants.h"
#include "client/pending_search.h"
#include "client/util.h"

using hyperdex::pending_search;

pending_search :: pending_search(uint64_t id,
                                 hyperdex_client_returncode* status,
                                 const hyperdex_client_attribute** attrs, size_t* attrs_sz)
    : pending_aggregation(id, status)
    , m_attrs(attrs)
    , m_attrs_sz(attrs_sz)
    , m_yield(false)
    , m_done(false)
{
    *m_attrs = NULL;
    *m_attrs_sz = 0;
}

pending_search :: ~pending_search() throw ()
{
}

bool
pending_search :: can_yield()
{
    return m_yield;
}

bool
pending_search :: yield(hyperdex_client_returncode* status, e::error* err)
{
    *status = HYPERDEX_CLIENT_SUCCESS;
    *err = e::error();
    m_yield = false;

    if (this->aggregation_done() && !m_done)
    {
        m_yield = true;
        m_done = true;
    }
    else if (m_done)
    {
        set_status(HYPERDEX_CLIENT_SEARCHDONE);
    }

    return true;
}

void
pending_search :: handle_failure(const server_id& si,
                                 const virtual_server_id& vsi)
{
    m_yield = true;
    PENDING_ERROR(RECONFIGURE) << "reconfiguration affecting "
                               << vsi << "/" << si;
    return pending_aggregation::handle_failure(si, vsi);
}

bool
pending_search :: handle_message(client* cl,
                                 const server_id& si,
                                 const virtual_server_id& vsi,
                                 network_msgtype mt,
                                 std::auto_ptr<e::buffer> msg,
                                 e::unpacker up,
                                 hyperdex_client_returncode* status,
                                 e::error* err)
{
    bool handled = pending_aggregation::handle_message(cl, si, vsi, mt, std::auto_ptr<e::buffer>(), up, status, err);
    assert(handled);

    *status = HYPERDEX_CLIENT_SUCCESS;
    *err = e::error();

    if (mt == RESP_SEARCH_DONE)
    {
        if (this->aggregation_done())
        {
            m_yield = true;
            m_done = true;
        }

        return true;
    }
    else if (mt != RESP_SEARCH_ITEM)
    {
        PENDING_ERROR(SERVERERROR) << "server " << vsi << " responded to SEARCH with " << mt;
        m_yield = true;
        return true;
    }

    e::slice key;
    std::vector<e::slice> value;
    up = up >> key >> value;

    if (up.error())
    {
        PENDING_ERROR(SERVERERROR) << "communication error: server "
                                   << vsi << " sent corrupt message="
                                   << msg->as_slice().hex()
                                   << " in response to a SEARCH";
        m_yield = true;
        return true;
    }

    hyperdex_client_returncode op_status;
    e::error op_error;

    if (!value_to_attributes(cl->m_config,
                             cl->m_config.get_region_id(vsi),
                             key.data(), key.size(), value,
                             &op_status, &op_error, m_attrs, m_attrs_sz, cl->m_convert_types))
    {
        set_status(op_status);
        set_error(op_error);
        m_yield = true;
        return true;
    }

    std::auto_ptr<e::buffer> smsg(e::buffer::create(HYPERDEX_CLIENT_HEADER_SIZE_REQ + sizeof(uint64_t)));
    smsg->pack_at(HYPERDEX_CLIENT_HEADER_SIZE_REQ) << static_cast<uint64_t>(client_visible_id());

    if (!cl->send(REQ_SEARCH_NEXT, vsi, cl->m_next_server_nonce++, smsg, this, status))
    {
        PENDING_ERROR(RECONFIGURE) << "could not send SEARCH_NEXT to " << vsi;
        m_yield = true;
        return true;
    }

    set_status(HYPERDEX_CLIENT_SUCCESS);
    set_error(e::error());
    m_yield = true;
    return true;
}
