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
                                 hyperclient_returncode* status,
                                 struct hyperclient_attribute** attrs, size_t* attrs_sz)
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
pending_search :: yield(hyperclient_returncode* status)
{
    *status = HYPERCLIENT_SUCCESS;
    m_yield = false;

    if (this->aggregation_done() && !m_done)
    {
        m_yield = true;
        m_done = true;
    }
    else if (m_done)
    {
        set_status(HYPERCLIENT_SEARCHDONE);
    }

    return true;
}

void
pending_search :: handle_failure(const server_id& si,
                                 const virtual_server_id& vsi)
{
    set_status(HYPERCLIENT_RECONFIGURE);
    m_yield = true;
    return pending_aggregation::handle_failure(si, vsi);
}

bool
pending_search :: handle_message(client* cl,
                                 const server_id& si,
                                 const virtual_server_id& vsi,
                                 network_msgtype mt,
                                 std::auto_ptr<e::buffer> msg,
                                 e::unpacker up,
                                 hyperclient_returncode* status)
{
    if (!pending_aggregation::handle_message(cl, si, vsi, mt, std::auto_ptr<e::buffer>(), up, status))
    {
        return false;
    }

    *status = HYPERCLIENT_SUCCESS;
    set_status(HYPERCLIENT_SERVERERROR);

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
        set_status(HYPERCLIENT_SERVERERROR);
        m_yield = true;
        return true;
    }

    uint16_t response;
    up = up >> response;

    if (up.error())
    {
        set_status(HYPERCLIENT_SERVERERROR);
        m_yield = true;
        return true;
    }

    // Otheriwise it is a SEARCH_ITEM message.
    e::slice key;
    std::vector<e::slice> value;

    if ((msg->unpack_from(HYPERCLIENT_HEADER_SIZE_RESP) >> key >> value).error())
    {
        set_status(HYPERCLIENT_SERVERERROR);
        m_yield = true;
        return true;
    }

    hyperclient_returncode op_status;

    if (!value_to_attributes(cl->m_config, cl->m_config.get_region_id(vsi),
                             key.data(), key.size(),
                             value, status, &op_status, m_attrs, m_attrs_sz))
    {
        set_status(op_status);
        m_yield = true;
        return true;
    }

    std::auto_ptr<e::buffer> smsg(e::buffer::create(HYPERCLIENT_HEADER_SIZE_REQ + sizeof(uint64_t)));
    smsg->pack_at(HYPERCLIENT_HEADER_SIZE_REQ) << static_cast<uint64_t>(client_visible_id());

    if (!cl->send(REQ_SEARCH_NEXT, vsi, cl->m_next_server_nonce++, smsg, this, status))
    {
        set_status(HYPERCLIENT_RECONFIGURE);
        m_yield = true;
        return true;
    }

    set_status(HYPERCLIENT_SUCCESS);
    m_yield = true;
    return true;
}
