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

// HyperDex
#include "client/pending_search_describe.h"

using hyperdex::pending_search_describe;

pending_search_describe :: pending_search_describe(uint64_t id,
                                                   hyperdex_client_returncode* status,
                                                   const char** description)
    : pending_aggregation(id, status)
    , m_description(description)
    , m_done(false)
    , m_msgs()
    , m_text()
{
}

pending_search_describe :: ~pending_search_describe() throw ()
{
}

bool
pending_search_describe :: can_yield()
{
    return this->aggregation_done() && !m_done;
}

bool
pending_search_describe :: yield(hyperdex_client_returncode* status, e::error* err)
{
    std::ostringstream ostr;

    for (size_t i = 0; i < m_msgs.size(); ++i)
    {
        ostr << m_msgs[i].first << " " << m_msgs[i].second << "\n";
    }

    m_text = ostr.str();
    *status = HYPERDEX_CLIENT_SUCCESS;
    *m_description = m_text.c_str();
    *err = e::error();
    assert(this->can_yield());
    m_done = true;
    set_status(HYPERDEX_CLIENT_SUCCESS);
    set_error(e::error());
    return true;
}

void
pending_search_describe :: handle_sent_to(const server_id& si,
                                          const virtual_server_id& vsi)
{
    add_text(vsi, "touched by search");
    return pending_aggregation::handle_sent_to(si, vsi);
}

void
pending_search_describe :: handle_failure(const server_id& si,
                                          const virtual_server_id& vsi)
{
    add_text(vsi, "failed");
    return pending_aggregation::handle_sent_to(si, vsi);
}

bool
pending_search_describe :: handle_message(client* cl,
                                          const server_id& si,
                                          const virtual_server_id& vsi,
                                          network_msgtype mt,
                                          std::auto_ptr<e::buffer>,
                                          e::unpacker up,
                                          hyperdex_client_returncode* status,
                                          e::error* err)
{
    bool handled = pending_aggregation::handle_message(cl, si, vsi, mt, std::auto_ptr<e::buffer>(), up, status, err);
    assert(handled);

    *status = HYPERDEX_CLIENT_SUCCESS;
    *err = e::error();

    if (mt != RESP_SEARCH_DESCRIBE)
    {
        add_text(vsi, "sent non-RESP_SEARCH_DESCRIBE message");
        return true;
    }

    e::slice text = up.remainder();
    add_text(vsi, text);
    return true;
}

void
pending_search_describe :: add_text(const hyperdex::virtual_server_id& vid,
                                    const e::slice& text)
{
    m_msgs.push_back(std::make_pair(vid, std::string(reinterpret_cast<const char*>(text.data()), text.size())));
}

void
pending_search_describe :: add_text(const hyperdex::virtual_server_id& vid,
                                    const char* text)
{
    m_msgs.push_back(std::make_pair(vid, std::string(text)));
}
