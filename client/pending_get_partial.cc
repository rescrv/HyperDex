// Copyright (c) 2013-2014, Cornell University
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

// HyperClient
#include "common/network_returncode.h"
#include "client/client.h"
#include "client/pending_get_partial.h"
#include "client/util.h"

using hyperdex::pending_get_partial;

pending_get_partial :: pending_get_partial(uint64_t id,
                                           hyperdex_client_returncode* status,
                                           const hyperdex_client_attribute** attrs,
                                           size_t* attrs_sz)
    : pending(id, status)
    , m_state(INITIALIZED)
    , m_attrs(attrs)
    , m_attrs_sz(attrs_sz)
{
}

pending_get_partial :: ~pending_get_partial() throw ()
{
}

bool
pending_get_partial :: can_yield()
{
    assert(m_state == RECV || m_state == YIELDED);
    return m_state == RECV;
}

bool
pending_get_partial :: yield(hyperdex_client_returncode* status, e::error* err)
{
    *status = HYPERDEX_CLIENT_SUCCESS;
    *err = e::error();
    m_state = YIELDED;
    return true;
}

void
pending_get_partial :: handle_sent_to(const server_id&,
                                      const virtual_server_id&)
{
    assert(m_state == INITIALIZED);
    m_state = SENT;
}

void
pending_get_partial :: handle_failure(const server_id& si,
                                      const virtual_server_id& vsi)
{
    assert(m_state == SENT);
    m_state = RECV;
    PENDING_ERROR(RECONFIGURE) << "reconfiguration affecting "
                               << vsi << "/" << si;
}

bool
pending_get_partial :: handle_message(client* cl,
                                      const server_id& si,
                                      const virtual_server_id& vsi,
                                      network_msgtype mt,
                                      std::auto_ptr<e::buffer> msg,
                                      e::unpacker up,
                                      hyperdex_client_returncode* status,
                                      e::error* err)
{
    m_state = RECV;
    *status = HYPERDEX_CLIENT_SUCCESS;
    *err = e::error();

    if (mt != RESP_GET_PARTIAL)
    {
        PENDING_ERROR(SERVERERROR) << "server vsi responded to GET with " << mt;
        return true;
    }

    uint16_t response;
    up = up >> response;

    if (up.error())
    {
        PENDING_ERROR(SERVERERROR) << "communication error: server "
                                   << vsi << " sent corrupt message="
                                   << msg->as_slice().hex()
                                   << " in response to a GET";
        return true;
    }

    switch (static_cast<network_returncode>(response))
    {
        case NET_SUCCESS:
            break;
        case NET_NOTFOUND:
            set_status(HYPERDEX_CLIENT_NOTFOUND);
            set_error(e::error());
            return true;
        case NET_BADDIMSPEC:
            PENDING_ERROR(SERVERERROR) << "server " << si
                                       << " reports that our request was invalid;"
                                       << " check its log for details";
            return true;
        case NET_NOTUS:
            PENDING_ERROR(RECONFIGURE) << "server " << si
                                       << " reports that it is no longer reponsible"
                                       << " for the requested object";
            return true;
        case NET_READONLY:
            PENDING_ERROR(READONLY) << "cluster is in read-only mode, but this was a read:"
                                    << " please file a bug";
            return true;
        case NET_SERVERERROR:
            PENDING_ERROR(SERVERERROR) << "server " << si
                                       << " reports a server error;"
                                       << " check its log for details";
            return true;
        case NET_CMPFAIL:
            PENDING_ERROR(SERVERERROR) << "server " << si
                                       << " unexpectedly reports that a comparison failed;"
                                       << " check its log for details";
            return true;
        case NET_OVERFLOW:
            PENDING_ERROR(OVERFLOW) << "server " << si
                                    << " reports that the operation would"
                                    << " cause a number overflow";
            return true;
        case NET_UNAUTHORIZED:
            PENDING_ERROR(UNAUTHORIZED) << "server " << si
                                        << " denied the request because it is unauthorized";
            return true;
        default:
            PENDING_ERROR(SERVERERROR) << "server " << si
                                       << " returned non-sensical returncode"
                                       << response;
            return true;
    }

    std::vector<std::pair<uint16_t, e::slice> > value;

    while (up.remain() && !up.error())
    {
        uint16_t attr_n;
        e::slice attr_s;
        up = up >> attr_n >> attr_s;
        value.push_back(std::make_pair(attr_n, attr_s));
    }

    if (up.error())
    {
        PENDING_ERROR(SERVERERROR) << "communication error: server "
                                   << vsi << " sent corrupt message="
                                   << msg->as_slice().hex()
                                   << " in response to a GET_PARTIAL";
        return true;
    }

    hyperdex_client_returncode op_status;
    e::error op_error;

    if (!value_to_attributes(cl->m_config,
                             cl->m_config.get_region_id(vsi),
                             value, &op_status, &op_error,
                             m_attrs, m_attrs_sz, cl->m_convert_types))
    {
        set_status(op_status);
        set_error(op_error);
        return true;
    }

    set_status(HYPERDEX_CLIENT_SUCCESS);
    set_error(e::error());
    return true;
}
