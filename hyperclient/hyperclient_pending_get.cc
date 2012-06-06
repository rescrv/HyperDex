// Copyright (c) 2011-2012, Cornell University
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
#include "hyperclient/constants.h"
#include "hyperclient/hyperclient_pending_get.h"
#include "hyperclient/util.h"

hyperclient :: pending_get :: pending_get(hyperclient_returncode* status,
                                          struct hyperclient_attribute** attrs,
                                          size_t* attrs_sz)
    : pending(status)
    , m_attrs(attrs)
    , m_attrs_sz(attrs_sz)
{
}

hyperclient :: pending_get :: ~pending_get() throw ()
{
}

hyperdex::network_msgtype
hyperclient :: pending_get :: request_type()
{
    return hyperdex::REQ_GET;
}

int64_t
hyperclient :: pending_get :: handle_response(hyperclient* cl,
                                              const po6::net::location& sender,
                                              std::auto_ptr<e::buffer> msg,
                                              hyperdex::network_msgtype type,
                                              hyperclient_returncode* status)
{
    *status = HYPERCLIENT_SUCCESS;

    if (type != hyperdex::RESP_GET)
    {
        cl->killall(sender, HYPERCLIENT_SERVERERROR);
        return 0;
    }

    e::buffer::unpacker up = msg->unpack_from(HYPERCLIENT_HEADER_SIZE);
    uint16_t response;
    up = up >> response;

    if (up.error())
    {
        cl->killall(sender, HYPERCLIENT_SERVERERROR);
        return 0;
    }

    switch (static_cast<hyperdex::network_returncode>(response))
    {
        case hyperdex::NET_SUCCESS:
            break;
        case hyperdex::NET_NOTFOUND:
            set_status(HYPERCLIENT_NOTFOUND);
            return client_visible_id();
        case hyperdex::NET_BADDIMSPEC:
            set_status(HYPERCLIENT_SERVERERROR);
            return client_visible_id();
        case hyperdex::NET_NOTUS:
            set_status(HYPERCLIENT_RECONFIGURE);
            return client_visible_id();
        case hyperdex::NET_READONLY:
            set_status(HYPERCLIENT_READONLY);
            return client_visible_id();
        case hyperdex::NET_SERVERERROR:
        case hyperdex::NET_CMPFAIL:
        case hyperdex::NET_BADMICROS:
        case hyperdex::NET_OVERFLOW:
        default:
            cl->killall(sender, HYPERCLIENT_SERVERERROR);
            return 0;
    }

    std::vector<e::slice> value;
    up = up >> value;

    if (up.error())
    {
        cl->killall(sender, HYPERCLIENT_SERVERERROR);
        return 0;
    }

    hyperclient_returncode op_status;

    if (!value_to_attributes(*cl->m_config, this->entity(), NULL, 0,
                             value, status, &op_status, m_attrs, m_attrs_sz))
    {
        set_status(op_status);
        return client_visible_id();
    }

    set_status(HYPERCLIENT_SUCCESS);
    return client_visible_id();
}
