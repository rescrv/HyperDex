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
#include "common/network_returncode.h"
#include "client/pending_atomic.h"

using hyperdex::pending_atomic;

pending_atomic :: pending_atomic(uint64_t id,
                                 hyperclient_returncode* status)
    : pending(id, status)
    , m_state(INITIALIZED)
{
}

pending_atomic :: ~pending_atomic() throw ()
{
}

bool
pending_atomic :: can_yield()
{
    assert(m_state == RECV || m_state == YIELDED);
    return m_state == RECV;
}

bool
pending_atomic :: yield(hyperclient_returncode*)
{
    m_state = YIELDED;
    return true;
}

void
pending_atomic :: handle_sent_to(const server_id&,
                                 const virtual_server_id&)
{
    assert(m_state == INITIALIZED);
    m_state = SENT;
}

void
pending_atomic :: handle_failure(const server_id&,
                                 const virtual_server_id&)
{
    assert(m_state == SENT);
    m_state = RECV;
    set_status(HYPERCLIENT_RECONFIGURE);
}

bool
pending_atomic :: handle_message(client*,
                                 const server_id&,
                                 const virtual_server_id&,
                                 network_msgtype mt,
                                 std::auto_ptr<e::buffer>,
                                 e::unpacker up,
                                 hyperclient_returncode* status)
{
    m_state = RECV;
    *status = HYPERCLIENT_SUCCESS;
    set_status(HYPERCLIENT_SERVERERROR);

    if (mt != RESP_ATOMIC)
    {
        return true;
    }

    uint16_t response;
    up = up >> response;

    if (up.error())
    {
        return true;
    }

    switch (static_cast<network_returncode>(response))
    {
        case NET_SUCCESS:
            set_status(HYPERCLIENT_SUCCESS);
            break;
        case NET_NOTFOUND:
            set_status(HYPERCLIENT_NOTFOUND);
            break;
        case NET_BADDIMSPEC:
        case NET_BADMICROS:
            set_status(HYPERCLIENT_SERVERERROR);
            break;
        case NET_NOTUS:
            set_status(HYPERCLIENT_RECONFIGURE);
            break;
        case NET_CMPFAIL:
            set_status(HYPERCLIENT_CMPFAIL);
            break;
        case NET_OVERFLOW:
            set_status(HYPERCLIENT_OVERFLOW);
            break;
        case NET_READONLY:
            set_status(HYPERCLIENT_READONLY);
            break;
        case NET_SERVERERROR:
        default:
            set_status(HYPERCLIENT_SERVERERROR);
            break;
    }

    return true;
}
