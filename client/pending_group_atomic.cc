// Copyright (c) 2011-2014, Cornell University
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
#include "client/pending_group_atomic.h"

using hyperdex::pending_group_atomic;

pending_group_atomic :: pending_group_atomic(uint64_t id,
                                             hyperdex_client_returncode* status,
                                             uint64_t* update_count)
    : pending_aggregation(id, status)
    , m_state(INITIALIZED)
    , m_update_count(update_count)
{
    *m_update_count = 0;
}

pending_group_atomic :: ~pending_group_atomic() throw ()
{
}

bool
pending_group_atomic :: can_yield()
{
    return m_state == DONE || m_state == FAILURE;
}

bool
pending_group_atomic :: yield(hyperdex_client_returncode* status, e::error* err)
{
    *status = HYPERDEX_CLIENT_SUCCESS;
    *err = e::error();
    assert(this->can_yield());
    m_state = YIELDED;
    return true;
}

void
pending_group_atomic :: handle_sent_to(const server_id& sid,
                                 const virtual_server_id& vid)
{
    pending_aggregation::handle_sent_to(sid, vid);

    // We could already have received something
    if(m_state == INITIALIZED)
    {
        m_state = SENT;
    }
}

void
pending_group_atomic :: handle_failure(const server_id& si,
                                       const virtual_server_id& vsi)
{
    pending_aggregation::handle_failure(si, vsi);

    assert(m_state == SENT);
    m_state = RECV;
    PENDING_ERROR(RECONFIGURE) << "reconfiguration affecting "
                               << vsi << "/" << si;
}

bool
pending_group_atomic :: handle_message(client* cl,
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

    if (mt != RESP_GROUP_ATOMIC)
    {
        PENDING_ERROR(SERVERERROR) << "server " << vsi << " responded to GROUP_ATOMIC with " << mt;
        return true;
    }

    uint64_t response;
    up = up >> response;

    // Remember how many fields we updated
    *m_update_count += response;

    if (up.error())
    {
        PENDING_ERROR(SERVERERROR) << "communication error: server "
                                   << vsi << " sent corrupt message="
                                   << msg->as_slice().hex()
                                   << " in response to a GROUP_ATOMIC";
        return true;
    }

    if(this->aggregation_done())
    {
        m_state = DONE;
        set_status(HYPERDEX_CLIENT_SUCCESS);
        set_error(e::error());
    }

    return true;
}
