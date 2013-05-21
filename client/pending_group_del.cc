// Copyright (c) 2012-2013, Cornell University
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
#include "client/pending_group_del.h"

using hyperdex::pending_group_del;

pending_group_del :: pending_group_del(uint64_t id,
                                       hyperclient_returncode* status)
    : pending_aggregation(id, status)
    , m_done(false)
{
}

pending_group_del :: ~pending_group_del() throw ()
{
}

bool
pending_group_del :: can_yield()
{
    return this->aggregation_done() && !m_done;
}

bool
pending_group_del :: yield(hyperclient_returncode* status)
{
    *status = HYPERCLIENT_SUCCESS;
    assert(this->can_yield());
    m_done = true;
    set_status(HYPERCLIENT_SUCCESS);
    return true;
}

bool
pending_group_del :: handle_message(client* cl,
                                    const server_id& si,
                                    const virtual_server_id& vsi,
                                    network_msgtype mt,
                                    std::auto_ptr<e::buffer>,
                                    e::unpacker up,
                                    hyperclient_returncode* status)
{
    if (!pending_aggregation::handle_message(cl, si, vsi, mt, std::auto_ptr<e::buffer>(), up, status))
    {
        return false;
    }

    *status = HYPERCLIENT_SUCCESS;

    if (mt != RESP_GROUP_DEL)
    {
        // HYPERCLIENT_SERVERERROR
        return true;
    }

    return true;
}
