// Copyright (c) 2012, Cornell University
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
#include "client/constants.h"
#include "client/complete.h"
#include "client/pending_group_del.h"
#include "client/util.h"

hyperclient :: pending_group_del :: pending_group_del(int64_t group_del_id,
                                                      e::intrusive_ptr<refcount> ref,
                                                      hyperclient_returncode* status)
    : pending(status)
    , m_ref(ref)
{
    this->set_client_visible_id(group_del_id);
}

hyperclient :: pending_group_del :: ~pending_group_del() throw ()
{
}

hyperdex::network_msgtype
hyperclient :: pending_group_del :: request_type()
{
    return hyperdex::REQ_GROUP_DEL;
}

int64_t
hyperclient :: pending_group_del :: handle_response(hyperclient* cl,
                                                    const server_id& sender,
                                                    std::auto_ptr<e::buffer>,
                                                    hyperdex::network_msgtype type,
                                                    hyperclient_returncode* status)
{
    *status = HYPERCLIENT_SUCCESS;

    if (type != hyperdex::RESP_GROUP_DEL)
    {
        cl->killall(sender, HYPERCLIENT_SERVERERROR);
        return 0;
    }

    if (m_ref->last_reference())
    {
        set_status(HYPERCLIENT_SUCCESS);
        return client_visible_id();
    }
    else
    {
        return 0;
    }
}
