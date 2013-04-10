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

// e
#include <e/guard.h>

// HyperDex
#include "common/ids.h"
#include "client/constants.h"
#include "client/complete.h"
#include "client/description.h"
#include "client/pending_search_description.h"
#include "client/util.h"

using hyperdex::virtual_server_id;

hyperclient :: pending_search_description :: pending_search_description(int64_t searchid,
                                                                        hyperclient_returncode* status,
                                                                        e::intrusive_ptr<description> sd)
    : pending(status)
    , m_searchid(searchid)
    , m_sd(sd)
{
    this->set_client_visible_id(searchid);
}

hyperclient :: pending_search_description :: ~pending_search_description() throw ()
{
}

hyperdex::network_msgtype
hyperclient :: pending_search_description :: request_type()
{
    return hyperdex::REQ_SEARCH_DESCRIBE;
}

int64_t
hyperclient :: pending_search_description :: handle_response(hyperclient* cl,
                                                             const hyperdex::server_id& sender,
                                                             std::auto_ptr<e::buffer> msg,
                                                             hyperdex::network_msgtype type,
                                                             hyperclient_returncode* status)
{
    *status = HYPERCLIENT_SUCCESS;

    if (type != hyperdex::RESP_SEARCH_DESCRIBE)
    {
        cl->killall(sender, HYPERCLIENT_SERVERERROR);
        return 0;
    }

    e::slice text = msg->unpack_from(HYPERCLIENT_HEADER_SIZE_RESP).as_slice();
    m_sd->add_text(sent_to(), text);

    if (m_sd->last_reference())
    {
        m_sd->compile();
        set_status(HYPERCLIENT_SUCCESS);
        return client_visible_id();
    }

    return 0;
}
