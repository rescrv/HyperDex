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

// HyperClient
#include "hyperclient/constants.h"
#include "hyperclient/hyperclient_completedop.h"
#include "hyperclient/hyperclient_pending_count.h"
#include "hyperclient/util.h"

hyperclient :: pending_count :: pending_count(int64_t count_id,
                                              std::tr1::shared_ptr<uint64_t> refcount,
                                              hyperclient_returncode* status,
                                              uint64_t* result)
    : pending(status)
    , m_refcount(refcount)
    , m_result(result)
{
    ++*m_refcount;
    this->set_client_visible_id(count_id);
}

hyperclient :: pending_count :: ~pending_count() throw ()
{
}

hyperdex::network_msgtype
hyperclient :: pending_count :: request_type()
{
    return hyperdex::REQ_COUNT;
}

int64_t
hyperclient :: pending_count :: handle_response(hyperclient* cl,
                                                const po6::net::location& sender,
                                                std::auto_ptr<e::buffer> msg,
                                                hyperdex::network_msgtype type,
                                                hyperclient_returncode* status)
{
    *status = HYPERCLIENT_SUCCESS;

    if (type != hyperdex::RESP_COUNT)
    {
        cl->killall(sender, HYPERCLIENT_SERVERERROR);
        return 0;
    }

    e::buffer::unpacker up = msg->unpack_from(HYPERCLIENT_HEADER_SIZE);
    uint64_t result;
    up = up >> result;
    *m_result += result;

    if (up.error())
    {
        cl->killall(sender, HYPERCLIENT_SERVERERROR);
        return 0;
    }

    if (--*m_refcount == 0)
    {
        return client_visible_id();
    }
    else
    {
        return 0;
    }
}
