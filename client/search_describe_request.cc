// Copyright (c) 2014, Cornell University
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

#include <e/strescape.h>

#include "client/search_describe_request.h"
#include "client/util.h"
#include "client/constants.h"

#include "common/attribute_check.h"
#include "common/datatype_info.h"
#include "common/funcall.h"
#include "common/macros.h"
#include "common/serialization.h"

BEGIN_HYPERDEX_NAMESPACE

search_describe_request::search_describe_request(client& cl_, const coordinator_link& coord_, const char* space_)
    : req(cl_, coord_, space_), group_req(req)
{
}

e::buffer* search_describe_request::create_message()
{
    size_t sz = HYPERDEX_CLIENT_HEADER_SIZE_REQ + pack_size(group_req.get_selection());

    e::buffer* msg = e::buffer::create(sz);
    msg->pack_at(HYPERDEX_CLIENT_HEADER_SIZE_REQ) << group_req.get_selection();

    return msg;
}

int search_describe_request::prepare(const hyperdex_client_attribute_check* selection, size_t selection_sz,
                        hyperdex_client_returncode& status)
{
    return group_req.prepare(selection, selection_sz, status);
}

END_HYPERDEX_NAMESPACE
