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

#include "client/atomic_group_request.h"
#include "client/util.h"
#include "client/constants.h"

#include "common/attribute_check.h"
#include "common/datatype_info.h"
#include "common/funcall.h"
#include "common/macros.h"
#include "common/serialization.h"

#define ERROR(CODE) \
    status = HYPERDEX_CLIENT_ ## CODE; \
    cl.m_last_error.set_loc(__FILE__, __LINE__); \
    cl.m_last_error.set_msg()

BEGIN_HYPERDEX_NAMESPACE

atomic_group_request::atomic_group_request(client& cl_, const coordinator_link& coord_, const char* space_)
    :  group_request(cl_, coord_, space_), funcs()
{
}

atomic_group_request::atomic_group_request(const atomic_group_request& other)
    :  group_request(other.cl, other.coord, other.space), funcs()
{
    // TODO: Use c++11 operator deletion
    throw std::runtime_error("Atomic requests must not be copied");
}

atomic_group_request& atomic_group_request::operator=(const atomic_group_request&)
{
    // TODO: Use c++11 operator deletion
    throw std::runtime_error("Atomic requests must not be copied");
    return *this;
}

int atomic_group_request::prepare(const hyperdex_client_keyop_info& opinfo,
                           const hyperdex_client_attribute_check* selection, size_t selection_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                           hyperdex_client_returncode& status)
{
    int ret = group_request::prepare(selection, selection_sz, status);

    if(ret < 0)
    {
        return ret;
    }

    size_t idx = 0;

    // Prepare the attrs
    idx = cl.prepare_funcs(space, *sc, opinfo, attrs, attrs_sz, &allocate, status, &funcs);

    if (idx < attrs_sz)
    {
        return -2 - selection_sz - idx;
    }

    // Prepare the mapattrs
    idx = cl.prepare_funcs(space, *sc, opinfo, mapattrs, mapattrs_sz, &allocate, status, &funcs);

    if (idx < mapattrs_sz)
    {
        return -2 - selection_sz - attrs_sz - idx;
    }

    return 0;
}

e::buffer* atomic_group_request::create_message(const hyperdex_client_keyop_info& opinfo)
{
    std::stable_sort(select.begin(), select.end());
    std::stable_sort(funcs.begin(), funcs.end());
    size_t sz = HYPERDEX_CLIENT_HEADER_SIZE_REQ
              + sizeof(uint8_t)
              + pack_size(select)
              + pack_size(funcs);

    e::buffer *msg = e::buffer::create(sz);
    uint8_t flags = (opinfo.fail_if_not_found ? 1 : 0)
                  | (opinfo.fail_if_found ? 2 : 0)
                  | (opinfo.erase ? 0 : 128);
    msg->pack_at(HYPERDEX_CLIENT_HEADER_SIZE_REQ)
        << flags << select << funcs;

    return msg;
}

END_HYPERDEX_NAMESPACE
