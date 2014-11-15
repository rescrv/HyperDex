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

#include "client/atomic_request.h"
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

atomic_request::atomic_request(client& cl_, const coordinator_link& coord_, const char* space_)
    : request(cl_, coord_, space_), checks(), funcs()
{
}

hyperdex_client_returncode atomic_request::validate_key(const e::slice& key) const
{
    hyperdex_client_returncode status = HYPERDEX_CLIENT_SUCCESS;

    try
    {
        const schema& sc = request::get_schema();

        datatype_info* di = datatype_info::lookup(sc.attrs[0].type);
        assert(di);

        if (!di->validate(key))
        {
            // This will update the status on error
            ERROR(WRONGTYPE) << "key must be type " << sc.attrs[0].type;
        }

        return status;
    }
    catch(std::exception& e)
    {
        ERROR(UNKNOWNSPACE) << e.what();
        return HYPERDEX_CLIENT_UNKNOWNSPACE;
    }
}

int atomic_request::prepare(const hyperdex_client_keyop_info& opinfo,
                           const hyperdex_client_attribute_check* chks, size_t chks_sz,
                           const hyperdex_client_attribute* attrs, size_t attrs_sz,
                           const hyperdex_client_map_attribute* mapattrs, size_t mapattrs_sz,
                           hyperdex_client_returncode& status)
{
    try
    {
        const schema& sc = request::get_schema();
        size_t idx = 0;

        // Prepare the checks
        idx = prepare_checks(sc, chks, chks_sz, status, &checks);

        if (idx < chks_sz)
        {
            return -2 - idx;
        }

        // Prepare the attrs
        idx = cl.prepare_funcs(space.c_str(), sc, opinfo, attrs, attrs_sz, &allocate, status, &funcs);

        if (idx < attrs_sz)
        {
            return -2 - chks_sz - idx;
        }

        // Prepare the mapattrs
        idx = cl.prepare_funcs(space.c_str(), sc, opinfo, mapattrs, mapattrs_sz, &allocate, status, &funcs);

        if (idx < mapattrs_sz)
        {
            return -2 - chks_sz - attrs_sz - idx;
        }

        return 0;
    }
    catch(std::exception& e)
    {
        ERROR(UNKNOWNSPACE) << e.what();
        return HYPERDEX_CLIENT_UNKNOWNSPACE;
    }
}

e::buffer* atomic_request::create_message(const hyperdex_client_keyop_info& opinfo, const e::slice& key)
{
    std::stable_sort(checks.begin(), checks.end());
    std::stable_sort(funcs.begin(), funcs.end());
    size_t sz = HYPERDEX_CLIENT_HEADER_SIZE_REQ
              + sizeof(uint8_t)
              + pack_size(key)
              + pack_size(checks)
              + pack_size(funcs);

    e::buffer *msg = e::buffer::create(sz);
    uint8_t flags = (opinfo.fail_if_not_found ? 1 : 0)
                  | (opinfo.fail_if_found ? 2 : 0)
                  | (opinfo.erase ? 0 : 128);
    msg->pack_at(HYPERDEX_CLIENT_HEADER_SIZE_REQ)
        << key << flags << checks << funcs;

    return msg;
}

END_HYPERDEX_NAMESPACE
