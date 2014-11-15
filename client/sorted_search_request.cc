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

#include "client/sorted_search_request.h"
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

sorted_search_request::sorted_search_request(client& cl_, const coordinator_link& coord_, const char* space_)
    : group_request(cl_, coord_, space_), sort_by_num(-1)
{
}

int sorted_search_request::prepare(const hyperdex_client_attribute_check* selection, size_t selection_sz,
                           hyperdex_client_returncode& status, const char* sort_by)
{
    int res = group_request::prepare(selection, selection_sz, status);
    if(res < 0)
    {
        return res;
    }

    try
    {
        const schema& sc = request::get_schema();
        sort_by_num = sc.lookup_attr(sort_by);

        if (sort_by_num == sc.attrs_sz)
        {
            ERROR(UNKNOWNATTR) << "\"" << e::strescape(sort_by)
                               << "\" is not an attribute of space \""
                               << e::strescape(space) << "\"";
            return -1 - selection_sz;
        }

        const datatype_info& sort_di = get_sort_di();

        if (!sort_di.comparable())
        {
            ERROR(WRONGTYPE) << "cannot sort by attribute \""
                             << e::strescape(sort_by)
                             << "\": it is not comparable";
            return -1 - selection_sz;
        }

        return 0;
    }
    catch(std::exception& e)
    {
        ERROR(UNKNOWNSPACE) << e.what();
        return -1;
    }
}

const datatype_info& sorted_search_request::get_sort_di() const
{
    return *datatype_info::lookup(get_schema().attrs[sort_by_num].type);
}

e::buffer* sorted_search_request::create_message(uint64_t limit, bool maximize)
{
    int8_t max = maximize ? 1 : 0;
    size_t sz = HYPERDEX_CLIENT_HEADER_SIZE_REQ
              + pack_size(select)
              + sizeof(limit)
              + sizeof(sort_by_num)
              + sizeof(max);

    e::buffer* msg = e::buffer::create(sz);
    msg->pack_at(HYPERDEX_CLIENT_HEADER_SIZE_REQ) << select << limit << sort_by_num << max;
    return msg;
}

END_HYPERDEX_NAMESPACE

