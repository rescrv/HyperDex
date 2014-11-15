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

#ifndef hyperdex_client_sorted_search_request_h_
#define hyperdex_client_sorted_search_request_h_

#include "common/datatype_info.h"
#include "client/group_request.h"

BEGIN_HYPERDEX_NAMESPACE

// Use this prepare a sorted search request
// Can only be used once, i.e. create one for each funcall
class sorted_search_request : public group_request
{
public:
    sorted_search_request(client& cl_, const coordinator_link& coord_, const char* space_);

    int prepare(const hyperdex_client_attribute_check* selection, size_t selection_sz,
                           hyperdex_client_returncode& status, const char* sort_by);
    e::buffer* create_message(uint64_t limit, bool maximize);

    uint16_t get_sort_by_index() const
    {
        return sort_by_num;
    }

    const datatype_info& get_sort_di() const;

private:
    uint16_t sort_by_num;
};

END_HYPERDEX_NAMESPACE

#endif // header guard


