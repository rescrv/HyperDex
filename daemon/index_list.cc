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
#include <e/endian.h>

// HyperDex
#include "daemon/index_list.h"

using hyperdex::datatype_info;
using hyperdex::index_info;
using hyperdex::index_list;

index_list :: index_list(hyperdatatype datatype)
    : m_datatype(datatype)
{
}

index_list :: ~index_list() throw ()
{
}

void
index_list :: extract_elements(const e::slice& list,
                              std::vector<e::slice>* elems)
{
    datatype_info* elem = datatype_info::lookup(m_datatype);
    const uint8_t* ptr = list.data();
    const uint8_t* end = list.data() + list.size();
    e::slice e;

    while (ptr < end)
    {
        bool stepped = elem->step(&ptr, end, &e);
        assert(stepped);
        elems->push_back(e);
    }

    assert(ptr == end);
}

datatype_info*
index_list :: element_datatype_info()
{
    return datatype_info::lookup(m_datatype);
}

index_info*
index_list :: element_index_info()
{
    return index_info::lookup(m_datatype);
}
