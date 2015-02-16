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
#include "daemon/index_map.h"

using hyperdex::datatype_info;
using hyperdex::index_info;
using hyperdex::index_map;

index_map :: index_map(hyperdatatype key_datatype,
                       hyperdatatype val_datatype)
    : m_key_datatype(key_datatype)
    , m_val_datatype(val_datatype)
{
}

index_map :: ~index_map() throw ()
{
}

hyperdatatype
index_map :: datatype() const
{
    return CREATE_CONTAINER2(HYPERDATATYPE_MAP_GENERIC,
                             m_key_datatype,
                             m_val_datatype);
}

void
index_map :: extract_elements(const e::slice& map,
                              std::vector<e::slice>* elems) const
{
    datatype_info* elem_k = datatype_info::lookup(m_key_datatype);
    datatype_info* elem_v = datatype_info::lookup(m_val_datatype);
    const uint8_t* ptr = map.data();
    const uint8_t* end = map.data() + map.size();
    e::slice key;
    e::slice val;

    while (ptr < end)
    {
        bool stepped;
        stepped = elem_k->step(&ptr, end, &key);
        assert(stepped);
        stepped = elem_v->step(&ptr, end, &val);
        assert(stepped);
        elems->push_back(key);
    }

    assert(ptr == end);
}

const datatype_info*
index_map :: element_datatype_info() const
{
    return datatype_info::lookup(m_key_datatype);
}

const index_info*
index_map :: element_index_info() const
{
    return index_info::lookup(m_key_datatype);
}
