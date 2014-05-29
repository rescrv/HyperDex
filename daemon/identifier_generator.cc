// Copyright (c) 2012-2014, Cornell University
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

// STL
#include <algorithm>

// e
#include <e/atomic.h>

// HyperDex
#include "daemon/identifier_generator.h"

using namespace e::atomic;
using hyperdex::identifier_generator;
using hyperdex::region_id;

const region_id identifier_generator::defaultri;

identifier_generator :: identifier_generator()
    : m_generators()
{
}

identifier_generator :: ~identifier_generator() throw ()
{
}

bool
identifier_generator :: bump(const region_id& ri, uint64_t id)
{
    uint64_t* val = NULL;

    if (!m_generators.mod(ri, &val))
    {
        return false;
    }

    uint64_t count = load_64_nobarrier(val);

    while (count <= id)
    {
        count = e::atomic::compare_and_swap_64_nobarrier(val, count, id + 1);
    }

    return true;
}

uint64_t
identifier_generator :: peek(const region_id& ri) const
{
    e::atomic::memory_barrier();
    uint64_t val = 0;

    if (!m_generators.get(ri, &val))
    {
        abort();
    }

    return val;
}

uint64_t
identifier_generator :: generate_id(const region_id& ri)
{
    uint64_t* val = NULL;

    if (!m_generators.mod(ri, &val))
    {
        abort();
    }

    return e::atomic::increment_64_nobarrier(val, 1) - 1;
}

void
identifier_generator :: adopt(region_id* ris, size_t ris_sz)
{
    generator_map_t new_generators;

    for (size_t i = 0; i < ris_sz; ++i)
    {
        uint64_t count = 0;

        if (!m_generators.get(ris[i], &count))
        {
            count = 1;
        }

        new_generators.put(ris[i], count);
    }

    m_generators.swap(&new_generators);
}

void
identifier_generator :: copy_from(const identifier_generator& ig)
{
    m_generators.copy_from(ig.m_generators);
}
