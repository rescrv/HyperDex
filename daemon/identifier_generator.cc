// Copyright (c) 2012-2013, Cornell University
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

using hyperdex::identifier_generator;
using hyperdex::region_id;

struct identifier_generator::counter
{
    counter() : ri(), count() {}
    counter(const region_id& r, uint64_t c) : ri(r), count(c) {}
    counter(const counter& other) : ri(other.ri), count(other.count) {}
    region_id ri;
    uint64_t count;

    bool operator < (const counter& rhs) const
    {
        if (ri < rhs.ri)
        {
            return true;
        }
        else if (ri == rhs.ri)
        {
            return count < rhs.count;
        }
        else
        {
            return false;
        }
    }
};

identifier_generator :: identifier_generator()
    : m_counters(NULL)
    , m_counters_sz(0)
{
    e::atomic::store_ptr_release(&m_counters, static_cast<counter*>(NULL));
    e::atomic::store_64_release(&m_counters_sz, 0);
}

identifier_generator :: ~identifier_generator() throw ()
{
    counter* counters = NULL;
    uint64_t counters_sz = 0;
    get_base(&counters, &counters_sz);

    if (counters)
    {
        delete[] counters;
    }
}

bool
identifier_generator :: bump(const region_id& ri, uint64_t id)
{
    counter* c = get_counter(ri);

    if (!c)
    {
        return false;
    }

    uint64_t count = c->count;

    while (count <= id)
    {
        count = e::atomic::compare_and_swap_64_nobarrier(&c->count, count, id + 1);
    }

    return true;
}

bool
identifier_generator :: peek(const region_id& ri, uint64_t* id) const
{
    counter* c = get_counter(ri);

    if (!c)
    {
        return false;
    }

    *id = e::atomic::increment_64_nobarrier(&c->count, 0);
    return true;
}

bool
identifier_generator :: generate_id(const region_id& ri, uint64_t* id)
{
    return generate_ids(ri, 1, id);
}

bool
identifier_generator :: generate_ids(const region_id& ri, uint64_t n, uint64_t* id)
{
    counter* c = get_counter(ri);

    if (!c)
    {
        return false;
    }

    *id = e::atomic::increment_64_nobarrier(&c->count, n) - n;
    return true;
}

void
identifier_generator :: adopt(region_id* ris, size_t ris_sz)
{
    // setup the new counters
    counter* new_counters = new counter[ris_sz];
    size_t new_sz = ris_sz;

    for (size_t i = 0; i < new_sz; ++i)
    {
        new_counters[i].ri = ris[i];
        new_counters[i].count = 1;
    }

    std::sort(new_counters, new_counters + new_sz);

    // get the old counters
    counter* old_counters = NULL;
    uint64_t old_sz = 0;
    get_base(&old_counters, &old_sz);

    // carry over values
    size_t o_idx = 0;
    size_t n_idx = 0;

    while (o_idx < old_sz && n_idx < new_sz)
    {
        if (old_counters[o_idx].ri == new_counters[n_idx].ri)
        {
            new_counters[n_idx].count = old_counters[o_idx].count;
            ++o_idx;
            ++n_idx;
        }
        else if (old_counters[o_idx].ri < new_counters[n_idx].ri)
        {
            ++o_idx;
        }
        else if (old_counters[o_idx].ri > new_counters[n_idx].ri)
        {
            ++n_idx;
        }
    }

    // install the new values
    e::atomic::store_ptr_release(&m_counters, new_counters);
    e::atomic::store_64_release(&m_counters_sz, new_sz);
    // free the old ones

    if (old_counters)
    {
        delete[] old_counters;
    }
}

void
identifier_generator :: copy_from(const identifier_generator& other)
{
    if (m_counters)
    {
        delete[] m_counters;
    }

    m_counters_sz = other.m_counters_sz;
    m_counters = new counter[m_counters_sz];

    for (size_t i = 0; i < m_counters_sz; ++i)
    {
        m_counters[i] = other.m_counters[i];
    }
}

void
identifier_generator :: get_base(counter** counters, uint64_t* counters_sz) const
{
    *counters_sz = e::atomic::load_64_acquire(&m_counters_sz);
    *counters = m_counters;
}

identifier_generator::counter*
identifier_generator :: get_counter(const region_id& ri) const
{
    counter* counters = NULL;
    uint64_t counters_sz = 0;
    get_base(&counters, &counters_sz);

    for (uint64_t i = 0; i < counters_sz; ++i)
    {
        if (counters[i].ri == ri)
        {
            return counters + i;
        }
    }

    return NULL;
}
