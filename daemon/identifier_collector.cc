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

// STL
#include <algorithm>

// e
#include <e/atomic.h>

// HyperDex
#include "daemon/identifier_collector.h"

using hyperdex::identifier_collector;
using hyperdex::region_id;

// indicates that count is the next ID for ri
struct identifier_collector::counter
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

identifier_collector :: identifier_collector()
    : m_lower_bounds(NULL)
    , m_lower_bounds_sz(0)
    , m_gaps_mtx()
    , m_gaps()
{
    po6::threads::mutex::hold hold(&m_gaps_mtx);
    e::atomic::store_ptr_release(&m_lower_bounds, static_cast<counter*>(NULL));
    e::atomic::store_64_release(&m_lower_bounds_sz, 0);
}

identifier_collector :: ~identifier_collector() throw ()
{
    po6::threads::mutex::hold hold(&m_gaps_mtx);
    counter* lower_bounds = NULL;
    uint64_t lower_bounds_sz = 0;
    get_base(&lower_bounds, &lower_bounds_sz);

    if (lower_bounds)
    {
        delete[] lower_bounds;
    }
}

bool
identifier_collector :: bump(const region_id& ri, uint64_t lb)
{
    counter* c = get_lower_bound(ri);

    if (!c)
    {
        return false;
    }

    uint64_t count = c->count;

    while (count < lb)
    {
        count = e::atomic::compare_and_swap_64_nobarrier(&c->count, count, lb);
    }

    po6::threads::mutex::hold hold(&m_gaps_mtx);
    squash_gaps(c);
    return true;
}

bool
identifier_collector :: collect(const region_id& ri, uint64_t id)
{
    counter* c = get_lower_bound(ri);

    if (!c)
    {
        return false;
    }

    uint64_t count = e::atomic::load_64_nobarrier(&c->count);
    bool push = true;

    if (count > id)
    {
        return true;
    }
    else if (count == id)
    {
        if (e::atomic::compare_and_swap_64_nobarrier(&c->count, id, id + 1) == id)
        {
            push = false;
        }
    }

    if (push)
    {
        po6::threads::mutex::hold hold(&m_gaps_mtx);
        m_gaps.push_back(counter(ri, id));
    }

    return true;
}

bool
identifier_collector :: lower_bound(const region_id& ri, uint64_t* lb)
{
    counter* c = get_lower_bound(ri);

    if (!c)
    {
        return false;
    }

    po6::threads::mutex::hold hold(&m_gaps_mtx);
    squash_gaps(c);
    *lb = e::atomic::load_64_nobarrier(&c->count);
    return true;
}

void
identifier_collector :: adopt(region_id* ris, size_t ris_sz)
{
    // setup the new counters
    counter* new_lower_bounds = new counter[ris_sz];
    size_t new_sz = ris_sz;

    for (size_t i = 0; i < new_sz; ++i)
    {
        new_lower_bounds[i].ri = ris[i];
        new_lower_bounds[i].count = 1;
    }

    std::sort(new_lower_bounds, new_lower_bounds + new_sz);

    // get the old counters
    counter* old_lower_bounds = NULL;
    size_t old_sz = 0;
    get_base(&old_lower_bounds, &old_sz);

    // carry over values
    size_t o_idx = 0;
    size_t n_idx = 0;

    while (o_idx < old_sz && n_idx < new_sz)
    {
        if (old_lower_bounds[o_idx].ri == new_lower_bounds[n_idx].ri)
        {
            new_lower_bounds[n_idx].count = old_lower_bounds[o_idx].count;
            ++o_idx;
            ++n_idx;
        }
        else if (old_lower_bounds[o_idx].ri < new_lower_bounds[n_idx].ri)
        {
            ++o_idx;
        }
        else if (old_lower_bounds[o_idx].ri > new_lower_bounds[n_idx].ri)
        {
            ++n_idx;
        }
    }

    // install the new values
    e::atomic::store_ptr_release(&m_lower_bounds, new_lower_bounds);
    e::atomic::store_64_release(&m_lower_bounds_sz, new_sz);
    // free the old ones

    if (old_lower_bounds)
    {
        delete[] old_lower_bounds;
    }

    // kill the dead gaps
    po6::threads::mutex::hold hold(&m_gaps_mtx);

    for (size_t i = 0; i < m_gaps.size(); )
    {
        if (get_lower_bound(m_gaps[i].ri))
        {
            ++i;
        }
        else
        {
            std::swap(m_gaps[i], m_gaps.back());
            m_gaps.pop_back();
        }
    }

    for (size_t i = 0; i < new_sz; ++i)
    {
        squash_gaps(new_lower_bounds + i);
    }
}

void
identifier_collector :: get_base(counter** lower_bounds, uint64_t* lower_bounds_sz)
{
    *lower_bounds_sz = e::atomic::load_64_acquire(&m_lower_bounds_sz);
    *lower_bounds = m_lower_bounds;
}

identifier_collector::counter*
identifier_collector :: get_lower_bound(const region_id& ri)
{
    counter* lower_bounds = NULL;
    uint64_t lower_bounds_sz = 0;
    get_base(&lower_bounds, &lower_bounds_sz);

    for (uint64_t i = 0; i < lower_bounds_sz; ++i)
    {
        if (lower_bounds[i].ri == ri)
        {
            return lower_bounds + i;
        }
    }

    return NULL;
}

void
identifier_collector :: squash_gaps(counter* c)
{
    std::sort(m_gaps.begin(), m_gaps.end());
    size_t idx = 0;

    while (idx < m_gaps.size() && m_gaps[idx].ri < c->ri)
    {
        ++idx;
    }

    if (idx == m_gaps.size())
    {
        return;
    }

    if (m_gaps[idx].ri > c->ri)
    {
        return;
    }

    size_t end_idx = idx;

    while (c->ri == m_gaps[end_idx].ri && 
           c->count == m_gaps[end_idx].count)
    {
        e::atomic::store_64_nobarrier(&c->count, m_gaps[end_idx].count + 1);
        ++end_idx;
    }

    while (end_idx < m_gaps.size())
    {
        m_gaps[idx] = m_gaps[end_idx];
        ++idx;
        ++end_idx;
    }

    m_gaps.resize(idx);
    return;
}
