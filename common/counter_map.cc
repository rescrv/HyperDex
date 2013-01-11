// Copyright (c) 2012, Cornell University
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

// HyperDex
#include "common/counter_map.h"

using hyperdex::counter_map;

counter_map :: counter_map()
    : m_counters()
{
}

counter_map :: ~counter_map() throw ()
{
}

void
counter_map :: adopt(const std::vector<region_id>& ris)
{
    std::vector<std::pair<region_id, uint64_t> > tmp;
    std::vector<std::pair<region_id, uint64_t> >::const_iterator lhs = m_counters.begin();
    std::vector<region_id>::const_iterator rhs = ris.begin();

    while (lhs != m_counters.end() && rhs != ris.end())
    {
        if (lhs->first == *rhs)
        {
            tmp.push_back(*lhs);
            ++lhs;
            ++rhs;
        }
        else if (lhs->first < *rhs)
        {
            ++lhs;
        }
        else if (lhs->first > *rhs)
        {
            tmp.push_back(std::make_pair(*rhs, static_cast<uint64_t>(1)));
            ++rhs;
        }
    }

    while (rhs != ris.end())
    {
        tmp.push_back(std::make_pair(*rhs, static_cast<uint64_t>(1)));
        ++rhs;
    }

    tmp.swap(m_counters);
}

void
counter_map :: peek(std::map<region_id, uint64_t>* ris)
{
    for (size_t i = 0; i < m_counters.size(); ++i)
    {
        (*ris)[m_counters[i].first] = m_counters[i].second;
    }
}

bool
counter_map :: lookup(const region_id& ri, uint64_t* count)
{
    std::vector<std::pair<region_id, uint64_t> >::iterator it;
    it = std::lower_bound(m_counters.begin(),
                          m_counters.end(),
                          std::make_pair(ri, static_cast<uint64_t>(0)));

    if (it == m_counters.end() || ri != it->first)
    {
        return false;
    }

    *count = __sync_fetch_and_add(&it->second, 1);
    return true;
}

bool
counter_map :: take_max(const region_id& ri, uint64_t count)
{
    std::vector<std::pair<region_id, uint64_t> >::iterator it;
    it = std::lower_bound(m_counters.begin(),
                          m_counters.end(),
                          std::make_pair(ri, static_cast<uint64_t>(0)));

    if (it == m_counters.end() || ri != it->first)
    {
        return false;
    }

    uint64_t current = __sync_fetch_and_add(&it->second, 0);

    if (current < count)
    {
        __sync_fetch_and_add(&it->second, count - current);
    }

    return true;
}
