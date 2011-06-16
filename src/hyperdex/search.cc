// Copyright (c) 2011, Cornell University
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

// C
#include <cassert>

// HyperDex
#include "hyperdex/search.h"

hyperdex :: search :: search(size_t n)
    : m_values(n)
    , m_mask(n)
{
}

hyperdex :: search :: ~search()
{
}

void
hyperdex :: search :: set(size_t idx, const e::buffer& val)
{
    assert(m_values.size() > idx);
    assert(m_mask.bits() > idx);

    m_mask.set(idx);
    m_values[idx] = val;
}

void
hyperdex :: search :: unset(size_t idx)
{
    assert(m_values.size() > idx);
    assert(m_mask.bits() > idx);

    m_mask.unset(idx);
    m_values[idx] = e::buffer();
}

void
hyperdex :: search :: clear()
{
    m_values = std::vector<e::buffer>(m_values.size());
    m_mask = e::bitfield(m_values.size());
}

bool
hyperdex :: search :: matches(const e::buffer& key,
                              const std::vector<e::buffer>& value)
                      const
{
    assert(m_values.size() == value.size() + 1);
    assert(m_mask.bits() == value.size() + 1);

    if (m_mask.get(0) && m_values[0] != key)
    {
        return false;
    }

    for (size_t i = 1; i < m_mask.bits(); ++i)
    {
        if (m_mask.get(i) && m_values[i] != value[i - 1])
        {
            return false;
        }
    }

    return true;
}
