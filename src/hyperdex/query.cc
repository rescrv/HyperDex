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
#include "hyperdex/query.h"

hyperdex :: query :: query(size_t n)
    : m_values(n)
    , m_mask(n, false)
{
}

hyperdex :: query :: ~query()
{
}

void
hyperdex :: query :: set(size_t idx, const std::string& val)
{
    assert(m_values.size() > idx);
    assert(m_mask.size() > idx);

    m_mask[idx] = true;
    m_values[idx] = val;
}

void
hyperdex :: query :: unset(size_t idx)
{
    assert(m_values.size() > idx);
    assert(m_mask.size() > idx);

    m_mask[idx] = false;
    m_values[idx] = "";
}

void
hyperdex :: query :: clear()
{
    m_values = std::vector<std::string>(m_values.size());
    m_mask = std::vector<bool>(m_mask.size(), false);
}

bool
hyperdex :: query :: matches(const std::vector<std::string>& row)
                     const
{
    assert(m_values.size() == row.size());
    assert(m_mask.size() == row.size());

    for (size_t i = 0; i < m_mask.size(); ++i)
    {
        if (m_mask[i] && m_values[i] != row[i])
        {
            return false;
        }
    }

    return true;
}
