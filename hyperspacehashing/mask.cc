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

#define __STDC_LIMIT_MACROS

// HyperspaceHashing
#include "bithacks.h"
#include "hashes_internal.h"
#include "hyperspacehashing/mask.h"

hyperspacehashing :: mask :: coordinate :: coordinate()
    : primary_mask(0)
    , primary_hash(0)
    , secondary_mask(0)
    , secondary_hash(0)
{
}

hyperspacehashing :: mask :: coordinate :: coordinate(uint32_t pm,
                                                      uint32_t ph,
                                                      uint32_t sm,
                                                      uint32_t sh)
    : primary_mask(pm)
    , primary_hash(ph)
    , secondary_mask(sm)
    , secondary_hash(sh)
{
}

hyperspacehashing :: mask :: coordinate :: coordinate(const coordinate& other)
    : primary_mask(other.primary_mask)
    , primary_hash(other.primary_hash)
    , secondary_mask(other.secondary_mask)
    , secondary_hash(other.secondary_hash)
{
}

hyperspacehashing :: mask :: coordinate :: ~coordinate() throw ()
{
}

bool
hyperspacehashing :: mask :: coordinate :: intersects(const coordinate& other) const
{
    return primary_intersects(other) && secondary_intersects(other);
}

bool
hyperspacehashing :: mask :: coordinate :: primary_intersects(const coordinate& other) const
{
    uint32_t mask = primary_mask & other.primary_mask;
    return (primary_hash & mask) == (other.primary_hash & mask);
}

bool
hyperspacehashing :: mask :: coordinate :: secondary_intersects(const coordinate& other) const
{
    uint32_t mask = secondary_mask & other.secondary_mask;
    return (secondary_hash & mask) == (other.secondary_hash & mask);
}

hyperspacehashing :: mask :: search_coordinate :: search_coordinate()
    : m_terms(0)
    , m_equality()
{
}

hyperspacehashing :: mask :: search_coordinate :: search_coordinate(const search_coordinate& other)
    : m_terms(other.m_terms)
    , m_equality(other.m_equality)
{
}

hyperspacehashing :: mask :: search_coordinate :: ~search_coordinate() throw ()
{
}

bool
hyperspacehashing :: mask :: search_coordinate :: matches(const coordinate& other) const
{
    return m_equality.intersects(other);
}

bool
hyperspacehashing :: mask :: search_coordinate :: matches(const e::buffer& key,
                                                          const std::vector<e::buffer>& value) const
{
    if (m_terms.is_equality(0) && m_terms.equality_value(0) != key)
    {
        return false;
    }

    for (size_t i = 1; i < m_terms.size(); ++i)
    {
        if (m_terms.is_equality(i) && m_terms.equality_value(i) != value[i - 1])
        {
            return false;
        }
    }

    return true;
}

hyperspacehashing :: mask :: search_coordinate :: search_coordinate(const search& terms,
                                                                    const coordinate& equality)
    : m_terms(terms)
    , m_equality(equality)
{
}

hyperspacehashing :: mask :: hasher :: hasher(const std::vector<hash_t> funcs)
    : m_funcs()
{
    convert(funcs, &m_funcs);
    assert(m_funcs.size() >= 1);
}

hyperspacehashing :: mask :: hasher :: ~hasher() throw ()
{
}

hyperspacehashing::mask::coordinate
hyperspacehashing :: mask :: hasher :: hash(const e::buffer& key) const
{
    assert(m_funcs.size() >= 1);
    uint32_t key_hash = static_cast<uint32_t>(m_funcs[0](key));
    return coordinate(UINT32_MAX, key_hash, 0, 0);
}

hyperspacehashing::mask::coordinate
hyperspacehashing :: mask :: hasher :: hash(const e::buffer& key,
                                            const std::vector<e::buffer>& value) const
{
    coordinate primary = hash(key);
    coordinate secondary = hash(value);
    return coordinate(primary.primary_mask, primary.primary_hash,
                      secondary.secondary_mask, secondary.secondary_hash);
}

hyperspacehashing::mask::coordinate
hyperspacehashing :: mask :: hasher :: hash(const std::vector<e::buffer>& value) const
{
    assert(value.size() + 1 == m_funcs.size());
    std::vector<uint64_t> value_hashes(value.size());

    for (size_t i = 1; i < m_funcs.size(); ++i)
    {
        value_hashes[i - 1] = m_funcs[i](value[i - 1]);
    }

    uint32_t value_hash = static_cast<uint32_t>(lower_interlace(value_hashes));
    return coordinate(0, 0, UINT32_MAX, value_hash);
}

hyperspacehashing::mask::search_coordinate
hyperspacehashing :: mask :: hasher :: hash(const search& s) const
{
    assert(s.size() == m_funcs.size());

    uint32_t primary_mask = 0;
    uint32_t primary_hash = 0;

    if (s.is_equality(0))
    {
        primary_mask = UINT32_MAX;
        primary_hash = static_cast<uint32_t>(m_funcs[0](s.equality_value(0)));
    }

    std::vector<uint64_t> value_masks(s.size());
    std::vector<uint64_t> value_hashes(s.size());

    for (size_t i = 1; i < m_funcs.size(); ++i)
    {
        if (s.is_equality(i))
        {
            value_masks[i - 1] = UINT64_MAX;
            value_hashes[i - 1] = m_funcs[i](s.equality_value(i));
        }
        else
        {
            value_masks[i - 1] = 0;
            value_hashes[i - 1] = 0;
        }
    }

    uint32_t secondary_mask = static_cast<uint32_t>(lower_interlace(value_masks));
    uint32_t secondary_hash = static_cast<uint32_t>(lower_interlace(value_hashes));
    coordinate coord(primary_mask, primary_hash, secondary_mask, secondary_hash);
    return search_coordinate(s, coord);
}

std::ostream&
hyperspacehashing :: mask :: operator << (std::ostream& lhs, const coordinate& rhs)
{
    lhs << "coordinate(" << rhs.primary_mask << ", " << rhs.primary_hash
        << ", " << rhs.secondary_mask << ", " << rhs.secondary_hash << ")";
    return lhs;
}
