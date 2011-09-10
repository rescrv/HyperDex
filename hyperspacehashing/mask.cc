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
hyperspacehashing :: mask :: coordinate :: contains(const coordinate& other) const
{
    return primary_contains(other) && secondary_contains(other);
}

bool
hyperspacehashing :: mask :: coordinate :: primary_contains(const coordinate& other) const
{
    return (primary_mask & other.primary_mask) == primary_mask &&
           (primary_hash & primary_mask) == (other.primary_hash & primary_mask);
}

bool
hyperspacehashing :: mask :: coordinate :: secondary_contains(const coordinate& other) const
{
    return (secondary_mask & other.secondary_mask) == secondary_mask &&
           (secondary_hash & secondary_mask) == (other.secondary_hash & secondary_mask);
}

hyperspacehashing :: mask :: hasher :: hasher(const std::vector<hash_t> funcs)
    : m_funcs(funcs)
{
    assert(m_funcs.size() >= 1);
}

hyperspacehashing :: mask :: hasher :: ~hasher() throw ()
{
}

hyperspacehashing::mask::coordinate
hyperspacehashing :: mask :: hasher :: hash(const e::buffer& key)
{
    assert(m_funcs.size() >= 1);
    uint32_t key_hash = static_cast<uint32_t>(m_funcs[0](key));
    return coordinate(UINT32_MAX, key_hash, 0, 0);
}

hyperspacehashing::mask::coordinate
hyperspacehashing :: mask :: hasher :: hash(const e::buffer& key,
                                            const std::vector<e::buffer>& value)
{
    coordinate primary = hash(key);
    coordinate secondary = hash(value);
    return coordinate(primary.primary_mask, primary.primary_hash,
                      secondary.secondary_mask, secondary.secondary_hash);
}

hyperspacehashing::mask::coordinate
hyperspacehashing :: mask :: hasher :: hash(const std::vector<e::buffer>& value)
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

hyperspacehashing::mask::coordinate
hyperspacehashing :: mask :: hasher :: hash(const equality_wildcard& ewc) const
{
    assert(ewc.size() == m_funcs.size());
    coordinate c;

    if (ewc.isset(0))
    {
        c.primary_mask = UINT32_MAX;
        c.primary_hash = static_cast<uint32_t>(m_funcs[0](ewc.get(0)));
    }
    else
    {
        c.primary_mask = 0;
        c.primary_hash = 0;
    }

    std::vector<uint64_t> mask_hashes;
    std::vector<uint64_t> value_hashes;
    mask_hashes.reserve(m_funcs.size() - 1);
    value_hashes.reserve(m_funcs.size() - 1);

    for (size_t i = 1; i < m_funcs.size(); ++i)
    {
        if (ewc.isset(i))
        {
            mask_hashes[i - 1] = UINT64_MAX;
            value_hashes[i - 1] = m_funcs[i](ewc.get(i));
        }
        else
        {
            mask_hashes[i - 1] = 0;
            value_hashes[i - 1] = 0;
        }
    }

    c.secondary_mask = static_cast<uint32_t>(lower_interlace(mask_hashes));
    c.secondary_hash = static_cast<uint32_t>(lower_interlace(value_hashes));
    return c;
}

std::ostream&
hyperspacehashing :: mask :: operator << (std::ostream& lhs, const coordinate& rhs)
{
    lhs << "coordinate(" << rhs.primary_mask << ", " << rhs.primary_hash
        << ", " << rhs.secondary_mask << ", " << rhs.secondary_hash << ")";
    return lhs;
}
