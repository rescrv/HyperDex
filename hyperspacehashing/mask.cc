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
#include "cfloat.h"
#include "hashes_internal.h"
#include "hyperspacehashing/mask.h"
#include "range_match.h"

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
    , m_range()
{
}

hyperspacehashing :: mask :: search_coordinate :: search_coordinate(const search_coordinate& other)
    : m_terms(other.m_terms)
    , m_equality(other.m_equality)
    , m_range(other.m_range)
{
}

hyperspacehashing :: mask :: search_coordinate :: ~search_coordinate() throw ()
{
}

bool
hyperspacehashing :: mask :: search_coordinate :: matches(const coordinate& other) const
{
    if (!m_equality.intersects(other))
    {
        return false;
    }

    for (size_t i = 0; i < m_range.size(); ++i)
    {
        if (!m_range[i].matches(other))
        {
            return false;
        }
    }

    return true;
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

    for (size_t i = 0; i < m_range.size(); ++i)
    {
        if (!m_range[i].matches(key, value))
        {
            return false;
        }
    }

    return true;
}

hyperspacehashing :: mask :: search_coordinate :: search_coordinate(const search& terms,
                                                                    const coordinate& equality,
                                                                    const std::vector<range_match>& range)
    : m_terms(terms)
    , m_equality(equality)
    , m_range(range)
{
}

hyperspacehashing :: mask :: hasher :: hasher(const std::vector<hash_t>& funcs)
    : m_funcs(funcs)
{
    assert(m_funcs.size() >= 1);
}

hyperspacehashing :: mask :: hasher :: hasher(const hasher& other)
    : m_funcs(other.m_funcs)
{
    assert(m_funcs.size() >= 1);
}

hyperspacehashing :: mask :: hasher :: ~hasher() throw ()
{
}

hyperspacehashing::mask::coordinate
hyperspacehashing :: mask :: hasher :: hash(const e::buffer& key) const
{
    uint32_t key_mask = 0;
    uint32_t key_hash = 0;

    switch (m_funcs[0])
    {
        case EQUALITY:
            key_mask = UINT32_MAX;
            key_hash = static_cast<uint32_t>(cityhash(key));
            break;
        case RANGE:
            key_mask = UINT32_MAX;
            key_hash = static_cast<uint32_t>(cfloat(lendian(key), 32));
            break;
        case NONE:
            key_mask = 0;
            key_hash = 0;
            break;
        default:
            assert(false);
    }

    return coordinate(key_mask, key_hash, 0, 0);
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
    size_t sz = std::min(static_cast<size_t>(33), m_funcs.size());
    size_t num = 0;
    uint64_t hashes[64];

    for (size_t i = 1; i < sz; ++i)
    {
        switch (m_funcs[i])
        {
            case EQUALITY:
                hashes[num] = cityhash(value[i - 1]);
                ++num;
                break;
            case RANGE:
                hashes[num] = 0;
                ++num;
                break;
            case NONE:
                break;
            default:
                assert(false);
        }
    }

    if (num == 0)
    {
        return coordinate(0, 0, 0, 0);
    }

    unsigned int numbits = 32 / num;
    unsigned int plusones = 32 % num;
    unsigned int space;
    size_t idx = 0;

    for (size_t i = 1; i < sz; ++i)
    {
        switch (m_funcs[i])
        {
            case EQUALITY:
                ++idx;
                break;
            case RANGE:
                space = numbits + (idx < plusones ? 1 : 0);
                hashes[idx] = cfloat(lendian(value[i - 1]), space);
                ++idx;
                break;
            case NONE:
                break;
            default:
                assert(false);
        }
    }

    uint64_t value_mask = UINT32_MAX;
    uint64_t value_hash = lower_interlace(hashes, num);
    return coordinate(0, 0, value_mask, value_hash);
}

hyperspacehashing::mask::search_coordinate
hyperspacehashing :: mask :: hasher :: hash(const search& s) const
{
    assert(s.size() == m_funcs.size());
    size_t sz = std::min(static_cast<size_t>(33), m_funcs.size());
    std::vector<range_match> range;
    uint32_t primary_mask = 0;
    uint32_t primary_hash = 0;

    // First create the primary search components
    switch (m_funcs[0])
    {
        case EQUALITY:

            if (s.is_equality(0))
            {
                primary_mask = UINT32_MAX;
                primary_hash = cityhash(s.equality_value(0));
            }
            else if (s.is_range(0))
            {
                uint64_t lower;
                uint64_t upper;
                s.range_value(0, &lower, &upper);
                range.push_back(range_match(0, lower, upper, 0, 0, 0));
            }

            break;
        case RANGE:

            if (s.is_range(0))
            {
                uint64_t lower;
                uint64_t upper;
                s.range_value(0, &lower, &upper);
                uint64_t clower = cfloat(lower, 32);
                uint64_t cupper = cfloat(upper, 32);
                uint64_t m;
                uint64_t h;
                cfloat_range(clower, cupper, 32, &m, &h);
                primary_mask = static_cast<uint32_t>(m);
                primary_hash = static_cast<uint32_t>(h);
                range.push_back(range_match(0, lower, upper, UINT32_MAX, clower, cupper));
            }

            break;
        case NONE:
            break;
        default:
            assert(false);
    }

    // Then create the secondary search components
    size_t num = 0;
    uint64_t masks[32];
    uint64_t hashes[32];

    for (size_t i = 1; i < sz; ++i)
    {
        switch (m_funcs[i])
        {
            case EQUALITY:

                if (s.is_equality(i))
                {
                    masks[num] = UINT64_MAX;
                    hashes[num] = cityhash(s.equality_value(i));
                }
                else
                {
                    masks[num] = 0;
                    hashes[num] = 0;
                }

                ++num;
                break;
            case RANGE:
                masks[num] = 0;
                hashes[num] = 0;
                ++num;
                break;
            case NONE:
                break;
            default:
                assert(false);
        }
    }

    if (num > 0)
    {
        uint64_t scratch[32];
        memset(scratch, 0, sizeof(scratch));
        unsigned int numbits = 32 / num;
        unsigned int plusones = 32 % num;
        unsigned int space;
        size_t idx = 0;

        for (size_t i = 1; i < sz; ++i)
        {
            if (s.is_range(i) && m_funcs[i] == RANGE)
            {
                uint64_t lower;
                uint64_t upper;
                s.range_value(i, &lower, &upper);
                space = numbits + (idx < plusones ? 1 : 0);
                uint64_t clower = cfloat(lower, space);
                uint64_t cupper = cfloat(upper, space);
                cfloat_range(clower, cupper, space, &masks[idx], &hashes[idx]);
                // There is nothing to be done to make the cfloat_range result
                // fold correctly into the equality coordinate.  It will happen
                // on its own.
                scratch[idx] = clower;
                clower = lower_interlace(scratch, num);
                scratch[idx] = cupper;
                cupper = lower_interlace(scratch, num);
                scratch[idx] = UINT64_MAX;
                uint64_t cmask = lower_interlace(scratch, num);
                range.push_back(range_match(i, lower, upper, cmask, clower, cupper));
                scratch[idx] = 0;
            }

            switch (m_funcs[i])
            {
                case EQUALITY:
                    ++idx;
                    break;
                case RANGE:
                    ++idx;
                    break;
                case NONE:
                    break;
                default:
                    assert(false);
            }
        }
    }

    for (size_t i = 1; i < s.size(); ++i)
    {
        if (s.is_range(i) && (i >= sz || m_funcs[i] != RANGE))
        {
            uint64_t lower;
            uint64_t upper;
            s.range_value(i, &lower, &upper);
            range.push_back(range_match(i, lower, upper, 0, 0, 0));
        }
    }

    coordinate c(primary_mask, primary_hash,
                 lower_interlace(masks, num), lower_interlace(hashes, num));
    return search_coordinate(s, c, range);
}

std::ostream&
hyperspacehashing :: mask :: operator << (std::ostream& lhs, const coordinate& rhs)
{
    lhs << "coordinate(" << rhs.primary_mask << ", " << rhs.primary_hash
        << ", " << rhs.secondary_mask << ", " << rhs.secondary_hash << ")";
    return lhs;
}
