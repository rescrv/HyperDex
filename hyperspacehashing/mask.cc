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
#include "hyperspacehashing/hyperspacehashing/mask.h"
#include "hyperspacehashing/bithacks.h"
#include "hyperspacehashing/cfloat.h"
#include "hyperspacehashing/hashes_internal.h"

hyperspacehashing :: mask :: coordinate :: coordinate()
    : primary_mask()
    , primary_hash()
    , secondary_lower_mask()
    , secondary_lower_hash()
    , secondary_upper_mask()
    , secondary_upper_hash()
{
}

hyperspacehashing :: mask :: coordinate :: coordinate(uint64_t pmask, uint64_t phash,
                                                      uint64_t slmask, uint64_t slhash,
                                                      uint64_t sumask, uint64_t suhash)
    : primary_mask(pmask)
    , primary_hash(phash)
    , secondary_lower_mask(slmask)
    , secondary_lower_hash(slhash)
    , secondary_upper_mask(sumask)
    , secondary_upper_hash(suhash)
{
}

hyperspacehashing :: mask :: coordinate :: coordinate(const coordinate& other)
    : primary_mask(other.primary_mask)
    , primary_hash(other.primary_hash)
    , secondary_lower_mask(other.secondary_lower_mask)
    , secondary_lower_hash(other.secondary_lower_hash)
    , secondary_upper_mask(other.secondary_upper_mask)
    , secondary_upper_hash(other.secondary_upper_hash)
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
    uint64_t mask = primary_mask & other.primary_mask;
    return (primary_hash & mask) == (other.primary_hash & mask);
}

bool
hyperspacehashing :: mask :: coordinate :: secondary_intersects(const coordinate& other) const
{
    uint64_t lmask = secondary_lower_mask & other.secondary_lower_mask;
    uint64_t umask = secondary_upper_mask & other.secondary_upper_mask;
    return (secondary_lower_hash & lmask) == (other.secondary_lower_hash & lmask) &&
           (secondary_upper_hash & umask) == (other.secondary_upper_hash & umask);
}

bool
hyperspacehashing :: mask :: coordinate :: operator == (const coordinate& rhs) const
{
    return primary_mask == rhs.primary_mask &&
           primary_hash == rhs.primary_hash &&
           secondary_lower_mask == rhs.secondary_lower_mask &&
           secondary_lower_hash == rhs.secondary_lower_hash &&
           secondary_upper_mask == rhs.secondary_upper_mask &&
           secondary_upper_hash == rhs.secondary_upper_hash;
}

hyperspacehashing :: mask :: hasher :: hasher(const std::vector<hash_t>& funcs)
    : m_funcs(funcs)
    , m_num()
    , m_nums(funcs.size(), -1)
    , m_space(funcs.size(), 64)
{
    assert(m_funcs.size() >= 1);

    for (size_t i = 1; m_num < 128 && i < m_funcs.size(); ++i)
    {
        switch (m_funcs[i])
        {
            case EQUALITY:
                m_nums[i] = m_num;
                ++m_num;
                break;
            case RANGE:
                m_nums[i] = m_num;
                ++m_num;
                break;
            case NONE:
                break;
            default:
                abort();
        }
    }

    if (m_num > 0)
    {
        unsigned int numbits = 128 / m_num;
        unsigned int plusones = 128 % m_num;

        for (size_t i = 0; i < m_funcs.size(); ++i)
        {
            m_space[i] = std::min(64U, numbits + (m_nums[i] < plusones ? 1 : 0));
        }
    }
}

hyperspacehashing :: mask :: hasher :: hasher(const hasher& other)
    : m_funcs(other.m_funcs)
    , m_num(other.m_num)
    , m_nums(other.m_nums)
    , m_space(other.m_space)
{
    assert(m_funcs.size() >= 1);
    assert(m_funcs.size() == m_nums.size());
    assert(m_nums.size() == m_space.size());
}

hyperspacehashing :: mask :: hasher :: ~hasher() throw ()
{
}

hyperspacehashing::mask::coordinate
hyperspacehashing :: mask :: hasher :: hash(const e::slice& key) const
{
    uint64_t key_mask = 0;
    uint64_t key_hash = 0;

    switch (m_funcs[0])
    {
        case EQUALITY:
            key_mask = UINT64_MAX;
            key_hash = cityhash(key);
            break;
        case RANGE:
            key_mask = UINT64_MAX;
            key_hash = cfloat(lendian(key), 64);
            break;
        case NONE:
            key_mask = 0;
            key_hash = 0;
            break;
        default:
            abort();
    }

    return coordinate(key_mask, key_hash, 0, 0, 0, 0);
}

hyperspacehashing::mask::coordinate
hyperspacehashing :: mask :: hasher :: hash(const e::slice& key,
                                            const std::vector<e::slice>& value) const
{
    coordinate primary = hash(key);
    coordinate secondary = hash(value);
    return coordinate(primary.primary_mask, primary.primary_hash,
                      secondary.secondary_lower_mask, secondary.secondary_lower_hash,
                      secondary.secondary_upper_mask, secondary.secondary_upper_hash);
}

hyperspacehashing::mask::coordinate
hyperspacehashing :: mask :: hasher :: hash(const std::vector<e::slice>& value) const
{
    assert(value.size() + 1 == m_funcs.size());
    assert(m_nums.size() == m_funcs.size());
    assert(m_nums.size() == m_space.size());
    uint64_t masks[128];
    uint64_t hashes[128];
    memset(masks, 0, sizeof(masks));
    memset(hashes, 0, sizeof(hashes));

    for (size_t i = 1; i < m_funcs.size(); ++i)
    {
        if (m_nums[i] != static_cast<unsigned int>(-1))
        {
            uint64_t cfl;

            switch (m_funcs[i])
            {
                case EQUALITY:
                    masks[m_nums[i]] = UINT64_MAX;
                    hashes[m_nums[i]] = cityhash(value[i - 1]);
                    break;
                case RANGE:
                    assert(m_space[i] <= 64);
                    cfl = cfloat(lendian(value[i - 1]), m_space[i]);
                    masks[m_nums[i]] = UINT64_MAX;
                    hashes[m_nums[i]] = cfl;
                    break;
                case NONE:
                    abort();
                default:
                    abort();
            }
        }
    }

    uint64_t lower_mask = 0;
    uint64_t upper_mask = 0;
    double_lower_interlace(masks, m_num, &lower_mask, &upper_mask);
    uint64_t lower_hash = 0;
    uint64_t upper_hash = 0;
    double_lower_interlace(hashes, m_num, &lower_hash, &upper_hash);
    return coordinate(0, 0, lower_mask, lower_hash, upper_mask, upper_hash);
}

hyperspacehashing::mask::coordinate
hyperspacehashing :: mask :: hasher :: hash(const search& s) const
{
    assert(s.size() == m_funcs.size());
    assert(m_nums.size() == m_funcs.size());
    assert(m_nums.size() == m_space.size());
    assert(m_nums.size() == s.size());

    // First create the primary search components
    uint64_t primary_mask = 0;
    uint64_t primary_hash = 0;

    if (m_nums[0] != static_cast<unsigned int>(-1))
    {
        uint64_t lower = 0;
        uint64_t upper = 0;
        uint64_t clower = 0;
        uint64_t cupper = 0;

        switch (m_funcs[0])
        {
            case EQUALITY:

                if (s.is_equality(0))
                {
                    primary_mask = UINT64_MAX;
                    primary_hash = cityhash(s.equality_value(0));
                }

                break;
            case RANGE:

                if (s.is_range(0))
                {
                    s.range_value(0, &lower, &upper);
                    clower = cfloat(lower, 64);
                    cupper = cfloat(upper, 64);
                    cfloat_range(clower, cupper, 64, &primary_mask, &primary_hash);
                }

                break;
            case NONE:
                abort();
            default:
                abort();
        }
    }

    // Then create the secondary search components
    uint64_t masks[128];
    uint64_t hashes[128];
    memset(masks, 0, sizeof(masks));
    memset(hashes, 0, sizeof(hashes));

    for (size_t i = 1; i < s.size(); ++i)
    {
        if (m_nums[i] != static_cast<unsigned int>(-1))
        {
            uint64_t lower = 0;
            uint64_t upper = 0;
            uint64_t clower = 0;
            uint64_t cupper = 0;

            switch (m_funcs[i])
            {
                case EQUALITY:

                    if (s.is_equality(i))
                    {
                        masks[m_nums[i]] = UINT64_MAX;
                        hashes[m_nums[i]] = cityhash(s.equality_value(i));
                    }

                    break;
                case RANGE:

                    if (s.is_range(i))
                    {
                        s.range_value(i, &lower, &upper);
                        clower = cfloat(lower, m_space[i]);
                        cupper = cfloat(upper, m_space[i]);
                        cfloat_range(clower, cupper, m_space[i], &masks[m_nums[i]], &hashes[m_nums[i]]);
                    }

                    break;
                case NONE:
                    abort();
                default:
                    abort();
            }
        }
    }

    uint64_t lower_mask = 0;
    uint64_t lower_hash = 0;
    uint64_t upper_mask = 0;
    uint64_t upper_hash = 0;
    double_lower_interlace(masks, m_num, &lower_mask, &upper_mask);
    double_lower_interlace(hashes, m_num, &lower_hash, &upper_hash);
    return coordinate(0, 0, lower_mask, lower_hash, upper_mask, upper_hash);
}
