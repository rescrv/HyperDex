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
//
#define __STDC_LIMIT_MACROS

// HyperspaceHashing
#include "bithacks.h"
#include "cfloat.h"
#include "hashes_internal.h"
#include "hyperspacehashing/prefix.h"
#include "range_match.h"

hyperspacehashing :: prefix :: coordinate :: coordinate()
    : prefix(0)
    , point(0)
{
}

hyperspacehashing :: prefix :: coordinate :: coordinate(uint8_t pr,
                                                        uint64_t po)
    : prefix(pr)
    , point(po)
{
}

hyperspacehashing :: prefix :: coordinate :: coordinate(const coordinate& other)
    : prefix(other.prefix)
    , point(other.point)
{
}

hyperspacehashing :: prefix :: coordinate :: ~coordinate() throw ()
{
}

bool
hyperspacehashing :: prefix :: coordinate :: contains(const coordinate& other) const
{
    uint64_t mask = lookup_msb_mask[prefix];
    return prefix <= other.prefix && ((mask & point) == (mask & other.point));
}

bool
hyperspacehashing :: prefix :: coordinate :: intersects(const coordinate& other) const
{
    uint64_t mask = lookup_msb_mask[std::min(prefix, other.prefix)];
    return (mask & point) == (mask & other.point);
}

hyperspacehashing :: prefix :: search_coordinate :: search_coordinate()
    : m_mask(0)
    , m_point(0)
    , m_range()
{
}

hyperspacehashing :: prefix :: search_coordinate :: search_coordinate(const search_coordinate& other)
    : m_mask(other.m_mask)
    , m_point(other.m_point)
    , m_range(other.m_range)
{
}

hyperspacehashing :: prefix :: search_coordinate :: ~search_coordinate() throw ()
{
}

hyperspacehashing::prefix::search_coordinate&
hyperspacehashing :: prefix :: search_coordinate :: operator = (const search_coordinate& rhs)
{
    // We rely upon others catching self-assignment.
    m_mask = rhs.m_mask;
    m_point = rhs.m_point;
    m_range = rhs.m_range;
    return *this;
}


bool
hyperspacehashing :: prefix :: search_coordinate :: matches(const coordinate& other) const
{
    uint64_t intersection = lookup_msb_mask[other.prefix] & m_mask;

    if ((m_point & intersection) != (other.point & intersection))
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

hyperspacehashing :: prefix :: search_coordinate :: search_coordinate(uint64_t mask, uint64_t point,
                                                                      const std::vector<range_match>& range)
    : m_mask(mask)
    , m_point(point)
    , m_range(range)
{
}

hyperspacehashing :: prefix :: hasher :: hasher(const std::vector<hash_t>& funcs)
    : m_funcs(funcs)
{
}

hyperspacehashing :: prefix :: hasher :: hasher(const hasher& other)
    : m_funcs(other.m_funcs)
{
}

hyperspacehashing :: prefix :: hasher :: ~hasher() throw ()
{
}

hyperspacehashing::prefix::coordinate
hyperspacehashing :: prefix :: hasher :: hash(const e::slice& key) const
{
#ifndef NDEBUG
    bool all_none = true;

    for (size_t i = 1; i < m_funcs.size(); ++i)
    {
        if (m_funcs[i] != NONE)
        {
            all_none = false;
        }
    }

    assert(all_none);
#endif

    switch (m_funcs[0])
    {
        case EQUALITY:
            return coordinate(64, cityhash(key));
        case RANGE:
            return coordinate(64, cfloat(lendian(key), 64));
        case NONE:
            abort();
        default:
            abort();
    }
}

hyperspacehashing::prefix::coordinate
hyperspacehashing :: prefix :: hasher :: hash(const e::slice& key, const std::vector<e::slice>& value) const
{
    assert(value.size() + 1 == m_funcs.size());
    size_t num = 0;
    uint64_t hashes[64];

    switch (m_funcs[0])
    {
        case EQUALITY:
            hashes[num] = cityhash(key);
            ++num;
            break;
        case RANGE:
            hashes[num] = 0;
            ++num;
            break;
        case NONE:
            break;
        default:
            abort();
    }

    for (size_t i = 1; num < 64 && i < m_funcs.size(); ++i)
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
                abort();
        }
    }

    if (num == 0)
    {
        return coordinate(0, 0);
    }

    unsigned int numbits = 64 / num;
    unsigned int plusones = 64 % num;
    unsigned int space;
    size_t idx = 0;

    switch (m_funcs[0])
    {
        case EQUALITY:
            ++idx;
            break;
        case RANGE:
            space = numbits + (idx < plusones ? 1 : 0);
            hashes[idx] = cfloat(lendian(key), space);
            hashes[idx] <<= 64 - space;
            ++idx;
            break;
        case NONE:
            break;
        default:
            abort();
    }

    for (size_t i = 1; idx < num && i < m_funcs.size(); ++i)
    {
        switch (m_funcs[i])
        {
            case EQUALITY:
                ++idx;
                break;
            case RANGE:
                space = numbits + (idx < plusones ? 1 : 0);
                hashes[idx] = cfloat(lendian(value[i - 1]), space);
                hashes[idx] <<= 64 - space;
                ++idx;
                break;
            case NONE:
                break;
            default:
                abort();
        }
    }

    return coordinate(64, upper_interlace(hashes, num));
}

hyperspacehashing::prefix::search_coordinate
hyperspacehashing :: prefix :: hasher :: hash(const search& s) const
{
    assert(s.size() == m_funcs.size());
    size_t num = 0;
    uint64_t masks[64];
    uint64_t hashes[64];

    for (size_t i = 0; num < 64 && i < s.size(); ++i)
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
                abort();
        }
    }

    std::vector<range_match> range;

    if (num > 0)
    {
        uint64_t scratch[64];
        memset(scratch, 0, sizeof(scratch));
        unsigned int numbits = 64 / num;
        unsigned int plusones = 64 % num;
        unsigned int space;
        size_t idx = 0;

        for (size_t i = 0; idx < num && i < s.size(); ++i)
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
                // Create the partial matching which is folded into
                // equality coordinate.
                masks[idx] <<= (64 - space);
                hashes[idx] <<= (64 - space);
                // Create interleaved variations of the clower, cupper.
                clower <<= (64 - space);
                cupper <<= (64 - space);
                scratch[idx] = clower;
                clower = upper_interlace(scratch, num);
                scratch[idx] = cupper;
                cupper = upper_interlace(scratch, num);
                scratch[idx] = UINT64_MAX;
                uint64_t cmask = upper_interlace(scratch, num);
                range.push_back(range_match(i, lower, upper, cmask, clower, cupper));
                scratch[idx] = 0;
            }
            else if (s.is_range(i))
            {
                uint64_t lower;
                uint64_t upper;
                s.range_value(i, &lower, &upper);
                range.push_back(range_match(i, lower, upper, 0, 0, 0));
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
                    abort();
            }
        }
    }

    size_t idx = 0;

    for (size_t i = 0; i < s.size(); ++i)
    {
        if (s.is_range(i) && (idx >= num || m_funcs[i] != RANGE))
        {
            uint64_t lower;
            uint64_t upper;
            s.range_value(i, &lower, &upper);
            range.push_back(range_match(i, lower, upper, 0, 0, 0));
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
                abort();
        }
    }

    return search_coordinate(upper_interlace(masks, num), upper_interlace(hashes, num), range);
}

hyperspacehashing::prefix::hasher&
hyperspacehashing :: prefix :: hasher :: operator = (const hasher& rhs)
{
    m_funcs = rhs.m_funcs;
    return *this;
}
