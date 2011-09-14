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
#include "hyperspacehashing/prefix.h"

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

hyperspacehashing :: prefix :: search_coordinate :: search_coordinate()
    : m_mask(0)
    , m_point(0)
{
}

hyperspacehashing :: prefix :: search_coordinate :: search_coordinate(const search_coordinate& other)
    : m_mask(other.m_mask)
    , m_point(other.m_point)
{
}

hyperspacehashing :: prefix :: search_coordinate :: ~search_coordinate() throw ()
{
}

bool
hyperspacehashing :: prefix :: search_coordinate :: matches(const coordinate& other) const
{
    uint64_t intersection = lookup_msb_mask[other.prefix] & m_mask;
    return (m_point & intersection) == (other.point & intersection);
}

hyperspacehashing :: prefix :: search_coordinate :: search_coordinate(uint64_t mask, uint64_t point)
    : m_mask(mask)
    , m_point(point)
{
}

hyperspacehashing :: prefix :: hasher :: hasher(const e::bitfield& dims,
                                                const std::vector<hash_t> funcs)
    : m_dims(dims)
    , m_funcs(funcs)
{
    assert(m_dims.bits() == m_funcs.size());
    assert(m_funcs.size() >= 1);
}

hyperspacehashing :: prefix :: hasher :: ~hasher() throw ()
{
}

hyperspacehashing::prefix::coordinate
hyperspacehashing :: prefix :: hasher :: hash(const e::buffer& key) const
{
    assert(m_dims.get(0));

#ifndef NDEBUG
    bool all_false = true;

    for (size_t i = 1; i < m_dims.bits(); ++i)
    {
        if (m_dims.get(i))
        {
            all_false = false;
        }
    }

    assert(all_false);
#endif

    return coordinate(64, m_funcs[0](key));
}

hyperspacehashing::prefix::coordinate
hyperspacehashing :: prefix :: hasher :: hash(const e::buffer& key, const std::vector<e::buffer>& value) const
{
    assert(value.size() + 1 == m_funcs.size());
    std::vector<uint64_t> hashes;
    hashes.reserve(m_funcs.size());

    if (m_dims.get(0))
    {
        hashes.push_back(m_funcs[0](key));
    }

    for (size_t i = 1; i < m_funcs.size(); ++i)
    {
        if (m_dims.get(i))
        {
            hashes.push_back(m_funcs[i](value[i - 1]));
        }
    }

    return coordinate(64, upper_interlace(hashes));
}

hyperspacehashing::prefix::search_coordinate
hyperspacehashing :: prefix :: hasher :: hash(const search& s) const
{
    assert(s.size() == m_funcs.size());
    std::vector<uint64_t> hashes;
    hashes.reserve(m_funcs.size());
    std::vector<uint64_t> masks;
    masks.reserve(m_funcs.size());

    for (size_t i = 0; i < m_funcs.size(); ++i)
    {
        if (m_dims.get(i) && s.is_equality(i))
        {
            hashes.push_back(m_funcs[i](s.equality_value(i)));
            masks.push_back(UINT64_MAX);
        }
    }

    return search_coordinate(upper_interlace(masks), upper_interlace(hashes));
}
