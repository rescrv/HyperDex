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

// HyperspaceHashing
#include "hashes_internal.h"
#include <hyperspacehashing/search.h>

#include <iostream>

hyperspacehashing :: search :: search(size_t n)
    : m_equality_bits(n)
    , m_equality(n)
    , m_range_bits(n)
    , m_range_lower(n)
    , m_range_upper(n)
{
}

hyperspacehashing :: search :: ~search() throw ()
{
}

bool
hyperspacehashing :: search :: sanity_check() const
{
    return m_equality_bits.bits() == m_equality.size() &&
           m_equality.size() == m_range_bits.bits() &&
           m_range_bits.bits() == m_range_lower.size() &&
           m_range_lower.size() == m_range_upper.size();
}

size_t
hyperspacehashing :: search :: size() const
{
    assert(sanity_check());
    return m_equality.size();
}

bool
hyperspacehashing :: search :: is_equality(size_t idx) const
{
    assert(sanity_check());
    assert(idx < m_equality_bits.bits());
    return m_equality_bits.get(idx);
}

const e::slice&
hyperspacehashing :: search :: equality_value(size_t idx) const
{
    assert(sanity_check());
    assert(idx < m_equality_bits.bits());
    assert(m_equality_bits.get(idx));
    return m_equality[idx];
}

bool
hyperspacehashing :: search :: is_range(size_t idx) const
{
    assert(sanity_check());
    assert(idx < m_range_bits.bits());
    return m_range_bits.get(idx);
}

void
hyperspacehashing :: search :: range_value(size_t idx, uint64_t* lower, uint64_t* upper) const
{
    assert(sanity_check());
    assert(idx < m_range_bits.bits());
    assert(m_range_bits.get(idx));
    *lower = m_range_lower[idx];
    *upper = m_range_upper[idx];
}

bool
hyperspacehashing :: search :: matches(const e::slice& key, const std::vector<e::slice>& value) const
{
    assert(sanity_check());

    if (m_equality_bits.get(0))
    {
        if (m_equality[0] != key)
        {
            return false;
        }
    }
    else if (m_range_bits.get(0))
    {
        uint64_t hash = lendian(key);

        if (m_range_lower[0] > hash || hash >= m_range_upper[0])
        {
            return false;
        }
    }

    for (size_t i = 1; i < m_equality.size(); ++i)
    {
        if (m_equality_bits.get(i))
        {
            if (m_equality[i] != value[i - 1])
            {
                return false;
            }
        }
        else if (m_range_bits.get(i))
        {
            uint64_t hash = lendian(value[i - 1]);

            if (m_range_lower[i] > hash || hash >= m_range_upper[i])
            {
                return false;
            }
        }
    }

    return true;
}

size_t
hyperspacehashing :: search :: packed_size() const
{
    size_t sz = size() * (sizeof(uint8_t) * 2 + sizeof(uint64_t) * 4 + sizeof(uint32_t));

    for (size_t i = 0; i < size(); ++i)
    {
        sz += m_equality[i].size();
    }

    return sz;
}

void
hyperspacehashing :: search :: equality_set(size_t idx, const e::slice& val)
{
    assert(sanity_check());
    assert(idx < m_equality_bits.bits());
    assert(!m_equality_bits.get(idx));
    assert(!m_range_bits.get(idx));
    m_equality_bits.set(idx);
    m_equality[idx] = val;
}

void
hyperspacehashing :: search :: range_set(size_t idx, uint64_t start, uint64_t end)
{
    assert(sanity_check());
    assert(idx < m_equality_bits.bits());
    assert(!m_equality_bits.get(idx));
    assert(!m_range_bits.get(idx));
    m_range_bits.set(idx);
    m_range_lower[idx] = start;
    m_range_upper[idx] = end;
}

e::buffer::packer
hyperspacehashing :: operator << (e::buffer::packer lhs, const search& rhs)
{
    return lhs << rhs.m_equality_bits << rhs.m_equality
               << rhs.m_range_bits << rhs.m_range_lower << rhs.m_range_upper;
}

e::buffer::unpacker
hyperspacehashing :: operator >> (e::buffer::unpacker lhs, search& rhs)
{
    return lhs >> rhs.m_equality_bits >> rhs.m_equality
               >> rhs.m_range_bits >> rhs.m_range_lower >> rhs.m_range_upper;
}
