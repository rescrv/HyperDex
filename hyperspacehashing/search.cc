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
#include <hyperspacehashing/search.h>

hyperspacehashing :: search :: search(size_t n)
    : m_equality_bits(n)
    , m_equality(n)
{
}

hyperspacehashing :: search :: ~search() throw ()
{
}

size_t
hyperspacehashing :: search :: size() const
{
    assert(m_equality_bits.bits() == m_equality.size());
    return m_equality.size();
}

bool
hyperspacehashing :: search :: is_equality(size_t idx) const
{
    assert(idx < m_equality_bits.bits());
    assert(m_equality_bits.bits() == m_equality.size());
    return m_equality_bits.get(idx);
}

const e::buffer&
hyperspacehashing :: search :: equality_value(size_t idx) const
{
    assert(idx < m_equality_bits.bits());
    assert(m_equality_bits.bits() == m_equality.size());
    assert(m_equality_bits.get(idx));
    return m_equality[idx];
}

void
hyperspacehashing :: search :: equality_set(size_t idx, const e::buffer& val)
{
    assert(idx < m_equality_bits.bits());
    assert(m_equality_bits.bits() == m_equality.size());
    assert(!m_equality_bits.get(idx));
    m_equality_bits.set(idx);
    m_equality[idx] = val;
}

e::packer&
hyperspacehashing :: operator << (e::packer& lhs, const search& rhs)
{
    lhs << rhs.m_equality_bits << rhs.m_equality;
    return lhs;
}

e::unpacker&
hyperspacehashing :: operator >> (e::unpacker& lhs, search& rhs)
{
    lhs >> rhs.m_equality_bits >> rhs.m_equality;
    return lhs;
}
