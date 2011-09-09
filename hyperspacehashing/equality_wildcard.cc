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
#include "hyperspacehashing/equality_wildcard.h"

hyperspacehashing :: equality_wildcard :: equality_wildcard(size_t n)
    : m_mask(n)
    , m_value(n)
{
}

hyperspacehashing :: equality_wildcard :: ~equality_wildcard() throw ()
{
}

void
hyperspacehashing :: equality_wildcard :: set(size_t idx, const e::buffer& val)
{
    assert(idx < m_value.size());
    assert(idx < m_mask.bits());

    m_mask.set(idx);
    m_value[idx] = val;
}

void
hyperspacehashing :: equality_wildcard :: unset(size_t idx)
{
    assert(idx < m_value.size());
    assert(idx < m_mask.bits());

    m_mask.unset(idx);
}

void
hyperspacehashing :: equality_wildcard :: clear()
{
    m_value = std::vector<e::buffer>(m_value.size());
    m_mask = e::bitfield(m_value.size());
}
