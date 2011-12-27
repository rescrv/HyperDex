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
#include "range_match.h"

// There are three ways in which comparisons can be made to check if a
// coordinate is within a range.
//
// Simple comparison:
//      If the mask specifies the full coordinate and we know that no
//      interlacing took place, we can just compare the hash against the
//      cfloat-mapped lower and upper bounds.
//
// Interlace-tolerant comparison:
//      If the mask specifies the full coordinate, and interlacing took
//      place, we need to first pull out which bits are from the
//      cfloat-mapped number.  For this form, the lower and upper bounds
//      are pre-interlaced so that the number extracted from the full
//      coordinate's hash is directly comparable to m_clower and
//      m_cupper.
//
// Partial-tolerant comparison:
//      If the mask does not specify a full coordinate, ordering-based
//      comparisons do not work.  In this case, the best we can do is
//      compare bits that are the same in the cloat-mapped lower and
//      upper ranges to the hash.  This case is folded into the equality
//      comparison and thus is not handled by range_match.

hyperspacehashing :: range_match :: range_match(unsigned int idx,
                                                uint64_t lower, uint64_t upper,
                                                uint64_t cmask,
                                                uint64_t clower, uint64_t cupper)
    : m_idx(idx)
    , m_lower(lower)
    , m_upper(upper)
    , m_cmask(cmask)
    , m_clower(clower)
    , m_cupper(cupper)
{
}

bool
hyperspacehashing :: range_match :: matches(const prefix::coordinate& coord) const
{
    uint64_t prefix = lookup_msb_mask[coord.prefix];

    // Interlace-tolerant comparison
    if ((prefix & m_cmask) == m_cmask)
    {
        uint64_t hash = coord.point & m_cmask;
        return (m_clower <= hash) && (hash <= m_cupper);
    }
    // Partial-tolerant comparison handled in equality check.
    else
    {
        return true;
    }
}

bool
hyperspacehashing :: range_match :: matches(const e::slice& key,
                                            const std::vector<e::slice>& value) const
{
    uint64_t hash;

    // If we are dealing with the key
    if (m_idx == 0)
    {
        hash = lendian(key);
    }
    else
    {
        hash = lendian(value[m_idx - 1]);
    }

    return (m_lower <= hash) && (hash < m_upper);
}
