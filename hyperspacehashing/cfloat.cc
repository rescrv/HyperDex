
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

// C
#include <cassert>

// STL
#include <algorithm>

// HyperspaceHashing
#include "cfloat.h"

uint64_t
hyperspacehashing :: cfloat(uint64_t dec, unsigned int sz)
{
    unsigned int exp_sz = std::min(static_cast<unsigned int>(6), sz / 4);
    unsigned int frac_sz = sz - exp_sz;
    return cfloat(dec, exp_sz, frac_sz);
}

uint64_t
hyperspacehashing :: cfloat(uint64_t dec,
                            unsigned int exp_sz,
                            unsigned int frac_sz)
{
    if (exp_sz > 6)
    {
        frac_sz += exp_sz - 6;
        exp_sz = 6;
    }

    uint64_t enc = 0;
    uint64_t frac_range = UINT64_MAX;

    if (exp_sz > 0)
    {
        uint64_t pos = 0;
        uint64_t lower = 0;
        uint64_t upper = 1;
        unsigned int exp_bound = 1 << exp_sz;
        unsigned int exp_shift = 6 - exp_sz;

        for (pos = 0; pos < exp_bound; ++pos)
        {
            upper = 1ULL;
            upper <<= (pos << exp_shift);

            if (upper > dec)
            {
                break;
            }

            lower = upper;
        }

        if (upper == lower)
        {
            frac_range = (UINT64_MAX - lower) + 1;
            --pos;
        }
        else
        {
            frac_range = upper - lower;
            pos = pos > 0 ? pos - 1 : 0;
        }

        dec -= lower;
        enc |= pos << frac_sz;
    }

    if (frac_sz > 0)
    {
        double frac_used = static_cast<double>(dec)
                         / static_cast<double>(frac_range);
        assert(frac_used >= 0);
        assert(frac_used <= 1);

        uint64_t frac_bound = 1;
        frac_bound <<= frac_sz;
        assert(frac_bound > 0);
        uint64_t frac = frac_bound - 1;
        assert(frac <= frac_bound - 1);
        frac *= frac_used;
        frac = frac >= frac_bound ? frac_bound - 1 : frac;
        assert(frac <= frac_bound - 1);
        enc |= frac;
    }

    return enc;
}

void
hyperspacehashing :: cfloat_range(uint64_t clower,
                                  uint64_t cupper,
                                  unsigned int space,
                                  uint64_t* mask,
                                  uint64_t* range)
{
    uint64_t m = UINT64_MAX;
    m <<= (64 - space);
    m >>= (64 - space);
    m = (~(clower ^ cupper)) & m;
    bool seen_one = false;
    bool seen_zero = false;

    for (ssize_t b = 63; b >= 0; --b)
    {
        uint64_t bit = 1;
        bit <<= b;

        if ((m & bit))
        {
            seen_one = true;
        }

        if (seen_one && !(m & bit))
        {
            seen_zero = true;
        }

        if (seen_zero)
        {
            m &= ~bit;
        }
    }

    *mask = m;
    *range = clower & m;
}
