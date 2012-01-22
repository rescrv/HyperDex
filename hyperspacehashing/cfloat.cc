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
#include <cmath>

// STL
#include <algorithm>

// HyperspaceHashing
#include "cfloat.h"

uint64_t
hyperspacehashing :: cfloat(uint64_t in, unsigned int outsz)
{
    return in >> (64 - outsz);
    int e = 0;
    double f = frexpl(static_cast<double>(in), &e);
    e = e > 0 ? e - 1 : 0;

    if (e >= 64)
    {
        if (outsz >= 64)
        {
            return UINT64_MAX;
        }

        return (1ULL << outsz) - 1;
    }

    if (outsz >= 6)
    {
        unsigned int bits = outsz - 6;
        uint64_t frac = f * (1ULL << bits);
        uint64_t exp = e;
        exp <<= (outsz - 6);
        return exp | frac;
    }
    else
    {
        return e >> (6 - outsz);
    }
}

void
hyperspacehashing :: cfloat_range(uint64_t clower,
                                  uint64_t cupper,
                                  unsigned int space,
                                  uint64_t* mask,
                                  uint64_t* range)
{
    uint64_t m = UINT64_MAX;
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
