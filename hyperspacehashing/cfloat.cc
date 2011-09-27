
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
    if (sz == 1)
    {
        return dec > UINT16_MAX ? 1 : 0;
    }

    sz -= 2;
    unsigned int exp_sz = 0;
    unsigned int frac_sz = 0;
    unsigned int log2_of_input_sz = 0;
    uint64_t tag = 0;

    if (dec > UINT32_MAX)
    {
        log2_of_input_sz = 6U;
        tag = 3;
    }
    else if (dec > UINT16_MAX)
    {
        log2_of_input_sz = 5U;
        tag = 2;
    }
    else if (dec > UINT8_MAX)
    {
        log2_of_input_sz = 4U;
        tag = 1;
    }
    else
    {
        log2_of_input_sz = 3U;
        tag = 0;
    }

    exp_sz = std::min(log2_of_input_sz, sz / 2);
    frac_sz = sz - exp_sz;

    if (sz == 2)
    {
        return tag;
    }

    return tag << sz | cfloat(dec, log2_of_input_sz, exp_sz, frac_sz);
}

uint64_t
hyperspacehashing :: cfloat(uint64_t dec,
                            unsigned int log2_of_input_sz,
                            unsigned int exp_sz,
                            unsigned int frac_sz)
{
    assert(log2_of_input_sz <= 64);
    assert(log2_of_input_sz >= exp_sz);
    unsigned int input_sz = 1U << log2_of_input_sz;
    assert(input_sz == 64 || 1ULL << input_sz > dec);
    uint64_t enc = 0;
    uint64_t frac_range = UINT64_MAX;

    if (!dec)
    {
        return 0;
    }

    if (exp_sz > 0)
    {
        unsigned int exp_pos = 0;
        uint64_t exp_num = 1;

        // Shift exp_num until its set bit is in the same position as the
        // highest bit of dec.
        while (exp_pos + 1 < input_sz && (exp_num ^ dec) > exp_num)
        {
            ++exp_pos;
            exp_num <<= 1;
        }

        assert(exp_pos < input_sz);
        assert(exp_pos == 63 || (exp_num <= dec && exp_num * 2 > dec));

        // We find the looser-fitting lower and upper bounds.
        unsigned int incr = 1U << (log2_of_input_sz - exp_sz);
        exp_pos &= ~(incr - 1);
        exp_num = 1U << exp_pos;
        unsigned int next_pos = exp_pos + incr;

        if (next_pos >= 64)
        {
            frac_range = UINT64_MAX - exp_num + 1;
        }
        else
        {
            frac_range = (1ULL << next_pos) - exp_num;
        }

        dec -= exp_num;
        enc = exp_pos >> (log2_of_input_sz - exp_sz);
        enc <<= frac_sz;
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
        uint64_t frac = frac_bound;
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
