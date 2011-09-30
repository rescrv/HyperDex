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

#include <iostream>

// STL
#include <algorithm>

// HyperspaceHashing
#include "copint.h"

static unsigned int
constant(unsigned int insz, unsigned int outsz, unsigned int num)
{
    assert(insz <= 64);
    assert(insz >= outsz);
    assert(num <= 2);
    unsigned int nums[3];

    switch (insz)
    {
        case 64:
            nums[0] = 32;
            nums[1] = 48;
            nums[2] = 56;
            break;
        case 56:
            nums[0] = 24;
            nums[1] = 32;
            nums[2] = 48;
            break;
        case 48:
            nums[0] = 24;
            nums[1] = 32;
            nums[2] = 40;
            break;
        case 40:
            nums[0] = 16;
            nums[1] = 24;
            nums[2] = 32;
            break;
        case 32:
            nums[0] = 12;
            nums[1] = 16;
            nums[2] = 24;
            break;
        case 24:
            nums[0] = 10;
            nums[1] = 12;
            nums[2] = 16;
            break;
        case 16:
            nums[0] = 7;
            nums[1] = 9;
            nums[2] = 10;
            break;
        case 12:
            nums[0] = 8;
            nums[1] = 10;
            nums[2] = 11;
            break;
        case 11:
            nums[0] = 8;
            nums[1] = 9;
            nums[2] = 10;
            break;
        case 10:
            nums[0] = 7;
            nums[1] = 8;
            nums[2] = 9;
            break;
        case 9:
            nums[0] = 5;
            nums[1] = 6;
            nums[2] = 7;
            break;
        case 8:
            nums[0] = 4;
            nums[1] = 5;
            nums[2] = 6;
            break;
        case 7:
            nums[0] = 4;
            nums[1] = 5;
            nums[2] = 6;
            break;
        case 6:
            nums[0] = 2;
            nums[1] = 3;
            nums[2] = 4;
            break;
        case 5:
            nums[0] = 2;
            nums[1] = 3;
            nums[2] = 4;
            break;
        case 4:
            nums[0] = 1;
            nums[1] = 2;
            nums[2] = 2;
            break;
        case 3:
            nums[0] = 0;
            nums[1] = 1;
            nums[2] = 2;
            break;
        case 2:
            nums[0] = 0;
            nums[1] = 1;
            nums[2] = 2;
            break;
        case 1:
            nums[0] = 0;
            nums[1] = 1;
            nums[2] = 1;
            break;
        default:
            std::cout << "ABORT " << insz;
            abort();
    }

    return nums[num];
}

uint64_t
hyperspacehashing :: copint(uint64_t in, unsigned int outsz)
{
    //std::cout << "=======" << std::endl;
    return copint(in, 64, outsz);
}

uint64_t
hyperspacehashing :: copint(uint64_t in, unsigned int insz, unsigned int outsz)
{
    assert(insz >= outsz);
    assert(insz == 63 || in < (1 << insz));
    uint64_t next = in / (insz - outsz + 2);
    uint64_t tag = 0;
    unsigned int sz = 0;

    if (in >= (1ULL << constant(insz, outsz, 2)))
    {
        tag = 0x3;
        sz = insz;
    }
    else if (in >= (1ULL << constant(insz, outsz, 1)))
    {
        tag = 0x2;
        sz = constant(insz, outsz, 2);
    }
    else if (in >= (1ULL << constant(insz, outsz, 0)))
    {
        tag = 0x1;
        sz = constant(insz, outsz, 1);
    }
    else
    {
        tag = 0x0;
        sz = constant(insz, outsz, 0);
    }

    //std::cout << insz << ": " << in << " " << insz << " " << outsz << " " << tag << " " << sz << std::endl;

    if (outsz == 0)
    {
        return 0;
    }
    else if (outsz == 1)
    {
        return tag >> 1;
    }

    tag <<= outsz - 2;

    if (outsz - 2 >= sz)
    {
        return tag | in;
    }
    else
    {
        return tag | copint(next, sz, outsz - 2);
    }
}

void
hyperspacehashing :: copint_range(uint64_t clower,
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
