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

#ifndef bithacks_h_
#define bithacks_h_

// C
#include <cstdlib>
#include <stdint.h>

const uint64_t
lookup_msb_mask[] = {0ULL, 9223372036854775808ULL, 13835058055282163712ULL,
    16140901064495857664ULL, 17293822569102704640ULL, 17870283321406128128ULL,
    18158513697557839872ULL, 18302628885633695744ULL, 18374686479671623680ULL,
    18410715276690587648ULL, 18428729675200069632ULL, 18437736874454810624ULL,
    18442240474082181120ULL, 18444492273895866368ULL, 18445618173802708992ULL,
    18446181123756130304ULL, 18446462598732840960ULL, 18446603336221196288ULL,
    18446673704965373952ULL, 18446708889337462784ULL, 18446726481523507200ULL,
    18446735277616529408ULL, 18446739675663040512ULL, 18446741874686296064ULL,
    18446742974197923840ULL, 18446743523953737728ULL, 18446743798831644672ULL,
    18446743936270598144ULL, 18446744004990074880ULL, 18446744039349813248ULL,
    18446744056529682432ULL, 18446744065119617024ULL, 18446744069414584320ULL,
    18446744071562067968ULL, 18446744072635809792ULL, 18446744073172680704ULL,
    18446744073441116160ULL, 18446744073575333888ULL, 18446744073642442752ULL,
    18446744073675997184ULL, 18446744073692774400ULL, 18446744073701163008ULL,
    18446744073705357312ULL, 18446744073707454464ULL, 18446744073708503040ULL,
    18446744073709027328ULL, 18446744073709289472ULL, 18446744073709420544ULL,
    18446744073709486080ULL, 18446744073709518848ULL, 18446744073709535232ULL,
    18446744073709543424ULL, 18446744073709547520ULL, 18446744073709549568ULL,
    18446744073709550592ULL, 18446744073709551104ULL, 18446744073709551360ULL,
    18446744073709551488ULL, 18446744073709551552ULL, 18446744073709551584ULL,
    18446744073709551600ULL, 18446744073709551608ULL, 18446744073709551612ULL,
    18446744073709551614ULL, 18446744073709551615ULL};

inline uint64_t
lower_interlace(uint64_t* nums, size_t sz)
{
    uint64_t ret = 0;

    if (!sz)
    {
        return 0;
    }

    for (int i = 0; i < 64; ++i)
    {
        size_t quotient = i / sz;
        size_t modulus = i % sz;
        uint64_t bit = 1;
        bit <<= quotient;
        uint64_t hash = nums[modulus];
        ret |= (bit & hash) << (i - quotient);
    }

    return ret;
}

inline uint64_t
upper_interlace(uint64_t* nums, size_t sz)
{
    uint64_t ret = 0;

    if (!sz)
    {
        return 0;
    }

    for (int i = 0; i < 64; ++i)
    {
        size_t quotient = i / sz;
        size_t modulus = i % sz;
        uint64_t bit = 1;
        bit <<= 63 - quotient;
        uint64_t hash = nums[modulus];
        ret |= (bit & hash) >> (i - quotient);
    }

    return ret;
}

inline void
double_lower_interlace(uint64_t* nums, size_t sz, uint64_t* lower, uint64_t* upper)
{
    *lower = 0;
    *upper = 0;

    if (!sz)
    {
        return;
    }

    uint64_t result[2] = {0, 0};

    for (int i = 0; i < 128; ++i)
    {
        size_t quotient = i / sz;
        size_t modulus = i % sz;

        if (quotient >= 64)
        {
            break;
        }

        uint64_t bit = 1;
        bit <<= quotient;
        uint64_t hash = nums[modulus];

        if (i >= 64)
        {
            if (quotient > static_cast<size_t>(i - 64))
            {
                result[1] |= (bit & hash) >> (quotient - (i - 64));
            }
            else
            {
                result[1] |= (bit & hash) << ((i - 64) - quotient);
            }
        }
        else
        {
            result[0] |= (bit & hash) << (i - quotient);
        }
    }

    *lower = result[0];
    *upper = result[1];
}

#endif // bithacks_h_
