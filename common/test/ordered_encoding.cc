// Copyright (c) 2012-2013, Cornell University
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
#include <cmath>
#include <stdint.h>

// HyperDex
#include "th.h"
#include "common/ordered_encoding.h"

using hyperdex::ordered_encode_int64;
using hyperdex::ordered_decode_int64;
using hyperdex::ordered_encode_double;

TEST(OrderedEncoding, EncodeInt64)
{
    ASSERT_EQ(0xffffffffffffffffULL, ordered_encode_int64(INT64_MAX));
    ASSERT_EQ(0xfffffffffffffffeULL, ordered_encode_int64(INT64_MAX - 1));
    ASSERT_EQ(0x8000000000000001ULL, ordered_encode_int64(1));
    ASSERT_EQ(0x8000000000000000ULL, ordered_encode_int64(0));
    ASSERT_EQ(0x7fffffffffffffffULL, ordered_encode_int64(-1));
    ASSERT_EQ(0x0000000000000001ULL, ordered_encode_int64(INT64_MIN + 1));
    ASSERT_EQ(0x0000000000000000ULL, ordered_encode_int64(INT64_MIN));
}

TEST(OrderedEncoding, DecodeInt64)
{
    ASSERT_EQ(INT64_MAX,     ordered_decode_int64(0xffffffffffffffffULL));
    ASSERT_EQ(INT64_MAX - 1, ordered_decode_int64(0xfffffffffffffffeULL));
    ASSERT_EQ(1,             ordered_decode_int64(0x8000000000000001ULL));
    ASSERT_EQ(0,             ordered_decode_int64(0x8000000000000000ULL));
    ASSERT_EQ(-1,            ordered_decode_int64(0x7fffffffffffffffULL));
    ASSERT_EQ(INT64_MIN + 1, ordered_decode_int64(0x0000000000000001ULL));
    ASSERT_EQ(INT64_MIN,     ordered_decode_int64(0x0000000000000000ULL));
}

TEST(IndexEncode, Double)
{
    ASSERT_EQ(0x0000000000000000ULL, ordered_encode_double(-INFINITY));
    ASSERT_EQ(0xfff0000000000002ULL, ordered_encode_double(INFINITY));
    ASSERT_EQ(0xfff0000000000003ULL, ordered_encode_double(NAN));
    ASSERT_EQ(0x8000000000000001ULL, ordered_encode_double(0.));

    double old_d = -INFINITY;
    uint64_t old_e = 0x0000000000000000ULL;

    for (size_t i = 0; i < 1000000; ++i)
    {
        double d = drand48() * mrand48() * mrand48();
        uint64_t e = ordered_encode_double(d);

        if (isinf(d) && d < 0)
        {
            ASSERT_EQ(0x0000000000000000ULL, e);
        }
        else if (isinf(d) && d > 0)
        {
            ASSERT_EQ(0xfff0000000000002ULL, e);
        }
        else if (isnan(d))
        {
            ASSERT_EQ(0xfff0000000000003ULL, e);
        }
        else
        {
            ASSERT_TRUE(e > 0x0000000000000000ULL);
            ASSERT_TRUE(e < 0xfff0000000000002ULL);
            ASSERT_TRUE(e < 0xfff0000000000003ULL);

            if (d < 0)
            {
                ASSERT_TRUE(e < 0x8000000000000001ULL);
            }
            if (d > 0)
            {
                ASSERT_TRUE(e > 0x8000000000000001ULL);
            }

            if (old_d < d)
            {
                ASSERT_TRUE(old_e < e);
            }
            else if (old_d > d)
            {
                ASSERT_TRUE(old_e > e);
            }
            else
            {
                ASSERT_TRUE(old_e == e);
            }

            old_d = d;
            old_e = e;
        }
    }
}
