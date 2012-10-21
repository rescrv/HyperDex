// Copyright (c) 2012, Cornell University
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

// Google Test
#include <gtest/gtest.h>

// HyperDex
#include "daemon/index_encode.h"

#pragma GCC diagnostic ignored "-Wswitch-default"

using hyperdex::index_encode_int64;
using hyperdex::index_encode_double;

namespace
{

TEST(IndexEncode, Int64)
{
    char buf[8];
    index_encode_int64(INT64_MAX, buf);
    ASSERT_TRUE(memcmp("\xff\xff\xff\xff\xff\xff\xff\xff", buf, 8) == 0);
    index_encode_int64(INT64_MAX - 1, buf);
    ASSERT_TRUE(memcmp("\xff\xff\xff\xff\xff\xff\xff\xfe", buf, 8) == 0);
    index_encode_int64(1, buf);
    ASSERT_TRUE(memcmp("\x80\x00\x00\x00\x00\x00\x00\x01", buf, 8) == 0);
    index_encode_int64(0, buf);
    ASSERT_TRUE(memcmp("\x80\x00\x00\x00\x00\x00\x00\x00", buf, 8) == 0);
    index_encode_int64(-1, buf);
    ASSERT_TRUE(memcmp("\x7f\xff\xff\xff\xff\xff\xff\xff", buf, 8) == 0);
    index_encode_int64(INT64_MIN + 1, buf);
    ASSERT_TRUE(memcmp("\x00\x00\x00\x00\x00\x00\x00\x01", buf, 8) == 0);
    index_encode_int64(INT64_MIN, buf);
    ASSERT_TRUE(memcmp("\x00\x00\x00\x00\x00\x00\x00\x00", buf, 8) == 0);
}

TEST(IndexEncode, Double)
{
    char ninf_buf[8];
    index_encode_double(-INFINITY, ninf_buf);
    ASSERT_TRUE(memcmp("\x00\x00\x00\x00\x00\x00\x00\x00", ninf_buf, 8) == 0);
    char pinf_buf[8];
    index_encode_double(INFINITY, pinf_buf);
    ASSERT_TRUE(memcmp("\xff\xf0\x00\x00\x00\x00\x00\x02", pinf_buf, 8) == 0);
    char nan_buf[8];
    index_encode_double(NAN, nan_buf);
    ASSERT_TRUE(memcmp("\xff\xf0\x00\x00\x00\x00\x00\x03", nan_buf, 8) == 0);
    char zero_buf[8];
    index_encode_double(0, zero_buf);
    ASSERT_TRUE(memcmp("\x80\x00\x00\x00\x00\x00\x00\x01", zero_buf, 8) == 0);

    char old_buf[8];
    memset(old_buf, 0, 8);
    double old = -INFINITY;

    for (size_t i = 0; i < 1000000; ++i)
    {
        char buf[8];
        double d = drand48() * mrand48() * mrand48();
        index_encode_double(d, buf);

        if (isinf(d) && d < 0)
        {
            ASSERT_TRUE(memcmp(buf, ninf_buf, 8) == 0);
        }
        else if (isinf(d) && d > 0)
        {
            ASSERT_TRUE(memcmp(buf, pinf_buf, 8) == 0);
        }
        else if (isnan(d))
        {
            ASSERT_TRUE(memcmp(buf, nan_buf, 8) == 0);
        }
        else if (d == 0)
        {
            ASSERT_TRUE(memcmp(buf, zero_buf, 8) == 0);
        }
        else
        {
            if (d < 0)
            {
                ASSERT_TRUE(memcmp(buf, ninf_buf, 8) > 0);
                ASSERT_TRUE(memcmp(buf, pinf_buf, 8) < 0);
                ASSERT_TRUE(memcmp(buf, nan_buf, 8) < 0);
                ASSERT_TRUE(memcmp(buf, zero_buf, 8) < 0);
            }
            else if (d > 0)
            {
                ASSERT_TRUE(memcmp(buf, ninf_buf, 8) > 0);
                ASSERT_TRUE(memcmp(buf, pinf_buf, 8) < 0);
                ASSERT_TRUE(memcmp(buf, nan_buf, 8) < 0);

                ASSERT_TRUE(memcmp(buf, zero_buf, 8) > 0);
            }

            if (old < d)
            {
                if (memcmp(old_buf, buf, 8) > 0)
                {
                    std::cout << "cmp " << old << " " << d << std::endl;
                    for (size_t i = 0; i < 8; ++i) std::cout << (unsigned)old_buf[i] << " " << (unsigned)buf[i] << std::endl;
                    std::cout << "mem " << memcmp(old_buf, buf, 8) << std::endl;
                }

                ASSERT_LT(memcmp(old_buf, buf, 8), 0);
            }
            else if (old > d)
            {
                ASSERT_TRUE(memcmp(old_buf, buf, 8) > 0);
            }
            else
            {
                ASSERT_TRUE(memcmp(old_buf, buf, 8) == 0);
            }

            memmove(old_buf, buf, 8);
            old = d;
        }
    }
}

} // namespace
