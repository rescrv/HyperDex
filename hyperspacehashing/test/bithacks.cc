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

// Google Test
#include <gtest/gtest.h>

// HyperspaceHashing
#include "bithacks.h"

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

TEST(BithacksTest, LowerInterlace)
{
    std::vector<uint64_t> nums;
    ASSERT_EQ(0, lower_interlace(nums));
    nums.push_back(UINT64_MAX);
    ASSERT_EQ(UINT64_MAX, lower_interlace(nums));
    nums.push_back(0);
    ASSERT_EQ(0x5555555555555555ULL, lower_interlace(nums));

    nums.clear();
    ASSERT_EQ(0, lower_interlace(nums));
    nums.push_back(0xdeadbeefcafebabeULL);
    ASSERT_EQ(0xdeadbeefcafebabeULL, lower_interlace(nums));
    nums.push_back(0x1eaff00ddefec8edULL);
    ASSERT_EQ(0xf2ecfffce5c4edf6ULL, lower_interlace(nums));
    nums.push_back(0);
    ASSERT_EQ(0xb6d86896086996caULL, lower_interlace(nums));
}

TEST(BithacksTest, UpperInterlace)
{
    std::vector<uint64_t> nums;
    ASSERT_EQ(0, upper_interlace(nums));
    nums.push_back(UINT64_MAX);
    ASSERT_EQ(UINT64_MAX, upper_interlace(nums));
    nums.push_back(0);
    ASSERT_EQ(0xaaaaaaaaaaaaaaaaULL, upper_interlace(nums));

    nums.clear();
    ASSERT_EQ(0, upper_interlace(nums));
    nums.push_back(0xdeadbeefcafebabeULL);
    ASSERT_EQ(0xdeadbeefcafebabeULL, upper_interlace(nums));
    nums.push_back(0x1eaff00ddefec8edULL);
    ASSERT_EQ(0xa3fcccf7dfa8a8fbULL, upper_interlace(nums));
    nums.push_back(0);
    ASSERT_EQ(0x906db0c30d96cb69ULL, upper_interlace(nums));
}

} // namespace
