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

// HyperDisk
#include "log_entry.h"

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

TEST(LogEntry, BlankEntry)
{
    hyperdisk::log_entry le;
    EXPECT_EQ(0, le.coord.primary_mask);
    EXPECT_EQ(0, le.coord.primary_hash);
    EXPECT_EQ(0, le.coord.secondary_lower_mask);
    EXPECT_EQ(0, le.coord.secondary_lower_mask);
    EXPECT_EQ(0, le.coord.secondary_upper_hash);
    EXPECT_EQ(0, le.coord.secondary_upper_hash);
    EXPECT_TRUE(e::buffer() == le.key);
    EXPECT_TRUE(std::vector<e::buffer>() == le.value);
    EXPECT_EQ(0, le.version);
}

TEST(LogEntry, PutEntry)
{
    hyperdisk::log_entry le(hyperspacehashing::mask::coordinate(0, 1, 2, 3, 4, 5),
                            e::buffer("key", 3),
                            std::vector<e::buffer>(1, e::buffer("value", 5)),
                            8);
    EXPECT_EQ(0, le.coord.primary_mask);
    EXPECT_EQ(1, le.coord.primary_hash);
    EXPECT_EQ(2, le.coord.secondary_lower_mask);
    EXPECT_EQ(3, le.coord.secondary_lower_hash);
    EXPECT_EQ(4, le.coord.secondary_upper_mask);
    EXPECT_EQ(5, le.coord.secondary_upper_hash);
    EXPECT_TRUE(e::buffer("key", 3) == le.key);
    EXPECT_TRUE(std::vector<e::buffer>(1, e::buffer("value", 5)) == le.value);
    EXPECT_EQ(8, le.version);
}

TEST(LogEntry, DelEntry)
{
    hyperdisk::log_entry le(hyperspacehashing::mask::coordinate(0, 1, 2, 3, 4, 5),
                            e::buffer("key", 3));
    EXPECT_EQ(0, le.coord.primary_mask);
    EXPECT_EQ(1, le.coord.primary_hash);
    EXPECT_EQ(2, le.coord.secondary_lower_mask);
    EXPECT_EQ(3, le.coord.secondary_lower_hash);
    EXPECT_EQ(4, le.coord.secondary_upper_mask);
    EXPECT_EQ(5, le.coord.secondary_upper_hash);
    EXPECT_TRUE(e::buffer("key", 3) == le.key);
    EXPECT_TRUE(std::vector<e::buffer>() == le.value);
    EXPECT_EQ(0, le.version);
}

} // namespace
