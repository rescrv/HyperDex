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
#include "coordinate.h"

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

TEST(CoordinateTest, BlankCoordinate)
{
    hyperdisk::coordinate c;
    EXPECT_EQ(0, c.primary_mask);
    EXPECT_EQ(0, c.primary_hash);
    EXPECT_EQ(0, c.secondary_mask);
    EXPECT_EQ(0, c.secondary_hash);
}

TEST(CoordinateTest, FullConstructor)
{
    hyperdisk::coordinate c(0xdeadbeef, 0xcafebabe, 0x1eaff00d, 0x8badf00d);
    EXPECT_EQ(0xdeadbeef, c.primary_mask);
    EXPECT_EQ(0xcafebabe, c.primary_hash);
    EXPECT_EQ(0x1eaff00d, c.secondary_mask);
    EXPECT_EQ(0x8badf00d, c.secondary_hash);
}

TEST(CoordinateTest, CopyConstructor)
{
    hyperdisk::coordinate c(0xdeadbeef, 0xcafebabe, 0x1eaff00d, 0x8badf00d);
    hyperdisk::coordinate d(c);
    EXPECT_EQ(0xdeadbeef, d.primary_mask);
    EXPECT_EQ(0xcafebabe, d.primary_hash);
    EXPECT_EQ(0x1eaff00d, d.secondary_mask);
    EXPECT_EQ(0x8badf00d, d.secondary_hash);
}

TEST(CoordinateTest, PrimaryContains)
{
    using hyperdisk::coordinate;
    // Simply put, the null coordinate contains the null coordinate.
    EXPECT_TRUE(coordinate(0, 0, 0, 0).primary_contains(coordinate(0, 0, 0, 0)));

    // A more restrictive mask will not contain a less-restrictive mask.
    EXPECT_FALSE(coordinate(UINT32_MAX, 0, 0, 0).primary_contains(coordinate(0, 0, 0, 0)));

    // When the masks overlap, the hashes make the difference.
    EXPECT_TRUE(coordinate(1, 0, 0, 0).primary_contains(coordinate(1, 0, 0, 0)));
    EXPECT_FALSE(coordinate(1, 0, 0, 0).primary_contains(coordinate(1, 1, 0, 0)));
    EXPECT_TRUE(coordinate(1, 1, 0, 0).primary_contains(coordinate(1, 1, 0, 0)));
    EXPECT_TRUE(coordinate(1, 0, 0, 0).primary_contains(coordinate(3, 0, 0, 0)));
    EXPECT_FALSE(coordinate(1, 0, 0, 0).primary_contains(coordinate(3, 1, 0, 0)));
    EXPECT_TRUE(coordinate(1, 1, 0, 0).primary_contains(coordinate(3, 1, 0, 0)));
    EXPECT_TRUE(coordinate(1, 1, 0, 0).primary_contains(coordinate(3, 3, 0, 0)));
    EXPECT_FALSE(coordinate(1, 1, 0, 0).primary_contains(coordinate(3, 2, 0, 0)));

    // When the masks do not overlap, the hashes do not matter.
    EXPECT_FALSE(coordinate(1, 3, 0, 0).primary_contains(coordinate(2, 3, 0, 0)));
    EXPECT_FALSE(coordinate(1, 1, 0, 0).primary_contains(coordinate(2, 2, 0, 0)));
}

TEST(CoordinateTest, SecondaryContains)
{
    using hyperdisk::coordinate;
    // Simply put, the null coordinate contains the null coordinate.
    EXPECT_TRUE(coordinate(0, 0, 0, 0).secondary_contains(coordinate(0, 0, 0, 0)));
    // A more restrictive mask will not contain a less-restrictive mask.
    EXPECT_FALSE(coordinate(0, 0, UINT32_MAX, 0).secondary_contains(coordinate(0, 0, 0, 0)));

    // When the masks overlap, the hashes make the difference.
    EXPECT_TRUE(coordinate(0, 0, 1, 0).secondary_contains(coordinate(0, 0, 1, 0)));
    EXPECT_FALSE(coordinate(0, 0, 1, 0).secondary_contains(coordinate(0, 0, 1, 1)));
    EXPECT_TRUE(coordinate(0, 0, 1, 1).secondary_contains(coordinate(0, 0, 1, 1)));
    EXPECT_TRUE(coordinate(0, 0, 1, 0).secondary_contains(coordinate(0, 0, 3, 0)));
    EXPECT_FALSE(coordinate(0, 0, 1, 0).secondary_contains(coordinate(0, 0, 3, 1)));
    EXPECT_TRUE(coordinate(0, 0, 1, 1).secondary_contains(coordinate(0, 0, 3, 1)));
    EXPECT_TRUE(coordinate(0, 0, 1, 1).secondary_contains(coordinate(0, 0, 3, 3)));
    EXPECT_FALSE(coordinate(0, 0, 1, 1).secondary_contains(coordinate(0, 0, 3, 2)));

    // When the masks do not overlap, the hashes do not matter.
    EXPECT_FALSE(coordinate(0, 0, 1, 3).secondary_contains(coordinate(0, 0, 2, 3)));
    EXPECT_FALSE(coordinate(0, 0, 1, 1).secondary_contains(coordinate(0, 0, 2, 2)));
}

TEST(CoordinateTest, Contains)
{
    using hyperdisk::coordinate;
    EXPECT_TRUE(coordinate(0, 0, 0, 0).contains(coordinate(0, 0, 0, 0)));
    EXPECT_FALSE(coordinate(0, 0, UINT32_MAX, 0).contains(coordinate(0, 0, 0, 0)));
    EXPECT_FALSE(coordinate(UINT32_MAX, 0, 0, 0).contains(coordinate(0, 0, 0, 0)));
    EXPECT_FALSE(coordinate(UINT32_MAX, 0, UINT32_MAX, 0).contains(coordinate(0, 0, 0, 0)));
}

} // namespace
