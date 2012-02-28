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

// Google Test
#include <gtest/gtest.h>

// HyperDex
#include <hyperdex/ids.h>

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

TEST(IdsTest, SpaceidComparison)
{
    using hyperdex::spaceid;

    EXPECT_EQ(spaceid(0), spaceid(0));
    EXPECT_NE(spaceid(0), spaceid(4294967295));
    EXPECT_NE(spaceid(4294967295), spaceid(0));
    EXPECT_LT(spaceid(0), spaceid(4294967295));
    EXPECT_LE(spaceid(0), spaceid(4294967295));
    EXPECT_LE(spaceid(4294967295), spaceid(4294967295));
    EXPECT_GE(spaceid(4294967295), spaceid(4294967295));
    EXPECT_GE(spaceid(4294967295), spaceid(0));
    EXPECT_GT(spaceid(4294967295), spaceid(0));
}

TEST(IdsTest, SubspaceidComparison)
{
    using hyperdex::spaceid;
    using hyperdex::subspaceid;

    // All the previous tests still pass if we just add a 0.
    EXPECT_EQ(subspaceid(spaceid(0), 0), subspaceid(spaceid(0), 0));
    EXPECT_NE(subspaceid(spaceid(0), 0), subspaceid(spaceid(4294967295), 0));
    EXPECT_NE(subspaceid(spaceid(4294967295), 0), subspaceid(spaceid(0), 0));
    EXPECT_LT(subspaceid(spaceid(0), 0), subspaceid(spaceid(4294967295), 0));
    EXPECT_LE(subspaceid(spaceid(0), 0), subspaceid(spaceid(4294967295), 0));
    EXPECT_LE(subspaceid(spaceid(4294967295), 0), subspaceid(spaceid(4294967295), 0));
    EXPECT_GE(subspaceid(spaceid(4294967295), 0), subspaceid(spaceid(4294967295), 0));
    EXPECT_GE(subspaceid(spaceid(4294967295), 0), subspaceid(spaceid(0), 0));
    EXPECT_GT(subspaceid(spaceid(4294967295), 0), subspaceid(spaceid(0), 0));

    // We get the comparisons we want if the space is the same.
    spaceid s(5);
    EXPECT_EQ(subspaceid(s, 0), subspaceid(s, 0));
    EXPECT_NE(subspaceid(s, 0), subspaceid(s, 65535));
    EXPECT_NE(subspaceid(s, 65535), subspaceid(s, 0));
    EXPECT_LT(subspaceid(s, 0), subspaceid(s, 65535));
    EXPECT_LE(subspaceid(s, 0), subspaceid(s, 65535));
    EXPECT_LE(subspaceid(s, 65535), subspaceid(s, 65535));
    EXPECT_GE(subspaceid(s, 65535), subspaceid(s, 65535));
    EXPECT_GE(subspaceid(s, 65535), subspaceid(s, 0));
    EXPECT_GT(subspaceid(s, 65535), subspaceid(s, 0));
}

TEST(IdsTest, RegionidComparison)
{
    using hyperdex::spaceid;
    using hyperdex::subspaceid;
    using hyperdex::regionid;

    // All the previous tests still pass if we just add a pair of 0s.
    EXPECT_EQ(regionid(subspaceid(spaceid(0), 0), 0, 0), regionid(subspaceid(spaceid(0), 0), 0, 0));
    EXPECT_NE(regionid(subspaceid(spaceid(0), 0), 0, 0), regionid(subspaceid(spaceid(4294967295), 0), 0, 0));
    EXPECT_NE(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), regionid(subspaceid(spaceid(0), 0), 0, 0));
    EXPECT_LT(regionid(subspaceid(spaceid(0), 0), 0, 0), regionid(subspaceid(spaceid(4294967295), 0), 0, 0));
    EXPECT_LE(regionid(subspaceid(spaceid(0), 0), 0, 0), regionid(subspaceid(spaceid(4294967295), 0), 0, 0));
    EXPECT_LE(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), regionid(subspaceid(spaceid(4294967295), 0), 0, 0));
    EXPECT_GE(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), regionid(subspaceid(spaceid(4294967295), 0), 0, 0));
    EXPECT_GE(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), regionid(subspaceid(spaceid(0), 0), 0, 0));
    EXPECT_GT(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), regionid(subspaceid(spaceid(0), 0), 0, 0));
    spaceid s(5);
    EXPECT_EQ(regionid(subspaceid(s, 0), 0, 0), regionid(subspaceid(s, 0), 0, 0));
    EXPECT_NE(regionid(subspaceid(s, 0), 0, 0), regionid(subspaceid(s, 65535), 0, 0));
    EXPECT_NE(regionid(subspaceid(s, 65535), 0, 0), regionid(subspaceid(s, 0), 0, 0));
    EXPECT_LT(regionid(subspaceid(s, 0), 0, 0), regionid(subspaceid(s, 65535), 0, 0));
    EXPECT_LE(regionid(subspaceid(s, 0), 0, 0), regionid(subspaceid(s, 65535), 0, 0));
    EXPECT_LE(regionid(subspaceid(s, 65535), 0, 0), regionid(subspaceid(s, 65535), 0, 0));
    EXPECT_GE(regionid(subspaceid(s, 65535), 0, 0), regionid(subspaceid(s, 65535), 0, 0));
    EXPECT_GE(regionid(subspaceid(s, 65535), 0, 0), regionid(subspaceid(s, 0), 0, 0));
    EXPECT_GT(regionid(subspaceid(s, 65535), 0, 0), regionid(subspaceid(s, 0), 0, 0));

    // We get the comparisons we want if the subspace is the same.
    // I use a smaller mask in this section because 2**64 - 1 is distractingly
    // large.
    subspaceid ss(s, 10);
    EXPECT_EQ(regionid(ss, 0, 0), regionid(ss, 0, 0));
    EXPECT_EQ(regionid(ss, 255, 0), regionid(ss, 255, 0));
    EXPECT_EQ(regionid(ss, 0, 65535), regionid(ss, 0, 65535));
    EXPECT_EQ(regionid(ss, 255, 65535), regionid(ss, 255, 65535));
    EXPECT_NE(regionid(ss, 0, 0), regionid(ss, 255, 0));
    EXPECT_NE(regionid(ss, 0, 0), regionid(ss, 0, 65535));
    EXPECT_NE(regionid(ss, 0, 0), regionid(ss, 255, 65535));
    EXPECT_NE(regionid(ss, 255, 0), regionid(ss, 0, 0));
    EXPECT_NE(regionid(ss, 0, 65535), regionid(ss, 0, 0));
    EXPECT_NE(regionid(ss, 255, 65535), regionid(ss, 0, 0));
    EXPECT_LT(regionid(ss, 0, 0), regionid(ss, 0, 65535));
    EXPECT_LT(regionid(ss, 0, 0), regionid(ss, 255, 0));
    EXPECT_LT(regionid(ss, 0, 0), regionid(ss, 255, 65535));
    EXPECT_LE(regionid(ss, 0, 0), regionid(ss, 0, 65535));
    EXPECT_LE(regionid(ss, 0, 65535), regionid(ss, 0, 65535));
    EXPECT_LE(regionid(ss, 0, 0), regionid(ss, 255, 0));
    EXPECT_LE(regionid(ss, 255, 0), regionid(ss, 255, 0));
    EXPECT_LE(regionid(ss, 0, 0), regionid(ss, 255, 65535));
    EXPECT_LE(regionid(ss, 255, 65535), regionid(ss, 255, 65535));
    EXPECT_GE(regionid(ss, 255, 65535), regionid(ss, 255, 65535));
    EXPECT_GE(regionid(ss, 255, 65535), regionid(ss, 0, 0));
    EXPECT_GE(regionid(ss, 0, 65535), regionid(ss, 0, 65535));
    EXPECT_GE(regionid(ss, 0, 65535), regionid(ss, 0, 0));
    EXPECT_GE(regionid(ss, 255, 0), regionid(ss, 255, 0));
    EXPECT_GE(regionid(ss, 255, 0), regionid(ss, 0, 0));
    EXPECT_GT(regionid(ss, 255, 65535), regionid(ss, 0, 0));
    EXPECT_GT(regionid(ss, 255, 0), regionid(ss, 0, 0));
    EXPECT_GT(regionid(ss, 0, 65535), regionid(ss, 0, 0));
}

TEST(IdsTest, EntityidComparison)
{
    using hyperdex::spaceid;
    using hyperdex::subspaceid;
    using hyperdex::regionid;
    using hyperdex::entityid;

    // All the previous tests still pass if we just add a 0.
    EXPECT_EQ(entityid(regionid(subspaceid(spaceid(0), 0), 0, 0), 0), entityid(regionid(subspaceid(spaceid(0), 0), 0, 0), 0));
    EXPECT_NE(entityid(regionid(subspaceid(spaceid(0), 0), 0, 0), 0), entityid(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), 0));
    EXPECT_NE(entityid(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), 0), entityid(regionid(subspaceid(spaceid(0), 0), 0, 0), 0));
    EXPECT_LT(entityid(regionid(subspaceid(spaceid(0), 0), 0, 0), 0), entityid(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), 0));
    EXPECT_LE(entityid(regionid(subspaceid(spaceid(0), 0), 0, 0), 0), entityid(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), 0));
    EXPECT_LE(entityid(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), 0), entityid(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), 0));
    EXPECT_GE(entityid(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), 0), entityid(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), 0));
    EXPECT_GE(entityid(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), 0), entityid(regionid(subspaceid(spaceid(0), 0), 0, 0), 0));
    EXPECT_GT(entityid(regionid(subspaceid(spaceid(4294967295), 0), 0, 0), 0), entityid(regionid(subspaceid(spaceid(0), 0), 0, 0), 0));
    spaceid s(5);
    EXPECT_EQ(entityid(regionid(subspaceid(s, 0), 0, 0), 0), entityid(regionid(subspaceid(s, 0), 0, 0), 0));
    EXPECT_NE(entityid(regionid(subspaceid(s, 0), 0, 0), 0), entityid(regionid(subspaceid(s, 65535), 0, 0), 0));
    EXPECT_NE(entityid(regionid(subspaceid(s, 65535), 0, 0), 0), entityid(regionid(subspaceid(s, 0), 0, 0), 0));
    EXPECT_LT(entityid(regionid(subspaceid(s, 0), 0, 0), 0), entityid(regionid(subspaceid(s, 65535), 0, 0), 0));
    EXPECT_LE(entityid(regionid(subspaceid(s, 0), 0, 0), 0), entityid(regionid(subspaceid(s, 65535), 0, 0), 0));
    EXPECT_LE(entityid(regionid(subspaceid(s, 65535), 0, 0), 0), entityid(regionid(subspaceid(s, 65535), 0, 0), 0));
    EXPECT_GE(entityid(regionid(subspaceid(s, 65535), 0, 0), 0), entityid(regionid(subspaceid(s, 65535), 0, 0), 0));
    EXPECT_GE(entityid(regionid(subspaceid(s, 65535), 0, 0), 0), entityid(regionid(subspaceid(s, 0), 0, 0), 0));
    EXPECT_GT(entityid(regionid(subspaceid(s, 65535), 0, 0), 0), entityid(regionid(subspaceid(s, 0), 0, 0), 0));
    subspaceid ss(s, 10);
    EXPECT_EQ(entityid(regionid(ss, 0, 0), 0), entityid(regionid(ss, 0, 0), 0));
    EXPECT_EQ(entityid(regionid(ss, 255, 0), 0), entityid(regionid(ss, 255, 0), 0));
    EXPECT_EQ(entityid(regionid(ss, 0, 65535), 0), entityid(regionid(ss, 0, 65535), 0));
    EXPECT_EQ(entityid(regionid(ss, 255, 65535), 0), entityid(regionid(ss, 255, 65535), 0));
    EXPECT_NE(entityid(regionid(ss, 0, 0), 0), entityid(regionid(ss, 255, 0), 0));
    EXPECT_NE(entityid(regionid(ss, 0, 0), 0), entityid(regionid(ss, 0, 65535), 0));
    EXPECT_NE(entityid(regionid(ss, 0, 0), 0), entityid(regionid(ss, 255, 65535), 0));
    EXPECT_NE(entityid(regionid(ss, 255, 0), 0), entityid(regionid(ss, 0, 0), 0));
    EXPECT_NE(entityid(regionid(ss, 0, 65535), 0), entityid(regionid(ss, 0, 0), 0));
    EXPECT_NE(entityid(regionid(ss, 255, 65535), 0), entityid(regionid(ss, 0, 0), 0));
    EXPECT_LT(entityid(regionid(ss, 0, 0), 0), entityid(regionid(ss, 0, 65535), 0));
    EXPECT_LT(entityid(regionid(ss, 0, 0), 0), entityid(regionid(ss, 255, 0), 0));
    EXPECT_LT(entityid(regionid(ss, 0, 0), 0), entityid(regionid(ss, 255, 65535), 0));
    EXPECT_LE(entityid(regionid(ss, 0, 0), 0), entityid(regionid(ss, 0, 65535), 0));
    EXPECT_LE(entityid(regionid(ss, 0, 65535), 0), entityid(regionid(ss, 0, 65535), 0));
    EXPECT_LE(entityid(regionid(ss, 0, 0), 0), entityid(regionid(ss, 255, 0), 0));
    EXPECT_LE(entityid(regionid(ss, 255, 0), 0), entityid(regionid(ss, 255, 0), 0));
    EXPECT_LE(entityid(regionid(ss, 0, 0), 0), entityid(regionid(ss, 255, 65535), 0));
    EXPECT_LE(entityid(regionid(ss, 255, 65535), 0), entityid(regionid(ss, 255, 65535), 0));
    EXPECT_GE(entityid(regionid(ss, 255, 65535), 0), entityid(regionid(ss, 255, 65535), 0));
    EXPECT_GE(entityid(regionid(ss, 255, 65535), 0), entityid(regionid(ss, 0, 0), 0));
    EXPECT_GE(entityid(regionid(ss, 0, 65535), 0), entityid(regionid(ss, 0, 65535), 0));
    EXPECT_GE(entityid(regionid(ss, 0, 65535), 0), entityid(regionid(ss, 0, 0), 0));
    EXPECT_GE(entityid(regionid(ss, 255, 0), 0), entityid(regionid(ss, 255, 0), 0));
    EXPECT_GE(entityid(regionid(ss, 255, 0), 0), entityid(regionid(ss, 0, 0), 0));
    EXPECT_GT(entityid(regionid(ss, 255, 65535), 0), entityid(regionid(ss, 0, 0), 0));
    EXPECT_GT(entityid(regionid(ss, 255, 0), 0), entityid(regionid(ss, 0, 0), 0));
    EXPECT_GT(entityid(regionid(ss, 0, 65535), 0), entityid(regionid(ss, 0, 0), 0));

    // We get the comparisons we want if the region is the same.
    regionid r(ss, 32, 0xdeadbeef00000000);
    EXPECT_EQ(entityid(r, 0), entityid(r, 0));
    EXPECT_NE(entityid(r, 0), entityid(r, 255));
    EXPECT_NE(entityid(r, 255), entityid(r, 0));
    EXPECT_LT(entityid(r, 0), entityid(r, 255));
    EXPECT_LE(entityid(r, 0), entityid(r, 255));
    EXPECT_LE(entityid(r, 255), entityid(r, 255));
    EXPECT_GE(entityid(r, 255), entityid(r, 255));
    EXPECT_GE(entityid(r, 255), entityid(r, 0));
    EXPECT_GT(entityid(r, 255), entityid(r, 0));
}

} // namespace
