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
#include <hyperdex/mapper.h>

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

// Create a variety of mappers.
TEST(MapperTest, CtorAndDtor)
{
    for (size_t i = 1; i < (2 << 20); i *= 2)
    {
        std::vector<size_t> dimensions(i);

        for (size_t j = 1; j <= i; ++j)
        {
            dimensions[j - 1] = j;
        }

        hyperdex::mapper map(dimensions);
    }
}

// The constructed table breaks into 8 cells.
//
// 0 - hash(A) = A1; hash(B) = B1; hash(C) = C1
// 1 - hash(A) = A1; hash(B) = B1; hash(C) = C2
// 2 - hash(A) = A1; hash(B) = B2; hash(C) = C1
// 3 - hash(A) = A1; hash(B) = B2; hash(C) = C2
// 4 - hash(A) = A2; hash(B) = B1; hash(C) = C1
// 5 - hash(A) = A2; hash(B) = B1; hash(C) = C2
// 6 - hash(A) = A2; hash(B) = B2; hash(C) = C1
// 7 - hash(A) = A2; hash(B) = B2; hash(C) = C2

TEST(MapperTest, ThreeColumnTableOneColumnQuery)
{
    std::vector<size_t> d;
    d.push_back(2);
    d.push_back(2);
    d.push_back(2);

    hyperdex::mapper m(d);
    hyperdex::query q(3);
    std::vector<size_t> ret;

    // Test the first column for both options.
    q.set(0, "a");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(0, ret[0]);
    EXPECT_EQ(1, ret[1]);
    EXPECT_EQ(2, ret[2]);
    EXPECT_EQ(3, ret[3]);
    q.set(0, "b");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(4, ret[0]);
    EXPECT_EQ(5, ret[1]);
    EXPECT_EQ(6, ret[2]);
    EXPECT_EQ(7, ret[3]);

    // Test the second column for both options.
    q.unset(0);
    q.set(1, "a");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(0, ret[0]);
    EXPECT_EQ(1, ret[1]);
    EXPECT_EQ(4, ret[2]);
    EXPECT_EQ(5, ret[3]);
    q.set(1, "b");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(2, ret[0]);
    EXPECT_EQ(3, ret[1]);
    EXPECT_EQ(6, ret[2]);
    EXPECT_EQ(7, ret[3]);

    // Test the third column for both options.
    q.unset(1);
    q.set(2, "a");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(0, ret[0]);
    EXPECT_EQ(2, ret[1]);
    EXPECT_EQ(4, ret[2]);
    EXPECT_EQ(6, ret[3]);
    q.set(2, "b");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(1, ret[0]);
    EXPECT_EQ(3, ret[1]);
    EXPECT_EQ(5, ret[2]);
    EXPECT_EQ(7, ret[3]);
}

TEST(MapperTest, ThreeColumnTableTwoColumnQuery)
{
    std::vector<size_t> d;
    d.push_back(2);
    d.push_back(2);
    d.push_back(2);

    hyperdex::mapper m(d);
    hyperdex::query q(3);
    std::vector<size_t> ret;

    // Test the first,second columns for all four options.
    q.clear();
    q.set(0, "a");
    q.set(1, "a");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(2, ret.size());
    EXPECT_EQ(0, ret[0]);
    EXPECT_EQ(1, ret[1]);
    q.set(0, "a");
    q.set(1, "b");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(2, ret.size());
    EXPECT_EQ(2, ret[0]);
    EXPECT_EQ(3, ret[1]);
    q.set(0, "b");
    q.set(1, "a");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(2, ret.size());
    EXPECT_EQ(4, ret[0]);
    EXPECT_EQ(5, ret[1]);
    q.set(0, "b");
    q.set(1, "b");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(2, ret.size());
    EXPECT_EQ(6, ret[0]);
    EXPECT_EQ(7, ret[1]);

    // Test the first,third columns for all four options.
    q.clear();
    q.set(0, "a");
    q.set(2, "a");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(2, ret.size());
    EXPECT_EQ(0, ret[0]);
    EXPECT_EQ(2, ret[1]);
    q.set(0, "a");
    q.set(2, "b");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(2, ret.size());
    EXPECT_EQ(1, ret[0]);
    EXPECT_EQ(3, ret[1]);
    q.set(0, "b");
    q.set(2, "a");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(2, ret.size());
    EXPECT_EQ(4, ret[0]);
    EXPECT_EQ(6, ret[1]);
    q.set(0, "b");
    q.set(2, "b");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(2, ret.size());
    EXPECT_EQ(5, ret[0]);
    EXPECT_EQ(7, ret[1]);

    // Test the second,third columns for all four options.
    q.clear();
    q.set(1, "a");
    q.set(2, "a");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(2, ret.size());
    EXPECT_EQ(0, ret[0]);
    EXPECT_EQ(4, ret[1]);
    q.set(1, "a");
    q.set(2, "b");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(2, ret.size());
    EXPECT_EQ(1, ret[0]);
    EXPECT_EQ(5, ret[1]);
    q.set(1, "b");
    q.set(2, "a");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(2, ret.size());
    EXPECT_EQ(2, ret[0]);
    EXPECT_EQ(6, ret[1]);
    q.set(1, "b");
    q.set(2, "b");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(2, ret.size());
    EXPECT_EQ(3, ret[0]);
    EXPECT_EQ(7, ret[1]);
}

TEST(MapperTest, ThreeColumnTableThreeColumnQuery)
{
    std::vector<size_t> d;
    d.push_back(2);
    d.push_back(2);
    d.push_back(2);

    hyperdex::mapper m(d);
    hyperdex::query q(3);
    std::vector<size_t> ret;

    q.set(0, "a");
    q.set(1, "a");
    q.set(2, "a");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(1, ret.size());
    EXPECT_EQ(0, ret[0]);

    q.set(0, "a");
    q.set(1, "a");
    q.set(2, "b");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(1, ret.size());
    EXPECT_EQ(1, ret[0]);

    q.set(0, "a");
    q.set(1, "b");
    q.set(2, "a");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(1, ret.size());
    EXPECT_EQ(2, ret[0]);

    q.set(0, "a");
    q.set(1, "b");
    q.set(2, "b");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(1, ret.size());
    EXPECT_EQ(3, ret[0]);

    q.set(0, "b");
    q.set(1, "a");
    q.set(2, "a");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(1, ret.size());
    EXPECT_EQ(4, ret[0]);

    q.set(0, "b");
    q.set(1, "a");
    q.set(2, "b");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(1, ret.size());
    EXPECT_EQ(5, ret[0]);

    q.set(0, "b");
    q.set(1, "b");
    q.set(2, "a");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(1, ret.size());
    EXPECT_EQ(6, ret[0]);

    q.set(0, "b");
    q.set(1, "b");
    q.set(2, "b");
    ret.clear();
    m.map(q, 8, &ret);
    EXPECT_EQ(1, ret.size());
    EXPECT_EQ(7, ret[0]);
}

} // namespace
