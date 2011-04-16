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
#include <hyperdex/query.h>

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

// Create and destroy queries of various sizes.
TEST(QueryTest, CtorAndDtor)
{
    for (size_t i = 0; i < 65536; ++i)
    {
        hyperdex::query q(i);
    }
}

// Create a query and set/unset the values while testing that it still matches
// the row.
TEST(QueryTest, SetAndUnset)
{
    hyperdex::query q(10);
    std::vector<std::string> row(10, "value");

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_TRUE(q.matches(row));
        q.set(i, "value");
    }

    EXPECT_TRUE(q.matches(row));

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_TRUE(q.matches(row));
        q.set(i, "value");
    }

    EXPECT_TRUE(q.matches(row));
}

TEST(QueryTest, Clear)
{
    hyperdex::query q(2);
    std::vector<std::string> r;

    r.push_back("key");
    r.push_back("val");
    EXPECT_TRUE(q.matches(r));
    q.set(0, "not-key");
    q.set(1, "not-val");
    EXPECT_FALSE(q.matches(r));
    q.clear();
    EXPECT_TRUE(q.matches(r));
}

// Test non-matches
TEST(QueryTest, NegativeMatch)
{
    hyperdex::query q(2);
    std::vector<std::string> r;

    r.push_back("key");
    r.push_back("val");
    EXPECT_TRUE(q.matches(r));
    q.set(0, "not-key");
    EXPECT_FALSE(q.matches(r));
    q.unset(0);
    EXPECT_TRUE(q.matches(r));
    q.set(1, "not-val");
    EXPECT_FALSE(q.matches(r));
}

// If we try to set out of bounds we fail an assertion.
TEST(QueryTest, SetDeathTest)
{
    hyperdex::query q(5);

    ASSERT_DEATH(
        q.set(5, "out of bounds");
    , "Assertion");
}

// If we try to unset out of bounds we fail an assertion.
TEST(QueryTest, UnsetDeathTest)
{
    hyperdex::query q(5);

    ASSERT_DEATH(
        q.unset(5);
    , "Assertion");
}

// If we try to match with improper arity we fail an assertion.
TEST(QueryTest, MatchDeathTest)
{
    hyperdex::query q(5);

    ASSERT_DEATH(
        q.matches(std::vector<std::string>(4));
    , "Assertion");
}

} // namespace
