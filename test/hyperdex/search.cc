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
#include <hyperdex/search.h>

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

// Create and destroy queries of various sizes.
TEST(SearchTest, CtorAndDtor)
{
    for (size_t i = 1; i < 65536; i *= 2)
    {
        hyperdex::search q(i);
    }
}

// Create a search and set/unset the values while testing that it still matches
// the row.
TEST(SearchTest, SetAndUnset)
{
    hyperdex::search q(10);
    e::buffer key("value", 5);
    std::vector<e::buffer> value(9, e::buffer("value", 5));

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_TRUE(q.matches(key, value));
        q.set(i, e::buffer("value", 5));
    }

    EXPECT_TRUE(q.matches(key, value));

    for (size_t i = 0; i < 10; ++i)
    {
        EXPECT_TRUE(q.matches(key, value));
        q.unset(i);
    }

    EXPECT_TRUE(q.matches(key, value));
}

TEST(SearchTest, Clear)
{
    hyperdex::search q(2);
    e::buffer key("key", 3);
    std::vector<e::buffer> val;
    val.push_back(e::buffer("val", 4));
    EXPECT_TRUE(q.matches(key, val));
    q.set(0, e::buffer("not-key", 7));
    q.set(1, e::buffer("not-val", 7));
    EXPECT_FALSE(q.matches(key, val));
    q.clear();
    EXPECT_TRUE(q.matches(key, val));
}

// Test non-matches
TEST(SearchTest, NegativeMatch)
{
    hyperdex::search q(2);
    e::buffer key("key", 3);
    std::vector<e::buffer> val;
    val.push_back(e::buffer("val", 3));
    EXPECT_TRUE(q.matches(key, val));
    q.set(0, e::buffer("not-key", 7));
    EXPECT_FALSE(q.matches(key, val));
    q.unset(0);
    EXPECT_TRUE(q.matches(key, val));
    q.set(1, e::buffer("not-val", 7));
    EXPECT_FALSE(q.matches(key, val));
}

// If we try to set out of bounds we fail an assertion.
TEST(SearchTest, SetDeathTest)
{
    hyperdex::search q(5);

    ASSERT_DEATH(
        q.set(5, e::buffer("out of bounds", 13));
    , "Assertion");
}

// If we try to unset out of bounds we fail an assertion.
TEST(SearchTest, UnsetDeathTest)
{
    hyperdex::search q(5);

    ASSERT_DEATH(
        q.unset(5);
    , "Assertion");
}

// If we try to match with improper arity we fail an assertion.
TEST(SearchTest, MatchDeathTest)
{
    hyperdex::search q(5);

    ASSERT_DEATH(
        q.matches(e::buffer(), std::vector<e::buffer>(3));
    , "Assertion");
}

} // namespace
