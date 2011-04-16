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
#include <hyperdex/bloom.h>

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

// Create a bloom filter with 0.1% probability of false positive.
TEST(BloomTest, CtorAndDtor)
{
    hyperdex::bloom bf(1000, 0.01);
}

// Add some strings and check that they exist.
TEST(BloomTest, AddAndCheck)
{
    hyperdex::bloom bf(1000, 0.01);
    bf.add("foo");
    bf.add("bar");
    bf.add("baz");
    bf.add("quux");
    EXPECT_TRUE(bf.check("foo"));
    EXPECT_TRUE(bf.check("bar"));
    EXPECT_TRUE(bf.check("baz"));
    EXPECT_TRUE(bf.check("quux"));
    EXPECT_FALSE(bf.check("foobarbazquux"));
}

// Check that the assignment operator works.
TEST(BloomTest, Assignment)
{
    hyperdex::bloom bf(1000, 0.01);
    bf.add("foo");
    hyperdex::bloom bf2(1, 1);
    bf2 = bf;
    EXPECT_TRUE(bf2.check("foo"));
}

// Check that the copy constructor works.
TEST(BloomTest, CopyConstructor)
{
    hyperdex::bloom bf(1000, 0.01);
    bf.add("foo");
    hyperdex::bloom bf2(bf);
    EXPECT_TRUE(bf2.check("foo"));
}

// Check that serialization works.
TEST(BloomTest, Serialization)
{
    hyperdex::bloom bf(1000, 0.01);
    bf.add("foo");
    bf.add("bar");
    bf.add("baz");
    bf.add("quux");

    hyperdex::bloom bf2(bf.serialize());
    EXPECT_TRUE(bf2.check("foo"));
    EXPECT_TRUE(bf2.check("bar"));
    EXPECT_TRUE(bf2.check("baz"));
    EXPECT_TRUE(bf2.check("quux"));
    EXPECT_FALSE(bf2.check("foobarbazquux"));
}

} // namespace
