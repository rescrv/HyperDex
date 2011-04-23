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

// STL
#include <stdexcept>

// Google Test
#include <gtest/gtest.h>

// HyperDex
#include <hyperdex/entity.h>

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

TEST(EntityTest, CtorAndDtor)
{
    hyperdex::entity e;
}

TEST(EntityTest, SerializedConstructor)
{
    hyperdex::entity e("\x05\x12\x34\x56\x78\xaa\xbb\xcc\xdd\xee");
    EXPECT_EQ(hyperdex::entity::CHAIN_REPLICA, e.type);
    EXPECT_EQ(0x12345678, e.table);
    EXPECT_EQ(0xaabb, e.subspace);
    EXPECT_EQ(0xccdd, e.zone);
    EXPECT_EQ(0xee, e.number);
}

TEST(EntityTest, Serialization)
{
    hyperdex::entity e(hyperdex::entity::CHAIN_REPLICA, 0x12345678, 0xaabb, 0xccdd, 0xee);
    char buf[10];
    e.serialize(buf);
    EXPECT_EQ(0, memcmp(buf, "\x05\x12\x34\x56\x78\xaa\xbb\xcc\xdd\xee", 10));
}

TEST(EntityTest, BadType)
{
    bool caught = false;

    try
    {
        hyperdex::entity e("\x06\x12\x34\x56\x78\xaa\xbb\xcc\xdd\xee");
    }
    catch (std::invalid_argument& e)
    {
        caught = true;
    }

    EXPECT_TRUE(caught);
}

} // namespace
