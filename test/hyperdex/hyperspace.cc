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
#include <hyperdex/hyperspace.h>

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

TEST(HyperspaceTest, SpacingTest)
{
    EXPECT_EQ(0xdeadbeefcafebabe, hyperdex::spacing(0xdeadbeefcafebabe, 1));
    EXPECT_EQ(0xa2a888a28aa8a8aa, hyperdex::spacing(0xdeadbeefcafebabe, 2));
    EXPECT_EQ(0x9049208209048249, hyperdex::spacing(0xdeadbeefcafebabe, 3));
    EXPECT_EQ(0x8808888080808808, hyperdex::spacing(0xdeadbeefcafebabe, 4));
    EXPECT_EQ(0x8401084200802008, hyperdex::spacing(0xdeadbeefcafebabe, 5));
    EXPECT_EQ(0x8200208208008008, hyperdex::spacing(0xdeadbeefcafebabe, 6));
    EXPECT_EQ(0x8100040810200080, hyperdex::spacing(0xdeadbeefcafebabe, 7));
    EXPECT_EQ(0x8080008080808000, hyperdex::spacing(0xdeadbeefcafebabe, 8));
    EXPECT_EQ(0x8040001008040200, hyperdex::spacing(0xdeadbeefcafebabe, 9));
    EXPECT_EQ(0x8020000200802008, hyperdex::spacing(0xdeadbeefcafebabe, 10));
    EXPECT_EQ(0x8010000040080100, hyperdex::spacing(0xdeadbeefcafebabe, 11));
    EXPECT_EQ(0x8008000008008008, hyperdex::spacing(0xdeadbeefcafebabe, 12));
    EXPECT_EQ(0x8004000001000800, hyperdex::spacing(0xdeadbeefcafebabe, 13));
    EXPECT_EQ(0x8002000000200080, hyperdex::spacing(0xdeadbeefcafebabe, 14));
    EXPECT_EQ(0x8001000000040008, hyperdex::spacing(0xdeadbeefcafebabe, 15));
    EXPECT_EQ(0x8000800000008000, hyperdex::spacing(0xdeadbeefcafebabe, 16));
    EXPECT_EQ(0x8000400000001000, hyperdex::spacing(0xdeadbeefcafebabe, 17));
    EXPECT_EQ(0x8000200000000200, hyperdex::spacing(0xdeadbeefcafebabe, 18));
    EXPECT_EQ(0x8000100000000040, hyperdex::spacing(0xdeadbeefcafebabe, 19));
    EXPECT_EQ(0x8000080000000008, hyperdex::spacing(0xdeadbeefcafebabe, 20));
    EXPECT_EQ(0x8000040000000001, hyperdex::spacing(0xdeadbeefcafebabe, 21));
    EXPECT_EQ(0x8000020000000000, hyperdex::spacing(0xdeadbeefcafebabe, 22));
    EXPECT_EQ(0x8000010000000000, hyperdex::spacing(0xdeadbeefcafebabe, 23));
    EXPECT_EQ(0x8000008000000000, hyperdex::spacing(0xdeadbeefcafebabe, 24));
    EXPECT_EQ(0x8000004000000000, hyperdex::spacing(0xdeadbeefcafebabe, 25));
    EXPECT_EQ(0x8000002000000000, hyperdex::spacing(0xdeadbeefcafebabe, 26));
    EXPECT_EQ(0x8000001000000000, hyperdex::spacing(0xdeadbeefcafebabe, 27));
    EXPECT_EQ(0x8000000800000000, hyperdex::spacing(0xdeadbeefcafebabe, 28));
    EXPECT_EQ(0x8000000400000000, hyperdex::spacing(0xdeadbeefcafebabe, 29));
    EXPECT_EQ(0x8000000200000000, hyperdex::spacing(0xdeadbeefcafebabe, 30));
    EXPECT_EQ(0x8000000100000000, hyperdex::spacing(0xdeadbeefcafebabe, 31));
    EXPECT_EQ(0x8000000080000000, hyperdex::spacing(0xdeadbeefcafebabe, 32));
    EXPECT_EQ(0x8000000040000000, hyperdex::spacing(0xdeadbeefcafebabe, 33));
    EXPECT_EQ(0x8000000020000000, hyperdex::spacing(0xdeadbeefcafebabe, 34));
    EXPECT_EQ(0x8000000010000000, hyperdex::spacing(0xdeadbeefcafebabe, 35));
    EXPECT_EQ(0x8000000008000000, hyperdex::spacing(0xdeadbeefcafebabe, 36));
    EXPECT_EQ(0x8000000004000000, hyperdex::spacing(0xdeadbeefcafebabe, 37));
    EXPECT_EQ(0x8000000002000000, hyperdex::spacing(0xdeadbeefcafebabe, 38));
    EXPECT_EQ(0x8000000001000000, hyperdex::spacing(0xdeadbeefcafebabe, 39));
    EXPECT_EQ(0x8000000000800000, hyperdex::spacing(0xdeadbeefcafebabe, 40));
    EXPECT_EQ(0x8000000000400000, hyperdex::spacing(0xdeadbeefcafebabe, 41));
    EXPECT_EQ(0x8000000000200000, hyperdex::spacing(0xdeadbeefcafebabe, 42));
    EXPECT_EQ(0x8000000000100000, hyperdex::spacing(0xdeadbeefcafebabe, 43));
    EXPECT_EQ(0x8000000000080000, hyperdex::spacing(0xdeadbeefcafebabe, 44));
    EXPECT_EQ(0x8000000000040000, hyperdex::spacing(0xdeadbeefcafebabe, 45));
    EXPECT_EQ(0x8000000000020000, hyperdex::spacing(0xdeadbeefcafebabe, 46));
    EXPECT_EQ(0x8000000000010000, hyperdex::spacing(0xdeadbeefcafebabe, 47));
    EXPECT_EQ(0x8000000000008000, hyperdex::spacing(0xdeadbeefcafebabe, 48));
    EXPECT_EQ(0x8000000000004000, hyperdex::spacing(0xdeadbeefcafebabe, 49));
    EXPECT_EQ(0x8000000000002000, hyperdex::spacing(0xdeadbeefcafebabe, 50));
    EXPECT_EQ(0x8000000000001000, hyperdex::spacing(0xdeadbeefcafebabe, 51));
    EXPECT_EQ(0x8000000000000800, hyperdex::spacing(0xdeadbeefcafebabe, 52));
    EXPECT_EQ(0x8000000000000400, hyperdex::spacing(0xdeadbeefcafebabe, 53));
    EXPECT_EQ(0x8000000000000200, hyperdex::spacing(0xdeadbeefcafebabe, 54));
    EXPECT_EQ(0x8000000000000100, hyperdex::spacing(0xdeadbeefcafebabe, 55));
    EXPECT_EQ(0x8000000000000080, hyperdex::spacing(0xdeadbeefcafebabe, 56));
    EXPECT_EQ(0x8000000000000040, hyperdex::spacing(0xdeadbeefcafebabe, 57));
    EXPECT_EQ(0x8000000000000020, hyperdex::spacing(0xdeadbeefcafebabe, 58));
    EXPECT_EQ(0x8000000000000010, hyperdex::spacing(0xdeadbeefcafebabe, 59));
    EXPECT_EQ(0x8000000000000008, hyperdex::spacing(0xdeadbeefcafebabe, 60));
    EXPECT_EQ(0x8000000000000004, hyperdex::spacing(0xdeadbeefcafebabe, 61));
    EXPECT_EQ(0x8000000000000002, hyperdex::spacing(0xdeadbeefcafebabe, 62));
    EXPECT_EQ(0x8000000000000001, hyperdex::spacing(0xdeadbeefcafebabe, 63));
    EXPECT_EQ(0x8000000000000000, hyperdex::spacing(0xdeadbeefcafebabe, 64));
}

} // namespace
