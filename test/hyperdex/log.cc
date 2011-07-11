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
#include <hyperdex/log.h>

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

TEST(LogTest, CtorAndDtor)
{
    hyperdex::log l;
}

TEST(LogTest, SimpleIteration)
{
    hyperdex::log l;

    // Add one thousand put/del pairs to the queue.
    for (uint64_t i = 0; i < 1000; ++i)
    {
        e::buffer buf;
        buf.pack() << i;
        l.append(i + 1000, buf, i + 2000, std::vector<e::buffer>(i, buf), std::vector<uint64_t>(i, i), i + 3000);
        l.append(i + 1000, buf, i + 2000);
    }

    // Verify that we get them back.
    hyperdex::log::iterator it(l.iterate());

    for (uint64_t i = 0; i < 1000; ++i)
    {
        e::buffer buf;
        buf.pack() << i;

        // Check the PUT.
        EXPECT_TRUE(it.valid());
        EXPECT_EQ(hyperdex::PUT, it.op());
        EXPECT_TRUE(buf == it.key());
        ASSERT_EQ(i, it.value().size());

        for (uint64_t j = 0; j < i; ++j)
        {
            EXPECT_TRUE(buf == it.value()[j]);
        }

        EXPECT_EQ(i + 3000, it.version());

        // Check the DEL.
        it.next();
        ASSERT_TRUE(it.valid());
        EXPECT_EQ(hyperdex::DEL, it.op());
        EXPECT_TRUE(buf == it.key());

        // Advance for next iteration
        it.next();
    }

    EXPECT_FALSE(it.valid());
}

TEST(LogTest, IterateAddIterate)
{
    hyperdex::log l;
    uint64_t count = 1;
    hyperdex::log::iterator it(l.iterate());

    // Add 10 items to the queue.
    for (; count <= 10; ++count)
    {
        e::buffer buf;
        buf.pack() << count;
        l.append(count + 1000, buf, count + 2000, std::vector<e::buffer>(1, buf), std::vector<uint64_t>(1, count), count);
    }

    // Iterate to the end.
    for (size_t i = 1; i <= 10; ++i)
    {
        EXPECT_TRUE(it.valid());
        EXPECT_EQ(i, it.version());
        it.next();
    }

    EXPECT_FALSE(it.valid());
    EXPECT_FALSE(it.valid());

    // Add 10 more items to the queue.
    for (; count <= 20; ++count)
    {
        e::buffer buf;
        buf.pack() << count;
        l.append(count + 1000, buf, count + 2000, std::vector<e::buffer>(1, buf), std::vector<uint64_t>(1, count), count);
    }

    // Iterate to the end.
    for (size_t i = 11; i <= 20; ++i)
    {
        EXPECT_TRUE(it.valid());
        EXPECT_EQ(i, it.version());
        it.next();
    }

    EXPECT_FALSE(it.valid());
}

static void
NOP(hyperdex::op_t, uint64_t, const e::buffer&, uint64_t,
    const std::vector<e::buffer>&, const std::vector<uint64_t>&,
    uint64_t)
{
}

TEST(LogTest, IterateFlushIterate)
{
    hyperdex::log l;

    // Grab an iterator
    hyperdex::log::iterator it(l.iterate());

    // Add 20 items to the queue.
    for (uint64_t count = 1; count <= 20; ++count)
    {
        e::buffer buf;
        buf.pack() << count;
        l.append(count + 1000, buf, count + 2000, std::vector<e::buffer>(1, buf), std::vector<uint64_t>(1, count), count);
    }

    // Iterate to the halfway point.
    for (size_t i = 1; i <= 10; ++i)
    {
        EXPECT_TRUE(it.valid());
        EXPECT_EQ(i, it.version());
        it.next();
    }

    // Flush the log
    l.flush(NOP);

    // Iterate the rest of the way.
    for (size_t i = 11; i <= 20; ++i)
    {
        EXPECT_TRUE(it.valid());
        EXPECT_EQ(i, it.version());
        it.next();
    }

    EXPECT_FALSE(it.valid());
}

TEST(LogTest, CopyIterator)
{
    hyperdex::log l;

    // Add one thousand put/del pairs to the queue.
    for (uint64_t i = 0; i < 1000; ++i)
    {
        e::buffer buf;
        buf.pack() << i;
        l.append(i + 1000, buf, i + 2000, std::vector<e::buffer>(i, buf), std::vector<uint64_t>(i, i), i + 3000);
        l.append(i + 1000, buf, i + 2000);
    }

    // Verify that we get them back.
    hyperdex::log::iterator it(l.iterate());
    hyperdex::log::iterator copy(it);

    for (uint64_t i = 0; i < 1000; ++i)
    {
        e::buffer buf;
        buf.pack() << i;

        // Check the PUT.
        EXPECT_TRUE(it.valid());
        EXPECT_TRUE(copy.valid());
        EXPECT_EQ(hyperdex::PUT, it.op());
        EXPECT_EQ(hyperdex::PUT, copy.op());
        EXPECT_TRUE(buf == it.key());
        EXPECT_TRUE(buf == copy.key());
        ASSERT_EQ(i, it.value().size());
        ASSERT_EQ(i, copy.value().size());

        for (uint64_t j = 0; j < i; ++j)
        {
            EXPECT_TRUE(buf == it.value()[j]);
            EXPECT_TRUE(buf == copy.value()[j]);
        }

        EXPECT_EQ(i + 3000, it.version());
        EXPECT_EQ(i + 3000, copy.version());

        // Check the DEL.
        it.next();
        copy.next();
        ASSERT_TRUE(it.valid());
        ASSERT_TRUE(copy.valid());
        EXPECT_EQ(hyperdex::DEL, it.op());
        EXPECT_EQ(hyperdex::DEL, copy.op());
        EXPECT_TRUE(buf == it.key());
        EXPECT_TRUE(buf == copy.key());

        // Advance for next iteration
        it.next();
        copy.next();
    }

    EXPECT_FALSE(it.valid());
    EXPECT_FALSE(copy.valid());
}

} // namespace
