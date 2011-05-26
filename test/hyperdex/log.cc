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

// Create a variety of mappers.
TEST(LogTest, CtorAndDtor)
{
    hyperdex::log l;
    l.shutdown();
}

TEST(LogTest, CtorAndDtorDeathTest)
{
    ASSERT_DEATH(
        hyperdex::log l;
    , "");
}

TEST(LogTest, NonEmptyLogDeathTest)
{
    ASSERT_DEATH(
        hyperdex::log l;
        l.append(std::auto_ptr<hyperdex::log::record>());
    , "");
}

class uint64_t_record : public hyperdex::log::record
{
    public:
        uint64_t_record() : val() {}

    public:
        uint64_t val;
};

class vector_iterator : public hyperdex::log::iterator
{
    public:
        vector_iterator(hyperdex::log* l,
                        std::vector<uint64_t>* seqnos,
                        std::vector<uint64_t>* values)
            : iterator(l), m_seqnos(seqnos), m_values(values) {}
        ~vector_iterator() {}

    private:
        void callback(uint64_t seqno, hyperdex::log::record* rec)
        {
            m_seqnos->push_back(seqno);
            m_values->push_back(static_cast<uint64_t_record*>(rec)->val);
        }

    private:
        vector_iterator(const vector_iterator&);

    private:
        vector_iterator& operator = (const vector_iterator&);

    private:
        std::vector<uint64_t>* m_seqnos;
        std::vector<uint64_t>* m_values;
};

TEST(LogTest, SimpleIteration)
{
    // Add one thousand numbers to the queue.  Verify that we get them back.
    std::vector<uint64_t> seqnos;
    std::vector<uint64_t> values;
    hyperdex::log l;

    for (uint64_t i = 0; i < 1000; ++i)
    {
        std::auto_ptr<uint64_t_record> ur(new uint64_t_record());
        ur->val = i;
        std::auto_ptr<hyperdex::log::record> r(ur.release());
        l.append(r);
    }

    vector_iterator i(&l, &seqnos, &values);

    while (i.step())
        ;

    ASSERT_EQ(1000, seqnos.size());
    ASSERT_EQ(1000, values.size());

    for (size_t j = 0; j < 1000; ++j)
    {
        EXPECT_EQ(j + 1, seqnos[j]);
        EXPECT_EQ(j, values[j]);
    }

    l.flush();
    l.shutdown();
}

TEST(LogTest, IterateAddIterate)
{
    // Add ten numbers, iterate to the end.  Add ten more, iterate more.
    std::vector<uint64_t> seqnos;
    std::vector<uint64_t> values;
    hyperdex::log l;
    int j;

    for (uint64_t i = 0; i < 10; ++i)
    {
        std::auto_ptr<uint64_t_record> ur(new uint64_t_record());
        ur->val = i;
        std::auto_ptr<hyperdex::log::record> r(ur.release());
        l.append(r);
    }

    vector_iterator i(&l, &seqnos, &values);

    for (j = 0; i.step(); ++j)
        ;

    EXPECT_EQ(10, j);

    for (uint64_t k = 10; k < 20; ++k)
    {
        std::auto_ptr<uint64_t_record> ur(new uint64_t_record());
        ur->val = k;
        std::auto_ptr<hyperdex::log::record> r(ur.release());
        l.append(r);
    }

    for (j = 0; i.step(); ++j)
        ;

    EXPECT_EQ(10, j);

    ASSERT_EQ(20, seqnos.size());
    ASSERT_EQ(20, values.size());

    for (size_t k = 0; k < 20; ++k)
    {
        EXPECT_EQ(k + 1, seqnos[k]);
        EXPECT_EQ(k, values[k]);
    }

    l.flush();
    l.shutdown();
}

TEST(LogTest, IterateDrainIterate)
{
    // Add ten numbers, iterate five times.  Drain the queue.  Iterate five more
    // times.
    std::vector<uint64_t> seqnos;
    std::vector<uint64_t> values;
    hyperdex::log l;
    int j;

    for (uint64_t i = 0; i < 10; ++i)
    {
        std::auto_ptr<uint64_t_record> ur(new uint64_t_record());
        ur->val = i;
        std::auto_ptr<hyperdex::log::record> r(ur.release());
        l.append(r);
    }

    vector_iterator i(&l, &seqnos, &values);

    for (j = 0; j < 5; ++j)
    {
        i.step();
    }

    l.flush();

    for (j = 0; j < 5; ++j)
    {
        i.step();
    }

    EXPECT_FALSE(i.step());
    ASSERT_EQ(10, seqnos.size());
    ASSERT_EQ(10, values.size());

    for (size_t k = 0; k < 10; ++k)
    {
        EXPECT_EQ(k + 1, seqnos[k]);
        EXPECT_EQ(k, values[k]);
    }

    l.flush();
    l.shutdown();
}

TEST(LogTest, ConcurrentIteration)
{
    // Create two iterators, and interleave their iteration over the log.
    std::vector<uint64_t> seqnos;
    std::vector<uint64_t> values;
    hyperdex::log l;
    int j;

    for (uint64_t i = 0; i < 10; ++i)
    {
        std::auto_ptr<uint64_t_record> ur(new uint64_t_record());
        ur->val = i;
        std::auto_ptr<hyperdex::log::record> r(ur.release());
        l.append(r);
    }

    vector_iterator i1(&l, &seqnos, &values);
    vector_iterator i2(&l, &seqnos, &values);

    for (j = 0; j < 10; ++j)
    {
        i1.step();
        i2.step();
    }

    EXPECT_FALSE(i1.step());
    EXPECT_FALSE(i2.step());

    ASSERT_EQ(20, seqnos.size());
    ASSERT_EQ(20, values.size());

    for (size_t k = 0; k < 10; ++ k)
    {
        EXPECT_EQ(k + 1, seqnos[2 * k]);
        EXPECT_EQ(k + 1, seqnos[2 * k + 1]);
        EXPECT_EQ(k, values[2 * k]);
        EXPECT_EQ(k, values[2 * k + 1]);
    }

    l.flush();
    l.shutdown();
}

} // namespace
