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

// HyperspaceHashing
#include "hyperspacehashing/hyperspacehashing/hashes.h"
#include "hyperspacehashing/hyperspacehashing/prefix.h"

#pragma GCC diagnostic ignored "-Wswitch-default"

using namespace hyperspacehashing;
using namespace hyperspacehashing::prefix;

namespace
{

TEST(PrefixTest, KeyOnly)
{
    std::vector<hash_t> hf(1, EQUALITY);
    hasher h(hf);
    coordinate c;
    c = h.hash(e::slice("key", 3));
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(11062368440904502521ULL, c.point);
    c = h.hash(e::slice("key", 3), std::vector<e::slice>());
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(11062368440904502521ULL, c.point);
}

TEST(PrefixTest, KeyOnlyWValue)
{
    std::vector<hash_t> hf(2);
    hf[0] = EQUALITY;
    hf[1] = NONE;
    hasher h(hf);
    coordinate c;
    c = h.hash(e::slice("key", 3));
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(11062368440904502521ULL, c.point);
    c = h.hash(e::slice("key", 3), std::vector<e::slice>(1));
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(11062368440904502521ULL, c.point);
}

TEST(PrefixTest, KeyValue)
{
    std::vector<hash_t> hf(2);
    coordinate c;

    // Key/Value: hash on value
    hf[0] = EQUALITY;
    hf[1] = NONE;
    hasher h(hf);
    c = h.hash(e::slice("key", 3), std::vector<e::slice>(1, e::slice("key", 3)));
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(11062368440904502521ULL, c.point);

    // Key/Value: hash on both
    hf[0] = EQUALITY;
    hf[1] = EQUALITY;
    h = hasher(hf);
    c = h.hash(e::slice("key", 3), std::vector<e::slice>(1, e::slice("key", 3)));
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(14106329784152965104ULL, c.point);

    // Key/<Value1,Value2>: hash on value1/value2
    hf.resize(3);
    hf[0] = NONE;
    hf[1] = EQUALITY;
    hf[2] = EQUALITY;
    h = hasher(hf);
    std::vector<e::slice> value;
    value.push_back(e::slice("value1", 6));
    value.push_back(e::slice("value2", 6));
    c = h.hash(e::slice("key", 3), value);
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(15831543697747031123ULL, c.point);
}

TEST(PrefixTest, KeyOnlyRange)
{
    std::vector<hash_t> hf(1, RANGE);
    hasher h(hf);
    coordinate c;
    c = h.hash(e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8));
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(16045690984503098046ULL, c.point);
    c = h.hash(e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8), std::vector<e::slice>());
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(16045690984503098046ULL, c.point);
}

TEST(PrefixTest, KeyOnlyWValueRange)
{
    std::vector<hash_t> hf(2);
    hf[0] = RANGE;
    hf[1] = NONE;
    hasher h(hf);
    coordinate c;
    c = h.hash(e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8));
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(16045690984503098046ULL, c.point);
    c = h.hash(e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8),
               std::vector<e::slice>(1));
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(16045690984503098046ULL, c.point);
}

TEST(PrefixTest, KeyValueRange)
{
    std::vector<hash_t> hf(2);
    coordinate c;
    std::vector<e::slice> value;

    // Key/Value: hash on key (key is range)
    hf[0] = RANGE;
    hf[1] = NONE;
    hasher h(hf);
    c = h.hash(e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8),
               std::vector<e::slice>(1, e::slice("key", 3)));
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(16045690984503098046ULL, c.point);

    // Key/Value: hash on value (value is range)
    hf[0] = NONE;
    hf[1] = RANGE;
    h = hasher(hf);
    c = h.hash(e::slice("key", 3),
               std::vector<e::slice>(1, e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8)));
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(16045690984503098046ULL, c.point);

    // Key/Value: hash on both (key is range)
    hf[0] = RANGE;
    hf[1] = EQUALITY;
    h = hasher(hf);
    c = h.hash(e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8),
               std::vector<e::slice>(1, e::slice("key", 3)));
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(16422878189975092730ULL, c.point);

    // Key/Value: hash on both (value is range)
    hf[0] = EQUALITY;
    hf[1] = RANGE;
    h = hasher(hf);
    c = h.hash(e::slice("key", 3),
               std::vector<e::slice>(1, e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8)));
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(15264603987064028917ULL, c.point);

    // Key/<Value1,Value2>: hash on value1/value2 (value1 is range)
    hf.resize(3);
    hf[0] = NONE;
    hf[1] = RANGE;
    hf[2] = EQUALITY;
    h = hasher(hf);
    value.clear();
    value.push_back(e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8));
    value.push_back(e::slice("value2", 6));
    c = h.hash(e::slice("key", 3), value);
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(17563152029238017275ULL, c.point);

    // Key/<Value1,Value2>: hash on value1/value2 (value2 is range)
    hf.resize(3);
    hf[0] = NONE;
    hf[1] = EQUALITY;
    hf[2] = RANGE;
    h = hasher(hf);
    value.clear();
    value.push_back(e::slice("value1", 6));
    value.push_back(e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8));
    c = h.hash(e::slice("key", 3), value);
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(15849544061395170391ULL, c.point);

    // Key/<Value1,Value2>: hash on value1/value2 (both are range)
    hf.resize(3);
    hf[0] = NONE;
    hf[1] = RANGE;
    hf[2] = RANGE;
    h = hasher(hf);
    value.clear();
    value.push_back(e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8));
    value.push_back(e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8));
    c = h.hash(e::slice("key", 3), value);
    ASSERT_EQ(64, c.prefix);
    ASSERT_EQ(17581152392886156543ULL, c.point);
}

} // namespace
