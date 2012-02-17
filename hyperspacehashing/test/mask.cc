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
#include "hyperspacehashing/hyperspacehashing/mask.h"

#pragma GCC diagnostic ignored "-Wswitch-default"

using namespace hyperspacehashing;
using namespace hyperspacehashing::mask;

// Mask-based hashing treats the key and the value separately.  This
// checks that hash(key) does not affect the secondary hash, and
// hash(value) does not affect the primary hash.  It then checks that
// hash(key, value) is the combination of the two simpler hashes.
static void
all_permutations(const std::vector<hash_t>& hf,
                 uint64_t pmask, uint64_t phash, const e::slice& key,
                 uint64_t slmask, uint64_t slhash,
                 uint64_t sumask, uint64_t suhash,
                 const std::vector<e::slice>& value)
{
    hasher h(hf);
    coordinate c;
    c = h.hash(key);
    ASSERT_EQ(pmask, c.primary_mask);
    ASSERT_EQ(phash, c.primary_hash);
    ASSERT_EQ(0, c.secondary_lower_mask);
    ASSERT_EQ(0, c.secondary_lower_hash);
    ASSERT_EQ(0, c.secondary_upper_mask);
    ASSERT_EQ(0, c.secondary_upper_hash);
    c = h.hash(value);
    ASSERT_EQ(0, c.primary_mask);
    ASSERT_EQ(0, c.primary_hash);
    ASSERT_EQ(slmask, c.secondary_lower_mask);
    ASSERT_EQ(slhash, c.secondary_lower_hash);
    ASSERT_EQ(sumask, c.secondary_upper_mask);
    ASSERT_EQ(suhash, c.secondary_upper_hash);
    c = h.hash(key, value);
    ASSERT_EQ(pmask, c.primary_mask);
    ASSERT_EQ(phash, c.primary_hash);
    ASSERT_EQ(slmask, c.secondary_lower_mask);
    ASSERT_EQ(slhash, c.secondary_lower_hash);
    ASSERT_EQ(sumask, c.secondary_upper_mask);
    ASSERT_EQ(suhash, c.secondary_upper_hash);
}

namespace
{

TEST(MaskTest, KeyOnly)
{
    std::vector<hash_t> hf(1, EQUALITY);
    all_permutations(hf,
                     UINT64_MAX, 0x99856d7c6e9accf9, e::slice("key", 3),
                     0, 0, 0, 0, std::vector<e::slice>());
}

TEST(MaskTest, KeyOnlyWValue)
{
    std::vector<hash_t> hf(2);
    hf[0] = EQUALITY;
    hf[1] = NONE;
    all_permutations(hf,
                     UINT64_MAX, 0x99856d7c6e9accf9, e::slice("key", 3),
                     0, 0, 0, 0, std::vector<e::slice>(1));
}

TEST(MaskTest, KeyValue)
{
    std::vector<hash_t> hf(2, EQUALITY);

    // Key/Value: both used
    hf[0] = EQUALITY;
    hf[1] = EQUALITY;
    all_permutations(hf,
                     UINT64_MAX, 0x99856d7c6e9accf9ULL, e::slice("key", 3),
                     UINT64_MAX, 0xee421cea2462bca6ULL, 0, 0, std::vector<e::slice>(1, e::slice("value", 5)));

    // Key/Value: The key is not used.
    hf[0] = NONE;
    hf[1] = EQUALITY;
    all_permutations(hf,
                     0, 0, e::slice("key", 3),
                     UINT64_MAX, 0xee421cea2462bca6ULL, 0, 0, std::vector<e::slice>(1, e::slice("value", 5)));

    // Key/<Value1,Value2>: both used
    hf.resize(3);
    hf[0] = EQUALITY;
    hf[1] = EQUALITY;
    hf[2] = EQUALITY;
    std::vector<e::slice> value;
    value.push_back(e::slice("value1", 6));
    value.push_back(e::slice("value2", 6));
    all_permutations(hf,
                     UINT64_MAX, 0x99856d7c6e9accf9ULL, e::slice("key", 3),
                     UINT64_MAX, 0xaa73898f407c3dc6ULL, UINT64_MAX, 0xe778f22e1d8638a3ULL, value);
}

TEST(MaskTest, KeyOnlyRange)
{
    std::vector<hash_t> hf(1, RANGE);
    all_permutations(hf,
                     UINT64_MAX, 0xdeadbeefcafebabeULL, e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8),
                     0, 0, 0, 0, std::vector<e::slice>());
}

TEST(MaskTest, KeyOnlyWValueRange)
{
    std::vector<hash_t> hf(2);
    hf[0] = RANGE;
    hf[1] = NONE;
    all_permutations(hf,
                     UINT64_MAX, 0xdeadbeefcafebabeULL, e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8),
                     0, 0, 0, 0, std::vector<e::slice>(1));
}

TEST(MaskTest, KeyValueRange)
{
    std::vector<hash_t> hf(2);
    std::vector<e::slice> value;

    // Key/Value: hash on both (key is range)
    hf[0] = RANGE;
    hf[1] = EQUALITY;
    all_permutations(hf,
                     UINT64_MAX, 0xdeadbeefcafebabeULL, e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8),
                     UINT64_MAX, 0xee421cea2462bca6ULL, 0, 0, std::vector<e::slice>(1, e::slice("value", 5)));

    // Key/Value: hash on both (value is range)
    hf[0] = EQUALITY;
    hf[1] = RANGE;
    all_permutations(hf,
                     UINT64_MAX, 0x99856d7c6e9accf9ULL, e::slice("key", 3),
                     UINT64_MAX, 0xdeadbeefcafebabeULL, 0, 0, std::vector<e::slice>(1, e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8)));

    // Key/<Value1,Value2>: hash on value1/value2 (value1 is range)
    hf.resize(3);
    hf[0] = NONE;
    hf[1] = RANGE;
    hf[2] = EQUALITY;
    value.clear();
    value.push_back(e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8));
    value.push_back(e::slice("value2", 6));
    all_permutations(hf,
                     0, 0, e::slice(),
                     UINT64_MAX, 0xfa66ddde456c6dd6ULL, UINT64_MAX, 0xf37ce67b4dd67cf7ULL, value);

    // Key/<Value1,Value2>: hash on value1/value2 (value2 is range)
    hf.resize(3);
    hf[0] = NONE;
    hf[1] = EQUALITY;
    hf[2] = RANGE;
    value.clear();
    value.push_back(e::slice("value1", 6));
    value.push_back(e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8));
    all_permutations(hf,
                     0, 0, e::slice(),
                     UINT64_MAX, 0xa0d9abadcadc9fecULL, UINT64_MAX, 0xe7f8d8a69facb8abULL, value);

    // Key/<Value1,Value2>: hash on value1/value2 (both are range)
    hf.resize(3);
    hf[0] = NONE;
    hf[1] = RANGE;
    hf[2] = RANGE;
    value.clear();
    value.push_back(e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8));
    value.push_back(e::slice("\xbe\xba\xfe\xca\xef\xbe\xad\xde", 8));
    all_permutations(hf,
                     0, 0, e::slice(),
                     UINT64_MAX, 0xf0ccfffccfcccffcULL, UINT64_MAX, 0xf3fcccf3cffcfcffULL, value);
}

} // namespace
