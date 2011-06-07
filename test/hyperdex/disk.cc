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

// POSIX
#include <unistd.h>

// Google Test
#include <gtest/gtest.h>

// HyperDex
#include <hyperdex/disk.h>

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

TEST(DiskTest, CtorAndDtor)
{
    hyperdex::zero_fill("tmp-disk");
    hyperdex::disk d("tmp-disk");
    unlink("tmp-disk");
}

TEST(DiskTest, Simple)
{
    hyperdex::zero_fill("tmp-disk");
    hyperdex::disk d("tmp-disk");
    unlink("tmp-disk");
    std::vector<e::buffer> value;
    std::vector<uint64_t> value_hashes;
    uint64_t version;

    ASSERT_EQ(hyperdex::NOTFOUND, d.get(e::buffer("key", 3), 11062368440904502521UL, &value, &version));
    version = 0xdeadbeefcafebabe;
    ASSERT_EQ(hyperdex::SUCCESS, d.put(e::buffer("key", 3), 11062368440904502521UL, value, value_hashes, version));
    ASSERT_EQ(hyperdex::SUCCESS, d.get(e::buffer("key", 3), 11062368440904502521UL, &value, &version));
    version = 0xdefec8edcafef00d;
    value.clear();
    value.push_back(e::buffer("value", 5));
    value_hashes.clear();
    value_hashes.push_back(17168316521448127654UL);
    ASSERT_EQ(hyperdex::SUCCESS, d.put(e::buffer("key", 3), 11062368440904502521UL, value, value_hashes, version));
    value.clear();
    ASSERT_EQ(hyperdex::SUCCESS, d.get(e::buffer("key", 3), 11062368440904502521UL, &value, &version));
    ASSERT_EQ(1, value.size());
    ASSERT_TRUE(e::buffer("value", 5) == value[0]);
    ASSERT_EQ(hyperdex::SUCCESS, d.del(e::buffer("key", 3), 11062368440904502521UL));
    ASSERT_EQ(hyperdex::NOTFOUND, d.get(e::buffer("key", 3), 11062368440904502521UL, &value, &version));
}

TEST(DiskTest, MultiPut)
{
    hyperdex::zero_fill("tmp-disk");
    hyperdex::disk d("tmp-disk");
    unlink("tmp-disk");
    std::vector<e::buffer> value;
    std::vector<uint64_t> value_hashes;
    uint64_t version;

    // Put one point.
    version = 64;
    value.push_back(e::buffer("value-one", 9));
    value_hashes.push_back(13379687116905876655UL);
    ASSERT_EQ(hyperdex::SUCCESS, d.put(e::buffer("one", 3), 0xe1001e63b5e57068L, value, value_hashes, version));

    // Put a second point.
    version = 128;
    value.clear();
    value_hashes.clear();
    value.push_back(e::buffer("value-two-a", 11));
    value_hashes.push_back(3863907495625339794);
    value.push_back(e::buffer("value-two-b", 11));
    value_hashes.push_back(1626659109720743906);
    ASSERT_EQ(hyperdex::SUCCESS, d.put(e::buffer("two", 3), 0x4a38a1aea3a81e5f, value, value_hashes, version));

    // Make sure we can get both.
    ASSERT_EQ(hyperdex::SUCCESS, d.get(e::buffer("one", 3), 0xe1001e63b5e57068L, &value, &version));
    ASSERT_EQ(64, version);
    ASSERT_EQ(1, value.size());
    ASSERT_TRUE(e::buffer("value-one", 9) == value[0]);

    ASSERT_EQ(hyperdex::SUCCESS, d.get(e::buffer("two", 3), 0x4a38a1aea3a81e5f, &value, &version));
    ASSERT_EQ(128, version);
    ASSERT_EQ(2, value.size());
    ASSERT_TRUE(e::buffer("value-two-a", 11) == value[0]);
    ASSERT_TRUE(e::buffer("value-two-b", 11) == value[1]);
}

TEST(DiskTest, DelPutDelPut)
{
    hyperdex::zero_fill("tmp-disk");
    hyperdex::disk d("tmp-disk");
    unlink("tmp-disk");
	e::buffer key("key", 3);
	uint64_t key_hash = 11062368440904502521UL;
    std::vector<e::buffer> value;
    std::vector<uint64_t> value_hashes;
    uint64_t version = 0xdeadbeefcafebabe;

	// Prepare the point.
	value.push_back(e::buffer("value", 5));
	value_hashes.push_back(17168316521448127654UL);

	// Alternate put/delete.
	ASSERT_EQ(hyperdex::NOTFOUND, d.del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d.put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d.del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d.put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d.del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d.put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d.del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d.put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d.del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d.put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d.del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d.put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d.del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d.put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d.del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d.put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d.del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d.put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d.del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d.put(key, key_hash, value, value_hashes, version));

    // A GET should succeed.
	ASSERT_EQ(hyperdex::SUCCESS, d.get(key, key_hash, &value, &version));
	ASSERT_EQ(1, value.size());
	ASSERT_TRUE(e::buffer("value", 5) == value[0]);
	ASSERT_EQ(0xdeadbeefcafebabe, version);

	// One last delete.
	ASSERT_EQ(hyperdex::SUCCESS, d.del(key, key_hash));

    // A GET should fail.
	ASSERT_EQ(hyperdex::NOTFOUND, d.get(key, key_hash, &value, &version));
}

} // namespace
