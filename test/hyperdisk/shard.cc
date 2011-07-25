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

// e
#include <e/guard.h>

// HyperDex
#include <hyperdisk/shard.h>

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

TEST(DiskTest, CtorAndDtor)
{
    e::intrusive_ptr<hyperdisk::shard> d = hyperdisk::shard::create("tmp-disk");
    e::guard g = e::makeobjguard(*d, &hyperdisk::shard::drop);
}

TEST(DiskTest, Simple)
{
    e::intrusive_ptr<hyperdisk::shard> d = hyperdisk::shard::create("tmp-disk");
    e::guard g = e::makeobjguard(*d, &hyperdisk::shard::drop);
    std::vector<e::buffer> value;
    std::vector<uint64_t> value_hashes;
    uint64_t version;

    ASSERT_EQ(hyperdex::NOTFOUND, d->get(e::buffer("key", 3), 11062368440904502521UL, &value, &version));
    version = 0xdeadbeefcafebabe;
    ASSERT_EQ(hyperdex::SUCCESS, d->put(e::buffer("key", 3), 11062368440904502521UL, value, value_hashes, version));
    ASSERT_EQ(hyperdex::SUCCESS, d->get(e::buffer("key", 3), 11062368440904502521UL, &value, &version));
    version = 0xdefec8edcafef00d;
    value.clear();
    value.push_back(e::buffer("value", 5));
    value_hashes.clear();
    value_hashes.push_back(17168316521448127654UL);
    ASSERT_EQ(hyperdex::SUCCESS, d->put(e::buffer("key", 3), 11062368440904502521UL, value, value_hashes, version));
    value.clear();
    ASSERT_EQ(hyperdex::SUCCESS, d->get(e::buffer("key", 3), 11062368440904502521UL, &value, &version));
    ASSERT_EQ(1, value.size());
    ASSERT_TRUE(e::buffer("value", 5) == value[0]);
    ASSERT_EQ(hyperdex::SUCCESS, d->del(e::buffer("key", 3), 11062368440904502521UL));
    ASSERT_EQ(hyperdex::NOTFOUND, d->get(e::buffer("key", 3), 11062368440904502521UL, &value, &version));
}

TEST(DiskTest, MultiPut)
{
    e::intrusive_ptr<hyperdisk::shard> d = hyperdisk::shard::create("tmp-disk");
    e::guard g = e::makeobjguard(*d, &hyperdisk::shard::drop);
    std::vector<e::buffer> value;
    std::vector<uint64_t> value_hashes;
    uint64_t version;

    // Put one point.
    version = 64;
    value.push_back(e::buffer("value-one", 9));
    value_hashes.push_back(13379687116905876655UL);
    ASSERT_EQ(hyperdex::SUCCESS, d->put(e::buffer("one", 3), 0xe1001e63b5e57068L, value, value_hashes, version));

    // Put a second point.
    version = 128;
    value.clear();
    value_hashes.clear();
    value.push_back(e::buffer("value-two-a", 11));
    value_hashes.push_back(3863907495625339794);
    value.push_back(e::buffer("value-two-b", 11));
    value_hashes.push_back(1626659109720743906);
    ASSERT_EQ(hyperdex::SUCCESS, d->put(e::buffer("two", 3), 0x4a38a1aea3a81e5f, value, value_hashes, version));

    // Make sure we can get both.
    ASSERT_EQ(hyperdex::SUCCESS, d->get(e::buffer("one", 3), 0xe1001e63b5e57068L, &value, &version));
    ASSERT_EQ(64, version);
    ASSERT_EQ(1, value.size());
    ASSERT_TRUE(e::buffer("value-one", 9) == value[0]);

    ASSERT_EQ(hyperdex::SUCCESS, d->get(e::buffer("two", 3), 0x4a38a1aea3a81e5f, &value, &version));
    ASSERT_EQ(128, version);
    ASSERT_EQ(2, value.size());
    ASSERT_TRUE(e::buffer("value-two-a", 11) == value[0]);
    ASSERT_TRUE(e::buffer("value-two-b", 11) == value[1]);
}

TEST(DiskTest, DelPutDelPut)
{
    e::intrusive_ptr<hyperdisk::shard> d = hyperdisk::shard::create("tmp-disk");
    e::guard g = e::makeobjguard(*d, &hyperdisk::shard::drop);
	e::buffer key("key", 3);
	uint64_t key_hash = 11062368440904502521UL;
    std::vector<e::buffer> value;
    std::vector<uint64_t> value_hashes;
    uint64_t version = 0xdeadbeefcafebabe;

	// Prepare the point.
	value.push_back(e::buffer("value", 5));
	value_hashes.push_back(17168316521448127654UL);

	// Alternate put/delete.
	ASSERT_EQ(hyperdex::NOTFOUND, d->del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d->put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d->del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d->put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d->del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d->put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d->del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d->put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d->del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d->put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d->del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d->put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d->del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d->put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d->del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d->put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d->del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d->put(key, key_hash, value, value_hashes, version));
	ASSERT_EQ(hyperdex::SUCCESS, d->del(key, key_hash));
	ASSERT_EQ(hyperdex::SUCCESS, d->put(key, key_hash, value, value_hashes, version));

    // A GET should succeed.
	ASSERT_EQ(hyperdex::SUCCESS, d->get(key, key_hash, &value, &version));
	ASSERT_EQ(1, value.size());
	ASSERT_TRUE(e::buffer("value", 5) == value[0]);
	ASSERT_EQ(0xdeadbeefcafebabe, version);

	// One last delete.
	ASSERT_EQ(hyperdex::SUCCESS, d->del(key, key_hash));

    // A GET should fail.
	ASSERT_EQ(hyperdex::NOTFOUND, d->get(key, key_hash, &value, &version));
}

TEST(DiskTest, Snapshot)
{
    e::intrusive_ptr<hyperdisk::shard> d = hyperdisk::shard::create("tmp-disk");
    e::guard g = e::makeobjguard(*d, &hyperdisk::shard::drop);
    bool valid = true;

    // Snapshot empty disk.
    e::intrusive_ptr<hyperdisk::shard::snapshot> s1a = d->make_snapshot();
    e::intrusive_ptr<hyperdisk::shard::snapshot> s1b = d->make_snapshot();
    EXPECT_FALSE(s1a->valid());

    // Put one element and snaphot the disk.
	const e::buffer key("key", 3);
	const uint64_t key_hash = 11062368440904502521UL;
    const std::vector<e::buffer> value;
    const std::vector<uint64_t> value_hashes;
    const uint64_t version = 0xdeadbeefcafebabe;
    ASSERT_EQ(hyperdex::SUCCESS, d->put(key, key_hash, value, value_hashes, version));
    e::intrusive_ptr<hyperdisk::shard::snapshot> s2a = d->make_snapshot();
    e::intrusive_ptr<hyperdisk::shard::snapshot> s2b = d->make_snapshot();

    for (int i = 0; i < 1000000; ++i)
    {
        valid &= s2a->valid();
    }

    ASSERT_TRUE(valid);
    EXPECT_EQ(version, s2a->version());
    EXPECT_TRUE(s2a->key() == key);
    EXPECT_TRUE(s2a->value() == value);
    s2a->next();
    valid = false;

    for (int i = 0; i < 1000000; ++i)
    {
        valid |= s2a->valid();
    }

    EXPECT_FALSE(valid);

    // Do a put which overwrites the value.
    const std::vector<e::buffer> value2(1, e::buffer("value", 5));
    const std::vector<uint64_t> value2_hashes(1, 17168316521448127654UL);
    const uint64_t version2 = 0xcafebabedeadbeef;
    ASSERT_EQ(hyperdex::SUCCESS, d->put(key, key_hash, value2, value2_hashes, version2));
    e::intrusive_ptr<hyperdisk::shard::snapshot> s3a = d->make_snapshot();
    e::intrusive_ptr<hyperdisk::shard::snapshot> s3b = d->make_snapshot();
    ASSERT_TRUE(s3a->valid());
    EXPECT_EQ(version2, s3a->version());
    EXPECT_TRUE(s3a->key() == key);
    EXPECT_TRUE(s3a->value() == value2);
    s3a->next();
    EXPECT_FALSE(s3a->valid());

    // Delete that value.
    EXPECT_EQ(hyperdex::SUCCESS, d->del(key, key_hash));
    e::intrusive_ptr<hyperdisk::shard::snapshot> s4a = d->make_snapshot();
    e::intrusive_ptr<hyperdisk::shard::snapshot> s4b = d->make_snapshot();
    EXPECT_FALSE(s4a->valid());

    // Perform back-to-back PUTs.
    const e::buffer b2b_key1("one", 3);
    const uint64_t b2b_key1_hash = 0xe1001e63b5e57068L;
    const uint64_t b2b_version1 = 0x42424242;
    const std::vector<e::buffer> b2b_value1(2, e::buffer("value-1", 7));
    const std::vector<uint64_t> b2b_value1_hashes(2, 2821271447910696549L);
    const e::buffer b2b_key2("two", 3);
    const uint64_t b2b_key2_hash = 0x4a38a1aea3a81e5fL;
    const uint64_t b2b_version2 = 0x41414141;
    const std::vector<e::buffer> b2b_value2(2, e::buffer("value-2", 7));
    const std::vector<uint64_t> b2b_value2_hashes(2, 9047582705417063893L);
    ASSERT_EQ(hyperdex::SUCCESS, d->put(b2b_key1, b2b_key1_hash, b2b_value1, b2b_value1_hashes, b2b_version1));
    ASSERT_EQ(hyperdex::SUCCESS, d->put(b2b_key2, b2b_key2_hash, b2b_value2, b2b_value2_hashes, b2b_version2));
    e::intrusive_ptr<hyperdisk::shard::snapshot> s5a = d->make_snapshot();
    e::intrusive_ptr<hyperdisk::shard::snapshot> s5b = d->make_snapshot();
    ASSERT_TRUE(s5a->valid());
    EXPECT_EQ(b2b_version1, s5a->version());
    EXPECT_TRUE(s5a->key() == b2b_key1);
    EXPECT_TRUE(s5a->value() == b2b_value1);
    s5a->next();
    ASSERT_TRUE(s5a->valid());
    EXPECT_EQ(b2b_version2, s5a->version());
    EXPECT_TRUE(s5a->key() == b2b_key2);
    EXPECT_TRUE(s5a->value() == b2b_value2);
    s5a->next();
    EXPECT_FALSE(s5a->valid());

    // Check snapshots again after other activity.
    // ---
    EXPECT_FALSE(s1b->valid());
    // ---
    ASSERT_TRUE(s2b->valid());
    EXPECT_EQ(version, s2b->version());
    EXPECT_TRUE(s2b->key() == key);
    EXPECT_TRUE(s2b->value() == value);
    s2b->next();
    EXPECT_FALSE(s2b->valid());
    // ---
    ASSERT_TRUE(s3b->valid());
    EXPECT_EQ(s3b->version(), version2);
    EXPECT_TRUE(s3b->key() == key);
    EXPECT_TRUE(s3b->value() == value2);
    s3b->next();
    EXPECT_FALSE(s3b->valid());
    // ---
    EXPECT_FALSE(s4b->valid());
    // ---
    ASSERT_TRUE(s5b->valid());
    EXPECT_EQ(b2b_version1, s5b->version());
    EXPECT_TRUE(s5b->key() == b2b_key1);
    EXPECT_TRUE(s5b->value() == b2b_value1);
    s5b->next();
    ASSERT_TRUE(s5b->valid());
    EXPECT_EQ(b2b_version2, s5b->version());
    EXPECT_TRUE(s5b->key() == b2b_key2);
    EXPECT_TRUE(s5b->value() == b2b_value2);
    s5b->next();
    EXPECT_FALSE(s5b->valid());
}

} // namespace
