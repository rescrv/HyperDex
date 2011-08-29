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
#include <fcntl.h>
#include <unistd.h>

// Google Test
#include <gtest/gtest.h>

// e
#include <e/guard.h>

// HyperDisk
#include "shard.h"
#include "shard_snapshot.h"

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

TEST(ShardTest, CtorAndDtor)
{
    po6::io::fd cwd(AT_FDCWD);
    e::intrusive_ptr<hyperdisk::shard> d = hyperdisk::shard::create(cwd, "tmp-disk");
    e::guard g = e::makeguard(::unlink, "tmp-disk");
}

TEST(ShardTest, Simple)
{
    po6::io::fd cwd(AT_FDCWD);
    e::intrusive_ptr<hyperdisk::shard> d = hyperdisk::shard::create(cwd, "tmp-disk");
    e::guard g = e::makeguard(::unlink, "tmp-disk");
    std::vector<e::buffer> value;
    uint64_t version;

    ASSERT_EQ(hyperdisk::NOTFOUND, d->get(0x6e9accf9UL, e::buffer("key", 3), &value, &version));
    version = 0xdeadbeefcafebabe;
    ASSERT_EQ(hyperdisk::SUCCESS, d->put(0x6e9accf9UL, 0, e::buffer("key", 3), value, version));
    ASSERT_EQ(hyperdisk::SUCCESS, d->get(0x6e9accf9UL, e::buffer("key", 3), &value, &version));
    version = 0xdefec8edcafef00d;

    value.clear();
    value.push_back(e::buffer("value", 5));
    ASSERT_EQ(hyperdisk::SUCCESS, d->put(0x6e9accf9UL, 0x2462bca6UL, e::buffer("key", 3), value, version));
    value.clear();
    ASSERT_EQ(hyperdisk::SUCCESS, d->get(0x6e9accf9UL, e::buffer("key", 3), &value, &version));
    ASSERT_EQ(1, value.size());
    ASSERT_TRUE(e::buffer("value", 5) == value[0]);
    ASSERT_EQ(hyperdisk::SUCCESS, d->del(0x6e9accf9UL, e::buffer("key", 3)));
    ASSERT_EQ(hyperdisk::NOTFOUND, d->get(0x6e9accf9UL, e::buffer("key", 3), &value, &version));
}

TEST(ShardTest, MultiPut)
{
    po6::io::fd cwd(AT_FDCWD);
    e::intrusive_ptr<hyperdisk::shard> d = hyperdisk::shard::create(cwd, "tmp-disk");
    e::guard g = e::makeguard(::unlink, "tmp-disk");
    std::vector<e::buffer> value;
    uint64_t version;

    // Put one point.
    version = 64;
    value.push_back(e::buffer("value-one", 9));
    ASSERT_EQ(hyperdisk::SUCCESS, d->put(0xb5e57068UL, 0x510b8cafUL, e::buffer("one", 3), value, version));

    // Put a second point.
    version = 128;
    value.clear();
    value.push_back(e::buffer("value-two-a", 11));
    value.push_back(e::buffer("value-two-b", 11));
    ASSERT_EQ(hyperdisk::SUCCESS, d->put(0xa3a81e5fUL, 0xf3ebf849UL, e::buffer("two", 3), value, version));

    // Make sure we can get both.
    ASSERT_EQ(hyperdisk::SUCCESS, d->get(0xb5e57068UL, e::buffer("one", 3), &value, &version));
    ASSERT_EQ(64, version);
    ASSERT_EQ(1, value.size());
    ASSERT_TRUE(e::buffer("value-one", 9) == value[0]);

    ASSERT_EQ(hyperdisk::SUCCESS, d->get(0xa3a81e5fUL, e::buffer("two", 3), &value, &version));
    ASSERT_EQ(128, version);
    ASSERT_EQ(2, value.size());
    ASSERT_TRUE(e::buffer("value-two-a", 11) == value[0]);
    ASSERT_TRUE(e::buffer("value-two-b", 11) == value[1]);
}

TEST(ShardTest, DelPutDelPut)
{
    po6::io::fd cwd(AT_FDCWD);
    e::intrusive_ptr<hyperdisk::shard> d = hyperdisk::shard::create(cwd, "tmp-disk");
    e::guard g = e::makeguard(::unlink, "tmp-disk");
	e::buffer key("key", 3);
	uint32_t primary_hash = 0x6e9accf9UL;
    std::vector<e::buffer> value;
	value.push_back(e::buffer("value", 5));
    uint64_t version = 0xdeadbeefcafebabe;

	// Alternate put/delete.
	ASSERT_EQ(hyperdisk::NOTFOUND, d->del(primary_hash, key));
	ASSERT_EQ(hyperdisk::SUCCESS, d->put(primary_hash, 0x2462bca6UL, key, value, version));
	ASSERT_EQ(hyperdisk::SUCCESS, d->del(primary_hash, key));
	ASSERT_EQ(hyperdisk::SUCCESS, d->put(primary_hash, 0x2462bca6UL, key, value, version));
	ASSERT_EQ(hyperdisk::SUCCESS, d->del(primary_hash, key));
	ASSERT_EQ(hyperdisk::SUCCESS, d->put(primary_hash, 0x2462bca6UL, key, value, version));
	ASSERT_EQ(hyperdisk::SUCCESS, d->del(primary_hash, key));
	ASSERT_EQ(hyperdisk::SUCCESS, d->put(primary_hash, 0x2462bca6UL, key, value, version));
	ASSERT_EQ(hyperdisk::SUCCESS, d->del(primary_hash, key));
	ASSERT_EQ(hyperdisk::SUCCESS, d->put(primary_hash, 0x2462bca6UL, key, value, version));
	ASSERT_EQ(hyperdisk::SUCCESS, d->del(primary_hash, key));
	ASSERT_EQ(hyperdisk::SUCCESS, d->put(primary_hash, 0x2462bca6UL, key, value, version));
	ASSERT_EQ(hyperdisk::SUCCESS, d->del(primary_hash, key));
	ASSERT_EQ(hyperdisk::SUCCESS, d->put(primary_hash, 0x2462bca6UL, key, value, version));
	ASSERT_EQ(hyperdisk::SUCCESS, d->del(primary_hash, key));
	ASSERT_EQ(hyperdisk::SUCCESS, d->put(primary_hash, 0x2462bca6UL, key, value, version));
	ASSERT_EQ(hyperdisk::SUCCESS, d->del(primary_hash, key));
	ASSERT_EQ(hyperdisk::SUCCESS, d->put(primary_hash, 0x2462bca6UL, key, value, version));
	ASSERT_EQ(hyperdisk::SUCCESS, d->del(primary_hash, key));
	ASSERT_EQ(hyperdisk::SUCCESS, d->put(primary_hash, 0x2462bca6UL, key, value, version));

    // A GET should succeed.
	ASSERT_EQ(hyperdisk::SUCCESS, d->get(primary_hash, key, &value, &version));
	ASSERT_EQ(1, value.size());
	ASSERT_TRUE(e::buffer("value", 5) == value[0]);
	ASSERT_EQ(0xdeadbeefcafebabe, version);

	// One last delete.
	ASSERT_EQ(hyperdisk::SUCCESS, d->del(primary_hash, key));

    // A GET should fail.
	ASSERT_EQ(hyperdisk::NOTFOUND, d->get(primary_hash, key, &value, &version));
}

TEST(ShardTest, Snapshot)
{
    po6::io::fd cwd(AT_FDCWD);
    e::intrusive_ptr<hyperdisk::shard> d = hyperdisk::shard::create(cwd, "tmp-disk");
    e::guard g = e::makeguard(::unlink, "tmp-disk");
    bool valid = true;

    // Snapshot empty disk.
    e::intrusive_ptr<hyperdisk::shard_snapshot> s1a = d->make_snapshot();
    e::intrusive_ptr<hyperdisk::shard_snapshot> s1b = d->make_snapshot();
    EXPECT_FALSE(s1a->valid());

    // Put one element and snaphot the disk.
	const e::buffer key("key", 3);
    const uint32_t primary_hash = 0x6e9accf9UL;
    const std::vector<e::buffer> value;
    const uint64_t version = 0xdeadbeefcafebabe;
    ASSERT_EQ(hyperdisk::SUCCESS, d->put(primary_hash, 0, key, value, version));
    e::intrusive_ptr<hyperdisk::shard_snapshot> s2a = d->make_snapshot();
    e::intrusive_ptr<hyperdisk::shard_snapshot> s2b = d->make_snapshot();

    for (int i = 0; i < 1000000; ++i)
    {
        valid &= s2a->valid();
    }

    ASSERT_TRUE(valid);
    EXPECT_EQ(primary_hash, s2a->primary_hash());
    EXPECT_EQ(0, s2a->secondary_hash());
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
    const uint64_t version2 = 0xcafebabedeadbeef;
    ASSERT_EQ(hyperdisk::SUCCESS, d->put(primary_hash, 0x2462bca6UL, key, value2, version2));
    e::intrusive_ptr<hyperdisk::shard_snapshot> s3a = d->make_snapshot();
    e::intrusive_ptr<hyperdisk::shard_snapshot> s3b = d->make_snapshot();
    ASSERT_TRUE(s3a->valid());
    EXPECT_EQ(primary_hash, s3a->primary_hash());
    EXPECT_EQ(0x2462bca6UL, s3a->secondary_hash());
    EXPECT_EQ(version2, s3a->version());
    EXPECT_TRUE(s3a->key() == key);
    EXPECT_TRUE(s3a->value() == value2);
    s3a->next();
    EXPECT_FALSE(s3a->valid());

    // Delete that value.
    EXPECT_EQ(hyperdisk::SUCCESS, d->del(primary_hash, key));
    e::intrusive_ptr<hyperdisk::shard_snapshot> s4a = d->make_snapshot();
    e::intrusive_ptr<hyperdisk::shard_snapshot> s4b = d->make_snapshot();
    EXPECT_FALSE(s4a->valid());

    // Perform back-to-back PUTs.
    const e::buffer b2b_key1("one", 3);
    const uint64_t b2b_version1 = 0x42424242;
    const std::vector<e::buffer> b2b_value1(2, e::buffer("value-1", 7));
    const e::buffer b2b_key2("two", 3);
    const uint64_t b2b_version2 = 0x41414141;
    const std::vector<e::buffer> b2b_value2(2, e::buffer("value-2", 7));
    ASSERT_EQ(hyperdisk::SUCCESS, d->put(0xb5e57068UL, 0x303c3c33UL, b2b_key1, b2b_value1, b2b_version1));
    ASSERT_EQ(hyperdisk::SUCCESS, d->put(0xa3a81e5fUL, 0xf3f333UL, b2b_key2, b2b_value2, b2b_version2));
    e::intrusive_ptr<hyperdisk::shard_snapshot> s5a = d->make_snapshot();
    e::intrusive_ptr<hyperdisk::shard_snapshot> s5b = d->make_snapshot();
    ASSERT_TRUE(s5a->valid());
    EXPECT_EQ(0xb5e57068UL, s5a->primary_hash());
    EXPECT_EQ(0x303c3c33UL, s5a->secondary_hash());
    EXPECT_EQ(b2b_version1, s5a->version());
    EXPECT_TRUE(s5a->key() == b2b_key1);
    EXPECT_TRUE(s5a->value() == b2b_value1);
    s5a->next();
    ASSERT_TRUE(s5a->valid());
    EXPECT_EQ(0xa3a81e5fUL, s5a->primary_hash());
    EXPECT_EQ(0xf3f333UL, s5a->secondary_hash());
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
    EXPECT_EQ(primary_hash, s2b->primary_hash());
    EXPECT_EQ(0, s2b->secondary_hash());
    EXPECT_EQ(version, s2b->version());
    EXPECT_TRUE(s2b->key() == key);
    EXPECT_TRUE(s2b->value() == value);
    s2b->next();
    EXPECT_FALSE(s2b->valid());
    // ---
    ASSERT_TRUE(s3b->valid());
    EXPECT_EQ(primary_hash, s3b->primary_hash());
    EXPECT_EQ(0x2462bca6UL, s3b->secondary_hash());
    EXPECT_EQ(s3b->version(), version2);
    EXPECT_TRUE(s3b->key() == key);
    EXPECT_TRUE(s3b->value() == value2);
    s3b->next();
    EXPECT_FALSE(s3b->valid());
    // ---
    EXPECT_FALSE(s4b->valid());
    // ---
    ASSERT_TRUE(s5b->valid());
    EXPECT_EQ(0xb5e57068UL, s5b->primary_hash());
    EXPECT_EQ(0x303c3c33UL, s5b->secondary_hash());
    EXPECT_EQ(b2b_version1, s5b->version());
    EXPECT_TRUE(s5b->key() == b2b_key1);
    EXPECT_TRUE(s5b->value() == b2b_value1);
    s5b->next();
    ASSERT_TRUE(s5b->valid());
    EXPECT_EQ(0xa3a81e5fUL, s5b->primary_hash());
    EXPECT_EQ(0xf3f333UL, s5b->secondary_hash());
    EXPECT_EQ(b2b_version2, s5b->version());
    EXPECT_TRUE(s5b->key() == b2b_key2);
    EXPECT_TRUE(s5b->value() == b2b_value2);
    s5b->next();
    EXPECT_FALSE(s5b->valid());
}

} // namespace
