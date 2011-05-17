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

// HyperDex
#include <hyperdex/configuration.h>

#pragma GCC diagnostic ignored "-Wswitch-default"

namespace
{

TEST(ConfigurationTest, CtorAndDtor)
{
    hyperdex::configuration c;
}

TEST(ConfigurationTest, SimpleHost)
{
    hyperdex::configuration c;
    EXPECT_TRUE(c.add_line("host\t0xdeadbeef\t127.0.0.1\t6970\t0\t6971\t2"));
    EXPECT_TRUE(c.add_line("host\t0xcafebabe\t127.0.0.1\t6972\t1\t6973\t1"));
    EXPECT_TRUE(c.add_line("host\t0x8badf00d\t127.0.0.1\t6974\t0\t6975\t0"));
    EXPECT_TRUE(c.add_line("host\t0xfee1dead\t127.0.0.1\t6976\t0\t6977\t0"));
    EXPECT_TRUE(c.add_line("space\t0xdefec8ed\tkv\tkey\tvalue"));
    EXPECT_TRUE(c.add_line("subspace\tkv\t0\tkey"));
    EXPECT_TRUE(c.add_line("subspace\tkv\t1\tvalue"));
    EXPECT_TRUE(c.add_line("region\tkv\t0\t2\t0x0000000000000000\t0xdeadbeef"));
    EXPECT_TRUE(c.add_line("region\tkv\t0\t2\t0x4000000000000000\t0xcafebabe"));
    EXPECT_TRUE(c.add_line("region\tkv\t0\t2\t0x8000000000000000\t0x8badf00d"));
    EXPECT_TRUE(c.add_line("region\tkv\t0\t2\t0xc000000000000000\t0xfee1dead"));
    EXPECT_TRUE(c.add_line("region\tkv\t1\t1\t0x0000000000000000\t0xdeadbeef\t0xcafebabe"));
    EXPECT_TRUE(c.add_line("region\tkv\t1\t1\t0x8000000000000000\t0x8badf00d\t0xfee1dead"));

    hyperdex::configuration::instance i_deadbeef(po6::net::location("127.0.0.1", 6970), 0,
                                                 po6::net::location("127.0.0.1", 6971), 2);
    hyperdex::configuration::instance i_cafebabe(po6::net::location("127.0.0.1", 6972), 1,
                                                 po6::net::location("127.0.0.1", 6973), 1);
    hyperdex::configuration::instance i_8badf00d(po6::net::location("127.0.0.1", 6974), 0,
                                                 po6::net::location("127.0.0.1", 6975), 0);
    hyperdex::configuration::instance i_fee1dead(po6::net::location("127.0.0.1", 6976), 0,
                                                 po6::net::location("127.0.0.1", 6977), 0);

    std::map<hyperdex::entity, hyperdex::configuration::instance> expected;
    expected.insert(std::make_pair(hyperdex::entity(0xdefec8ed, 0, 2, 0x0000000000000000, 0),
                                   i_deadbeef));
    expected.insert(std::make_pair(hyperdex::entity(0xdefec8ed, 0, 2, 0x4000000000000000, 0),
                                   i_cafebabe));
    expected.insert(std::make_pair(hyperdex::entity(0xdefec8ed, 0, 2, 0x8000000000000000, 0),
                                   i_8badf00d));
    expected.insert(std::make_pair(hyperdex::entity(0xdefec8ed, 0, 2, 0xc000000000000000, 0),
                                   i_fee1dead));
    expected.insert(std::make_pair(hyperdex::entity(0xdefec8ed, 0, 1, 0x0000000000000000, 0),
                                   i_deadbeef));
    expected.insert(std::make_pair(hyperdex::entity(0xdefec8ed, 0, 1, 0x0000000000000000, 1),
                                   i_cafebabe));
    expected.insert(std::make_pair(hyperdex::entity(0xdefec8ed, 0, 1, 0x8000000000000000, 0),
                                   i_8badf00d));
    EXPECT_FALSE(c.is_complete());
    expected.insert(std::make_pair(hyperdex::entity(0xdefec8ed, 0, 1, 0x8000000000000000, 1),
                                   i_fee1dead));

    EXPECT_TRUE(c.is_complete());
    EXPECT_TRUE(expected == c.entity_mapping());
}

TEST(ConfigurationTest, BadHostLine)
{
    hyperdex::configuration c;
    // Improper arity
    EXPECT_FALSE(c.add_line("host"));
    EXPECT_FALSE(c.add_line("host\t0xdeadbeef"));
    EXPECT_FALSE(c.add_line("host\t0xdeadbeef\t127.0.0.1"));
    EXPECT_FALSE(c.add_line("host\t0xdeadbeef\t127.0.0.1\t6970"));
    EXPECT_FALSE(c.add_line("host\t0xdeadbeef\t127.0.0.1\t6970\t0"));
    EXPECT_FALSE(c.add_line("host\t0xdeadbeef\t127.0.0.1\t6970\t0\t6971"));
    EXPECT_FALSE(c.add_line("host\t0xdeadbeef\t\t6970\t0\t6971\t2"));
    // Bad IP
    EXPECT_FALSE(c.add_line("host\t0xdeadbeef\t127.0.0\t6970\t0\t6971\t2"));
    // Duplicate
    EXPECT_TRUE(c.add_line("host\t0xdeadbeef\t127.0.0.1\t6970\t0\t6971\t2"));
    EXPECT_FALSE(c.add_line("host\t0xdeadbeef\t127.0.0.1\t6970\t0\t6971\t2"));
}

TEST(ConfigurationTest, IPv6Ready)
{
    hyperdex::configuration c;
    EXPECT_TRUE(c.add_line("host\t0xdeadbeef\t::1\t6970\t0\t6971\t2"));
}

TEST(ConfigurationTest, BadSpaceLine)
{
    hyperdex::configuration c;
    EXPECT_TRUE(c.add_line("space\t0xdeadbeef\tmeals\tkey\tval"));
    // The space ID must be unique.
    EXPECT_FALSE(c.add_line("space\t0xdeadbeef\tcafe\tkey\tval"));
    // The space name must be unique.
    EXPECT_FALSE(c.add_line("space\t0xcafebabe\tmeals\tkey\tval"));
    // The space must have at least one column.
    EXPECT_FALSE(c.add_line("space\t0x8badf00d\tset"));
    EXPECT_TRUE(c.add_line("space\t0x8badf00d\tset\tkey"));
}

TEST(ConfigurationTest, BadSubspaceLine)
{
    hyperdex::configuration c;
    EXPECT_TRUE(c.add_line("space\t0xdeadbeef\ttraditional\tkey\tval"));
    // You cannot create subspaces out of order
    EXPECT_FALSE(c.add_line("subspace\ttraditional\t1\tval"));
    // You cannot create subspace 0 with more than one dimension.
    EXPECT_FALSE(c.add_line("subspace\ttraditional\t0\tkey\tval"));
    EXPECT_TRUE(c.add_line("subspace\ttraditional\t0\tkey"));
    // You still cannot create subspaces out of order
    EXPECT_FALSE(c.add_line("subspace\ttraditional\t2\tkey\tval"));
    EXPECT_TRUE(c.add_line("subspace\ttraditional\t1\tval"));
    // You cannot create subspaces for which the space doesn't exist.
    EXPECT_FALSE(c.add_line("subspace\tset\t0\tkey"));
    // You cannot create subspaces which already exist
    EXPECT_FALSE(c.add_line("subspace\ttraditional\t1\val"));
    // You cannot create subspaces which do not match the columns of the space.
    EXPECT_FALSE(c.add_line("subspace\ttraditional\t2\tname"));
}

TEST(ConfigurationTest, BadRegionLine)
{
    hyperdex::configuration c;
    EXPECT_TRUE(c.add_line("host\t0xdeadbeef\t127.0.0.1\t6970\t0\t6971\t2"));
    EXPECT_TRUE(c.add_line("space\t0xdeadbeef\tset\titem"));
    EXPECT_TRUE(c.add_line("subspace\tset\t0\titem"));
    // Regions must reference spaces.
    EXPECT_FALSE(c.add_line("region\tnonspace\t0\t0\t0x0000000000000000\t0xdeadbeef"));
    // Regions must reference subspaces.
    EXPECT_FALSE(c.add_line("region\tset\t1\t0\t0x0000000000000000\t0xdeadbeef"));
    // Regions must reference hosts.
    EXPECT_FALSE(c.add_line("region\tset\t0\t0\t0x0000000000000000\t0xfacefeed"));
}

} // namespace
