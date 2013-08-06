// Copyright (c) 2013, Cornell University
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

// C
#include <cstring>

// HyperDex
#include <hyperdex/datastructures.h>
#include "test/th.h"

TEST(ClientDataStructures, ArenaCtorDtor)
{
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, AllocateAttribute)
{
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_client_attribute* attrs;
    attrs = hyperdex_ds_allocate_attribute(a, 10);

    for (size_t i = 0; i < 10; ++i)
    {
        // we don't care what it gets filled with, this just tests that we don't
        // overrun the allocated space.
        attrs[i].attr = "FOOBAR";
        attrs[i].value = NULL;
        attrs[i].value_sz = 0;
        attrs[i].datatype = HYPERDATATYPE_GENERIC;
    }

    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, AllocateAttributeCheck)
{
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_client_attribute_check* checks;
    checks = hyperdex_ds_allocate_attribute_check(a, 10);

    for (size_t i = 0; i < 10; ++i)
    {
        // we don't care what it gets filled with, this just tests that we don't
        // overrun the allocated space.
        checks[i].attr = "FOOBAR";
        checks[i].value = NULL;
        checks[i].value_sz = 0;
        checks[i].datatype = HYPERDATATYPE_GENERIC;
        checks[i].predicate = HYPERPREDICATE_FAIL;
    }

    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, AllocateMapAttribute)
{
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_client_map_attribute* mapattrs;
    mapattrs = hyperdex_ds_allocate_map_attribute(a, 10);

    for (size_t i = 0; i < 10; ++i)
    {
        // we don't care what it gets filled with, this just tests that we don't
        // overrun the allocated space.
        mapattrs[i].attr = "FOOBAR";
        mapattrs[i].map_key = NULL;
        mapattrs[i].map_key_sz = 0;
        mapattrs[i].map_key_datatype = HYPERDATATYPE_GENERIC;
        mapattrs[i].value = NULL;
        mapattrs[i].value_sz = 0;
        mapattrs[i].value_datatype = HYPERDATATYPE_GENERIC;
    }

    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, PackInt)
{
    char buf[sizeof(int64_t)];
    hyperdex_ds_pack_int(0, buf);
    ASSERT_TRUE(memcmp(buf, "\x00\x00\x00\x00\x00\x00\x00\x00", 8) == 0);
    hyperdex_ds_pack_int(0xdeadbeef, buf);
    ASSERT_TRUE(memcmp(buf, "\xef\xbe\xad\xde\x00\x00\x00\x00", 8) == 0);
}

TEST(ClientDataStructures, UnpackInt)
{
    int64_t num;
    ASSERT_TRUE(hyperdex_ds_unpack_int("\x00\x00\x00\x00\x00\x00\x00\x00", 8, &num) == 0);
    ASSERT_EQ(num, 0);
    ASSERT_TRUE(hyperdex_ds_unpack_int("\xef\xbe\xad\xde\x00\x00\x00\x00", 8, &num) == 0);
    ASSERT_EQ(num, 0xdeadbeef);
    num = 0x8badf00d;
    ASSERT_TRUE(hyperdex_ds_unpack_int("\xef\xbe\xad\xde\x00\x00\x00\x00", 7, &num) < 0);
    ASSERT_EQ(num, 0x8badf00d);
}

TEST(ClientDataStructures, PackFloat)
{
    double num = 9006104071832581.0;
    char buf[sizeof(double)];
    hyperdex_ds_pack_float(num, buf);
    ASSERT_TRUE(memcmp(buf, "\x05\x04\x03\x02\x01\xff\x3f\x43", 8) == 0);
}

TEST(ClientDataStructures, UnpackFloat)
{
    double num;
    ASSERT_TRUE(hyperdex_ds_unpack_float("\x05\x04\x03\x02\x01\xff\x3f\x43", 8, &num) == 0);
    ASSERT_GE(num, 9006104071832580.9);
    ASSERT_LE(num, 9006104071832581.1);
}

TEST(ClientDataStructures, CopyString)
{
    char buf[6] = {'h', 'e', 'l', 'l', 'o', '\x00'};
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_ds_returncode status;
    const char* tmp = NULL;
    size_t tmp_sz = 0;

    ASSERT_TRUE(hyperdex_ds_copy_string(a, buf, 6,
                                        &status,
                                        &tmp, &tmp_sz) == 0);
    ASSERT_EQ(status, HYPERDEX_DS_SUCCESS);
    ASSERT_TRUE(tmp_sz == 6);
    ASSERT_TRUE(strncmp(tmp, "hello", 6) == 0);
    memmove(buf, "world\x00", 6);
    ASSERT_TRUE(strncmp(tmp, "hello", 6) == 0);

    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, CopyInt)
{
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_ds_returncode status;
    const char* tmp = NULL;
    size_t tmp_sz = 0;

    ASSERT_TRUE(hyperdex_ds_copy_int(a, 0xdeadbeef,
                                     &status,
                                     &tmp, &tmp_sz) == 0);
    ASSERT_EQ(status, HYPERDEX_DS_SUCCESS);
    ASSERT_TRUE(tmp_sz == 8);
    ASSERT_TRUE(memcmp(tmp, "\xef\xbe\xad\xde\x00\x00\x00\x00", 8) == 0);

    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, AllocateList)
{
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_ds_list* list = hyperdex_ds_allocate_list(a);
    list = list; // no more warnings
    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, ListString)
{
    hyperdex_ds_returncode status;
    const char* tmp = NULL;
    size_t tmp_sz = 0;
    hyperdatatype datatype;
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_ds_list* list;

    // example 1
    list = hyperdex_ds_allocate_list(a);
    ASSERT_TRUE(hyperdex_ds_list_finalize(list, &status,
                                          &tmp, &tmp_sz,
                                          &datatype) == 0);
    ASSERT_EQ(tmp_sz, 0UL);
    ASSERT_EQ(datatype, HYPERDATATYPE_LIST_GENERIC);
    // example 2
    list = hyperdex_ds_allocate_list(a);
    ASSERT_TRUE(hyperdex_ds_list_append_string(list, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_list_append_string(list, "world", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_list_finalize(list, &status,
                                          &tmp, &tmp_sz,
                                          &datatype) == 0);
    ASSERT_EQ(tmp_sz, 18UL);
    ASSERT_TRUE(memcmp(tmp, "\x05\x00\x00\x00hello\x05\x00\x00\x00world", 18) == 0);
    ASSERT_EQ(datatype, HYPERDATATYPE_LIST_STRING);
    // error on cross-ds insert
    list = hyperdex_ds_allocate_list(a);
    ASSERT_TRUE(hyperdex_ds_list_append_string(list, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_list_append_int(list, 0, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    list = hyperdex_ds_allocate_list(a);
    ASSERT_TRUE(hyperdex_ds_list_append_string(list, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_list_append_float(list, 0., &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);

    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, ListInt)
{
    hyperdex_ds_returncode status;
    const char* tmp = NULL;
    size_t tmp_sz = 0;
    hyperdatatype datatype;
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_ds_list* list;

    // example 1
    list = hyperdex_ds_allocate_list(a);
    ASSERT_TRUE(hyperdex_ds_list_finalize(list, &status,
                                          &tmp, &tmp_sz,
                                          &datatype) == 0);
    ASSERT_EQ(tmp_sz, 0UL);
    ASSERT_EQ(datatype, HYPERDATATYPE_LIST_GENERIC);
    // example 2
    list = hyperdex_ds_allocate_list(a);
    ASSERT_TRUE(hyperdex_ds_list_append_int(list, 1, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_list_append_int(list, -1, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_list_append_int(list, 0xdeadbeef, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_list_finalize(list, &status,
                                          &tmp, &tmp_sz,
                                          &datatype) == 0);
    ASSERT_EQ(tmp_sz, 24UL);
    ASSERT_TRUE(memcmp(tmp, "\x01\x00\x00\x00\x00\x00\x00\x00"
                            "\xff\xff\xff\xff\xff\xff\xff\xff"
                            "\xef\xbe\xad\xde\x00\x00\x00\x00", 24) == 0);
    ASSERT_EQ(datatype, HYPERDATATYPE_LIST_INT64);
    // error on cross-ds insert
    list = hyperdex_ds_allocate_list(a);
    ASSERT_TRUE(hyperdex_ds_list_append_int(list, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_list_append_string(list, "hello", 5, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    list = hyperdex_ds_allocate_list(a);
    ASSERT_TRUE(hyperdex_ds_list_append_int(list, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_list_append_float(list, 0., &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);

    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, ListFloat)
{
    hyperdex_ds_returncode status;
    const char* tmp = NULL;
    size_t tmp_sz = 0;
    hyperdatatype datatype;
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_ds_list* list;

    // example 1
    list = hyperdex_ds_allocate_list(a);
    ASSERT_TRUE(hyperdex_ds_list_finalize(list, &status,
                                          &tmp, &tmp_sz,
                                          &datatype) == 0);
    ASSERT_EQ(tmp_sz, 0UL);
    ASSERT_EQ(datatype, HYPERDATATYPE_LIST_GENERIC);
    // example 2
    list = hyperdex_ds_allocate_list(a);
    ASSERT_TRUE(hyperdex_ds_list_append_float(list, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_list_append_float(list, 3.1415, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_list_finalize(list, &status,
                                          &tmp, &tmp_sz,
                                          &datatype) == 0);
    ASSERT_EQ(tmp_sz, 16UL);
    ASSERT_TRUE(memcmp(tmp, "\x00\x00\x00\x00\x00\x00\x00\x00"
                            "o\x12\x83\xc0\xca!\t@", 16) == 0);
    ASSERT_EQ(datatype, HYPERDATATYPE_LIST_FLOAT);
    // error on cross-ds insert
    list = hyperdex_ds_allocate_list(a);
    ASSERT_TRUE(hyperdex_ds_list_append_float(list, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_list_append_string(list, "hello", 5, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    list = hyperdex_ds_allocate_list(a);
    ASSERT_TRUE(hyperdex_ds_list_append_float(list, 0., &status) == 0);
    ASSERT_TRUE(hyperdex_ds_list_append_int(list, 0, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);

    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, SetString)
{
    hyperdex_ds_returncode status;
    const char* tmp = NULL;
    size_t tmp_sz = 0;
    hyperdatatype datatype;
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_ds_set* set;

    // example 1
    set = hyperdex_ds_allocate_set(a);
    ASSERT_TRUE(hyperdex_ds_set_finalize(set, &status,
                                         &tmp, &tmp_sz,
                                         &datatype) == 0);
    ASSERT_EQ(tmp_sz, 0UL);
    ASSERT_EQ(datatype, HYPERDATATYPE_SET_GENERIC);
    // example 2
    set = hyperdex_ds_allocate_set(a);
    ASSERT_TRUE(hyperdex_ds_set_insert_string(set, "world", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_set_insert_string(set, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_set_finalize(set, &status,
                                         &tmp, &tmp_sz,
                                         &datatype) == 0);
    ASSERT_EQ(tmp_sz, 18UL);
    ASSERT_TRUE(memcmp(tmp, "\x05\x00\x00\x00hello\x05\x00\x00\x00world", 16) == 0);
    ASSERT_EQ(datatype, HYPERDATATYPE_SET_STRING);
    // error on cross-ds insert
    set = hyperdex_ds_allocate_set(a);
    ASSERT_TRUE(hyperdex_ds_set_insert_string(set, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_set_insert_int(set, 0, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    set = hyperdex_ds_allocate_set(a);
    ASSERT_TRUE(hyperdex_ds_set_insert_string(set, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_set_insert_float(set, 0., &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);

    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, SetInt)
{
    hyperdex_ds_returncode status;
    const char* tmp = NULL;
    size_t tmp_sz = 0;
    hyperdatatype datatype;
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_ds_set* set;

    // example 1
    set = hyperdex_ds_allocate_set(a);
    ASSERT_TRUE(hyperdex_ds_set_finalize(set, &status,
                                         &tmp, &tmp_sz,
                                         &datatype) == 0);
    ASSERT_EQ(tmp_sz, 0UL);
    ASSERT_EQ(datatype, HYPERDATATYPE_SET_GENERIC);
    // example 2
    set = hyperdex_ds_allocate_set(a);
    ASSERT_TRUE(hyperdex_ds_set_insert_int(set, 1, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_set_insert_int(set, -1, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_set_insert_int(set, 0xdeadbeef, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_set_finalize(set, &status,
                                         &tmp, &tmp_sz,
                                         &datatype) == 0);
    ASSERT_EQ(tmp_sz, 24UL);
    ASSERT_TRUE(memcmp(tmp, "\xff\xff\xff\xff\xff\xff\xff\xff"
                            "\x01\x00\x00\x00\x00\x00\x00\x00"
                            "\xef\xbe\xad\xde\x00\x00\x00\x00", 24) == 0);
    ASSERT_EQ(datatype, HYPERDATATYPE_SET_INT64);
    // error on cross-ds insert
    set = hyperdex_ds_allocate_set(a);
    ASSERT_TRUE(hyperdex_ds_set_insert_int(set, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_set_insert_string(set, "hello", 5, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    set = hyperdex_ds_allocate_set(a);
    ASSERT_TRUE(hyperdex_ds_set_insert_int(set, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_set_insert_float(set, 0., &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);

    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, SetFloat)
{
    hyperdex_ds_returncode status;
    const char* tmp = NULL;
    size_t tmp_sz = 0;
    hyperdatatype datatype;
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_ds_set* set;

    // example 1
    set = hyperdex_ds_allocate_set(a);
    ASSERT_TRUE(hyperdex_ds_set_finalize(set, &status,
                                         &tmp, &tmp_sz,
                                         &datatype) == 0);
    ASSERT_EQ(tmp_sz, 0UL);
    ASSERT_EQ(datatype, HYPERDATATYPE_SET_GENERIC);
    // example 2
    set = hyperdex_ds_allocate_set(a);
    ASSERT_TRUE(hyperdex_ds_set_insert_float(set, 3.1415, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_set_insert_float(set, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_set_finalize(set, &status,
                                         &tmp, &tmp_sz,
                                         &datatype) == 0);
    ASSERT_EQ(tmp_sz, 16UL);
    ASSERT_TRUE(memcmp(tmp, "\x00\x00\x00\x00\x00\x00\x00\x00"
                            "o\x12\x83\xc0\xca!\t@", 16) == 0);
    ASSERT_EQ(datatype, HYPERDATATYPE_SET_FLOAT);
    // error on cross-ds insert
    set = hyperdex_ds_allocate_set(a);
    ASSERT_TRUE(hyperdex_ds_set_insert_float(set, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_set_insert_string(set, "hello", 5, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    set = hyperdex_ds_allocate_set(a);
    ASSERT_TRUE(hyperdex_ds_set_insert_float(set, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_set_insert_int(set, 0, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);

    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, MapStringString)
{
    hyperdex_ds_returncode status;
    const char* tmp = NULL;
    size_t tmp_sz = 0;
    hyperdatatype datatype;
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_ds_map* map;

    // example 1
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_finalize(map, &status,
                                         &tmp, &tmp_sz,
                                         &datatype) == 0);
    ASSERT_EQ(tmp_sz, 0UL);
    ASSERT_EQ(datatype, HYPERDATATYPE_MAP_GENERIC);
    // example 2
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_string(map, "world", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "map key", 7, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_string(map, "map val", 7, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "map", 3, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_string(map, "encoding", 8, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_finalize(map, &status,
                                         &tmp, &tmp_sz,
                                         &datatype) == 0);
    ASSERT_EQ(tmp_sz, 59UL);
    ASSERT_TRUE(memcmp(tmp, "\x05\x00\x00\x00hello\x05\x00\x00\x00world"
                            "\x03\x00\x00\x00map\x08\x00\x00\x00""encoding"
                            "\x07\x00\x00\x00map key\x07\x00\x00\x00map val", 59) == 0);
    ASSERT_EQ(datatype, HYPERDATATYPE_MAP_STRING_STRING);
    // double key insert
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_WRONG_STATE);
    // double value insert
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_string(map, "hello", 5, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_WRONG_STATE);
    // error on cross-ds insert
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_string(map, "world", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_int(map, 0, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_string(map, "world", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "map key", 7, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_int(map, 0, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_string(map, "world", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_float(map, 0., &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_string(map, "world", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "map key", 7, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_float(map, 0., &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);

    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, MapStringInt)
{
    hyperdex_ds_returncode status;
    const char* tmp = NULL;
    size_t tmp_sz = 0;
    hyperdatatype datatype;
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_ds_map* map;

    // example 1
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_finalize(map, &status,
                                         &tmp, &tmp_sz,
                                         &datatype) == 0);
    ASSERT_EQ(tmp_sz, 0UL);
    ASSERT_EQ(datatype, HYPERDATATYPE_MAP_GENERIC);
    // example 2
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "world", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_int(map, -1, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_int(map, 1, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_finalize(map, &status,
                                         &tmp, &tmp_sz,
                                         &datatype) == 0);
    ASSERT_EQ(tmp_sz, 34UL);
    ASSERT_TRUE(memcmp(tmp, "\x05\x00\x00\x00hello\x01\x00\x00\x00\x00\x00\x00\x00"
                            "\x05\x00\x00\x00world\xff\xff\xff\xff\xff\xff\xff\xff", 34) == 0);
    ASSERT_EQ(datatype, HYPERDATATYPE_MAP_STRING_INT64);
    // double key insert
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_WRONG_STATE);
    // double value insert
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_int(map, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_int(map, 0, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_WRONG_STATE);
    // error on cross-ds insert
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_int(map, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_int(map, 0, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_int(map, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "map key", 7, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_string(map, "hello", 5, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_int(map, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_float(map, 0., &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_int(map, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "map key", 7, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_float(map, 0., &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);

    hyperdex_ds_arena_destroy(a);
}

TEST(ClientDataStructures, MapStringFloat)
{
    hyperdex_ds_returncode status;
    const char* tmp = NULL;
    size_t tmp_sz = 0;
    hyperdatatype datatype;
    hyperdex_ds_arena* a = hyperdex_ds_arena_create();
    hyperdex_ds_map* map;

    // example 1
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_finalize(map, &status,
                                         &tmp, &tmp_sz,
                                         &datatype) == 0);
    ASSERT_EQ(tmp_sz, 0UL);
    ASSERT_EQ(datatype, HYPERDATATYPE_MAP_GENERIC);
    // example 2
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "zero", 4, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_float(map, 0, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "pi", 2, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_float(map, 3.1415, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_finalize(map, &status,
                                         &tmp, &tmp_sz,
                                         &datatype) == 0);
    ASSERT_EQ(tmp_sz, 30UL);
    ASSERT_TRUE(memcmp(tmp, "\x02\x00\x00\x00pio\x12\x83\xc0\xca!\t@"
                            "\x04\x00\x00\x00zero\x00\x00\x00\x00\x00\x00\x00\x00", 30) == 0);
    ASSERT_EQ(datatype, HYPERDATATYPE_MAP_STRING_FLOAT);
    // double key insert
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_WRONG_STATE);
    // double value insert
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_float(map, 3.1415, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_float(map, 3.1415, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_WRONG_STATE);
    // error on cross-ds insert
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_float(map, 3.1415, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_float(map, 3.1415, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_float(map, 3.1415, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "map key", 7, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_string(map, "hello", 5, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_float(map, 3.1415, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_float(map, 3.1415, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);
    map = hyperdex_ds_allocate_map(a);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "hello", 5, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_float(map, 3.1415, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_key_string(map, "map key", 7, &status) == 0);
    ASSERT_TRUE(hyperdex_ds_map_insert_val_int(map, 0, &status) == -1);
    ASSERT_EQ(status, HYPERDEX_DS_MIXED_TYPES);

    hyperdex_ds_arena_destroy(a);
}

// XXX test cases for the rest of the map types

TEST(ClientDataStructures, IterateListString)
{
    hyperdex_ds_iterator iter;
    const char* ptr = "\x05\x00\x00\x00hello\x05\x00\x00\x00world";
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_LIST_STRING, ptr, 18);
    const char* str = NULL;
    size_t str_sz = 0;

    ASSERT_TRUE(hyperdex_ds_iterate_list_string_next(&iter, &str, &str_sz) == 1);
    ASSERT_EQ(str_sz, 5UL);
    ASSERT_TRUE(memcmp(str, "hello", 5) == 0);

    ASSERT_TRUE(hyperdex_ds_iterate_list_string_next(&iter, &str, &str_sz) == 1);
    ASSERT_EQ(str_sz, 5UL);
    ASSERT_TRUE(memcmp(str, "world", 5) == 0);

    ASSERT_TRUE(hyperdex_ds_iterate_list_string_next(&iter, &str, &str_sz) == 0);
}

TEST(ClientDataStructures, IterateListInt)
{
    hyperdex_ds_iterator iter;
    const char* ptr = "\x01\x00\x00\x00\x00\x00\x00\x00"
                      "\xff\xff\xff\xff\xff\xff\xff\xff"
                      "\xef\xbe\xad\xde\x00\x00\x00\x00";
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_LIST_INT64, ptr, 24);
    int64_t num = 0;

    ASSERT_TRUE(hyperdex_ds_iterate_list_int_next(&iter, &num) == 1);
    ASSERT_EQ(num, 1L);

    ASSERT_TRUE(hyperdex_ds_iterate_list_int_next(&iter, &num) == 1);
    ASSERT_EQ(num, -1L);

    ASSERT_TRUE(hyperdex_ds_iterate_list_int_next(&iter, &num) == 1);
    ASSERT_EQ(num, 0xdeadbeef);

    ASSERT_TRUE(hyperdex_ds_iterate_list_int_next(&iter, &num) == 0);
}

TEST(ClientDataStructures, IterateListFloat)
{
    hyperdex_ds_iterator iter;
    const char* ptr = "\x00\x00\x00\x00\x00\x00\x00\x00"
                      "o\x12\x83\xc0\xca!\t@";
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_LIST_FLOAT, ptr, 16);
    double num = 1;

    ASSERT_TRUE(hyperdex_ds_iterate_list_float_next(&iter, &num) == 1);
    ASSERT_GE(num, -0.001);
    ASSERT_LE(num, 0.001);

    ASSERT_TRUE(hyperdex_ds_iterate_list_float_next(&iter, &num) == 1);
    ASSERT_GE(num, 3.14149999);
    ASSERT_LE(num, 3.14150001);

    ASSERT_TRUE(hyperdex_ds_iterate_list_float_next(&iter, &num) == 0);
}

TEST(ClientDataStructures, IterateSetString)
{
    hyperdex_ds_iterator iter;
    const char* ptr = "\x05\x00\x00\x00hello\x05\x00\x00\x00world";
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_SET_STRING, ptr, 18);
    const char* str = NULL;
    size_t str_sz = 0;

    ASSERT_TRUE(hyperdex_ds_iterate_set_string_next(&iter, &str, &str_sz) == 1);
    ASSERT_EQ(str_sz, 5UL);
    ASSERT_TRUE(memcmp(str, "hello", 5) == 0);

    ASSERT_TRUE(hyperdex_ds_iterate_set_string_next(&iter, &str, &str_sz) == 1);
    ASSERT_EQ(str_sz, 5UL);
    ASSERT_TRUE(memcmp(str, "world", 5) == 0);

    ASSERT_TRUE(hyperdex_ds_iterate_set_string_next(&iter, &str, &str_sz) == 0);
}

TEST(ClientDataStructures, IterateSetInt)
{
    hyperdex_ds_iterator iter;
    const char* ptr = "\xff\xff\xff\xff\xff\xff\xff\xff"
                      "\x01\x00\x00\x00\x00\x00\x00\x00"
                      "\xef\xbe\xad\xde\x00\x00\x00\x00";
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_SET_INT64, ptr, 24);
    int64_t num = 0;

    ASSERT_TRUE(hyperdex_ds_iterate_set_int_next(&iter, &num) == 1);
    ASSERT_EQ(num, -1L);

    ASSERT_TRUE(hyperdex_ds_iterate_set_int_next(&iter, &num) == 1);
    ASSERT_EQ(num, 1L);

    ASSERT_TRUE(hyperdex_ds_iterate_set_int_next(&iter, &num) == 1);
    ASSERT_EQ(num, 0xdeadbeef);

    ASSERT_TRUE(hyperdex_ds_iterate_set_int_next(&iter, &num) == 0);
}

TEST(ClientDataStructures, IterateSetFloat)
{
    hyperdex_ds_iterator iter;
    const char* ptr = "\x00\x00\x00\x00\x00\x00\x00\x00"
                      "o\x12\x83\xc0\xca!\t@";
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_SET_FLOAT, ptr, 16);
    double num = 1;

    ASSERT_TRUE(hyperdex_ds_iterate_set_float_next(&iter, &num) == 1);
    ASSERT_GE(num, -0.001);
    ASSERT_LE(num, 0.001);

    ASSERT_TRUE(hyperdex_ds_iterate_set_float_next(&iter, &num) == 1);
    ASSERT_GE(num, 3.14149999);
    ASSERT_LE(num, 3.14150001);

    ASSERT_TRUE(hyperdex_ds_iterate_set_float_next(&iter, &num) == 0);
}

TEST(ClientDataStructures, IterateMapStringString)
{
    hyperdex_ds_iterator iter;
    const char* ptr = "\x05\x00\x00\x00hello\x05\x00\x00\x00world"
                      "\x03\x00\x00\x00map\x08\x00\x00\x00""encoding"
                      "\x07\x00\x00\x00map key\x07\x00\x00\x00map val";
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_STRING_STRING, ptr, 59);
    const char* key = NULL;
    size_t key_sz = 0;
    const char* val = NULL;
    size_t val_sz = 0;

    ASSERT_TRUE(hyperdex_ds_iterate_map_string_string_next(&iter, &key, &key_sz, &val, &val_sz) == 1);
    ASSERT_EQ(key_sz, 5UL);
    ASSERT_TRUE(memcmp(key, "hello", 5) == 0);
    ASSERT_EQ(val_sz, 5UL);
    ASSERT_TRUE(memcmp(val, "world", 5) == 0);

    ASSERT_TRUE(hyperdex_ds_iterate_map_string_string_next(&iter, &key, &key_sz, &val, &val_sz) == 1);
    ASSERT_EQ(key_sz, 3UL);
    ASSERT_TRUE(memcmp(key, "map", 3) == 0);
    ASSERT_EQ(val_sz, 8UL);
    ASSERT_TRUE(memcmp(val, "encoding", 8) == 0);

    ASSERT_TRUE(hyperdex_ds_iterate_map_string_string_next(&iter, &key, &key_sz, &val, &val_sz) == 1);
    ASSERT_EQ(key_sz, 7UL);
    ASSERT_TRUE(memcmp(key, "map key", 7) == 0);
    ASSERT_EQ(val_sz, 7UL);
    ASSERT_TRUE(memcmp(val, "map val", 7) == 0);

    ASSERT_TRUE(hyperdex_ds_iterate_map_string_string_next(&iter, &key, &key_sz, &val, &val_sz) == 0);
}

TEST(ClientDataStructures, IterateMapStringInt)
{
    hyperdex_ds_iterator iter;
    const char* ptr = "\x05\x00\x00\x00hello\x01\x00\x00\x00\x00\x00\x00\x00"
                      "\x05\x00\x00\x00world\xff\xff\xff\xff\xff\xff\xff\xff";
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_STRING_INT64, ptr, 34);
    const char* key = NULL;
    size_t key_sz = 0;
    int64_t val = 0;

    ASSERT_TRUE(hyperdex_ds_iterate_map_string_int_next(&iter, &key, &key_sz, &val) == 1);
    ASSERT_EQ(key_sz, 5UL);
    ASSERT_TRUE(memcmp(key, "hello", 5) == 0);
    ASSERT_EQ(val, 1L);

    ASSERT_TRUE(hyperdex_ds_iterate_map_string_int_next(&iter, &key, &key_sz, &val) == 1);
    ASSERT_EQ(key_sz, 5UL);
    ASSERT_TRUE(memcmp(key, "world", 5) == 0);
    ASSERT_EQ(val, -1L);

    ASSERT_TRUE(hyperdex_ds_iterate_map_string_int_next(&iter, &key, &key_sz, &val) == 0);
}

TEST(ClientDataStructures, IterateMapStringFloat)
{
    hyperdex_ds_iterator iter;
    const char* ptr = "\x02\x00\x00\x00pio\x12\x83\xc0\xca!\t@"
                      "\x04\x00\x00\x00zero\x00\x00\x00\x00\x00\x00\x00\x00";
    hyperdex_ds_iterator_init(&iter, HYPERDATATYPE_MAP_STRING_FLOAT, ptr, 30);
    const char* key = NULL;
    size_t key_sz = 0;
    double val = 0;

    ASSERT_TRUE(hyperdex_ds_iterate_map_string_float_next(&iter, &key, &key_sz, &val) == 1);
    ASSERT_EQ(key_sz, 2UL);
    ASSERT_TRUE(memcmp(key, "pi", 2) == 0);
    ASSERT_GE(val, 3.14149999);
    ASSERT_LE(val, 3.14150001);

    ASSERT_TRUE(hyperdex_ds_iterate_map_string_float_next(&iter, &key, &key_sz, &val) == 1);
    ASSERT_EQ(key_sz, 4UL);
    ASSERT_TRUE(memcmp(key, "zero", 4) == 0);
    ASSERT_GE(val, -0.0001);
    ASSERT_LE(val, 0.0001);

    ASSERT_EQ(hyperdex_ds_iterate_map_string_float_next(&iter, &key, &key_sz, &val), 0);
}
