// Copyright (c) 2014, Cornell University
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

#include <unistd.h>

// STL
#include <memory>

// e
#include <e/slice.h>

// po6
#include <po6/error.h>

// armnod
#include "ygor.h"

// HyperDex
#include <hyperdex/admin.hpp>
#include <hyperdex/client.hpp>
#include <hyperdex/datastructures.h>

const char *_space_from_name = "profiles";
const char *_space_from_description = "space profiles \
key username \
attributes \
    string name, \
    float height, \
    int profile_views";
const char *_space_to_name = "profiles2";
const char *_space_to_description = "space profiles2 \
key username \
attributes \
    string name, \
    float height, \
    int profile_views";

static bool _quiet = false;
static int _testno = 0;
static hyperdex::Client *_cl = NULL;
static hyperdex::Admin *_ad = NULL;

static void test0();
static void add_space();
static void remove_space();

int
main(int argc, const char *argv[])
{
    try {
        // TODO: make the host and port customizable
        hyperdex::Client cl("127.0.0.1", 1982);
        _cl = &cl;
        hyperdex::Admin ad("127.0.0.1", 1982);
        _ad = &ad;

        test0();

        return EXIT_SUCCESS;
    } catch (po6::error &e) {
        std::cerr << "system error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}

static void
success()
{
    if (!_quiet) std::cout << "Test " << _testno << ":  [\x1b[32mOK\x1b[0m]\n";
}

#define FAIL(REASON) \
    do { \
        if (!_quiet) std::cout << "Test " << _testno << ":  [\x1b[31mFAIL\x1b[0m]\n" \
                                   << "location: " << __FILE__ << ":" << __LINE__ << "\n" \
                                   << "reason:  " << REASON << "\n"; \
        abort(); \
    } while (0)

static void
setup_space()
{
    hyperdex_admin_returncode status;
    if (_ad->add_space(_space_from_description, &status) < 0) {
        FAIL("admin operation failed: " << status);
    }
    if (_ad->add_space(_space_to_description, &status) < 0) {
        FAIL("admin operation failed: " << status);
    }
}

static void
remove_space()
{
    hyperdex_admin_returncode status;
    if (_ad->rm_space(_space_from_name, &status) < 0) {
        FAIL("admin operation failed: " << status);
    }

    if (_ad->rm_space(_space_to_name, &status) < 0) {
        FAIL("admin operation failed: " << status);
    }
}

static armnod_generator *
get_random_generator(const char *method,
                     const char *charset)
{
    armnod_config *config = armnod_config_create();
    if (armnod_config_method(config, method) < 0) goto wrong_config;
    if (armnod_config_charset(config, charset) < 0) goto wrong_config;
    if (armnod_config_set_size(config, 1024) < 0) goto wrong_config;

    {
        armnod_generator *gen(armnod_generator_create(config));
        armnod_generator_seed(gen, 0);
        return gen;
    }

wrong_config:
    FAIL("armnod_config received a wrong parameter.");
}

static void
add_data(const char *space_name, uint64_t num_objects)
{
    armnod_generator *alpha_gen = get_random_generator("normal", "alpha");
    armnod_generator *digit_gen = get_random_generator("normal", "digit");

    const char *key_format = "object_%lu";
    for (size_t i = 0; i < num_objects; i++) {
        char key[20] = {0};
        snprintf(key, 19, key_format, i);
        size_t key_sz = strlen(key);

        hyperdex_client_attribute attrs[3];

        size_t attrs_sz = 0;

        attrs[0].attr = "name";
        attrs[0].value = armnod_generate(alpha_gen);
        attrs[0].value_sz = strlen(attrs[0].value);
        attrs[0].datatype = HYPERDATATYPE_STRING;

        attrs[1].attr = "height";
        double valf = atof(armnod_generate(digit_gen));
        char buff[8] = {0};
        hyperdex_ds_pack_float(valf, buff);
        attrs[1].value = buff;
        attrs[1].value_sz = 8;
        attrs[1].datatype = HYPERDATATYPE_FLOAT;

        attrs[2].attr = "profile_views";
        int64_t vali = atoi(armnod_generate(digit_gen));
        char bufi[8] = {0};
        hyperdex_ds_pack_int(vali, bufi);
        attrs[2].value = bufi;
        attrs[2].value_sz = 8;
        attrs[2].datatype = HYPERDATATYPE_INT64;

        hyperdex_client_returncode op_status;
        if (_cl->put(space_name, key, key_sz, attrs, 3, &op_status) < 0) {
            FAIL("client operation failed: " << op_status);
        }

        hyperdex_client_returncode loop_status;
        if (_cl->loop(-1, &loop_status) < 0) {
            FAIL("client operation failed: " << loop_status);
        }
    }
}

inline static void
check_attr(const hyperdex_client_attribute *attr,
           const char *name,
           const char *value,
           hyperdatatype datatype)
{
    if (strcmp(attr->attr, name) != 0)
        FAIL("presence check: attribute is \"" << attr->attr << "\" instead of \"name\"");

    if (attr->datatype != datatype)
        FAIL("presence check: attribute is not of datatype \"string\"");

    switch (datatype) {
    case HYPERDATATYPE_STRING: {
        std::string str(attr->value, attr->value_sz);
        if (str.compare(value) != 0)
            FAIL("presence check: attribute has the value \""
                 << str << "\" instead of \"" << value << "\"");
        break;
    }
    case HYPERDATATYPE_INT64: {
        int64_t num;
        hyperdex_ds_unpack_int(attr->value, attr->value_sz, &num);
        int64_t val_int = atoi(value);
        if (num != val_int)
            FAIL("presence check: attribute has the value \""
                 << num << "\" instead of \"" << val_int << "\"");
        break;
    }
    case HYPERDATATYPE_FLOAT: {
        double num;
        hyperdex_ds_unpack_float(attr->value, attr->value_sz, &num);
        double val_float = atof(value);
        if (num != val_float)
            FAIL("presence check: attribute has the value \""
                 << num << "\" instead of \"" << val_float << "\"");
        break;
    }
    default:
        FAIL("Unknown datatype " << datatype);
    }
}

static void
read_and_verify_data(const char *space_name, uint64_t num_objects)
{
    armnod_generator *alpha_gen = get_random_generator("normal", "alpha");
    armnod_generator *digit_gen = get_random_generator("normal", "digit");

    const char *key_format = "object_%lu";
    for (size_t i = 0; i < num_objects; i++) {
        char key[20] = {0};
        snprintf(key, 19, key_format, i);
        size_t key_sz = strlen(key);

        const hyperdex_client_attribute *attrs;
        size_t attrs_sz;

        hyperdex_client_returncode get_status;
        int64_t gid = _cl->get(space_name, key, key_sz, &get_status, &attrs, &attrs_sz);
        if (gid < 0) {
            FAIL("get encountered error: " << get_status);
        }

        hyperdex_client_returncode loop_status;
        int64_t lid = _cl->loop(10000, &loop_status);
        if (lid < 0) {
            FAIL("loop encountered error: " << loop_status);
        }

        if (gid != lid)
            FAIL("loop id (" << lid << ") does not match get id (" << gid << ")");

        if (get_status != HYPERDEX_CLIENT_SUCCESS)
            FAIL("operation " << gid << " (a presence check) returned " << get_status);

        check_attr(&attrs[0], "name", armnod_generate(alpha_gen), HYPERDATATYPE_STRING);
        check_attr(&attrs[1], "height", armnod_generate(digit_gen), HYPERDATATYPE_FLOAT);
        check_attr(&attrs[2], "profile_views", armnod_generate(digit_gen), HYPERDATATYPE_INT64);
    }

    // armnod_config_destroy(alpha_gen->config);
    armnod_generator_destroy(alpha_gen);
    // armnod_config_destroy(digit_gen->config);
    armnod_generator_destroy(digit_gen);
}

static uint64_t num_objects = 100000;

static void
test0()
{
    setup_space();

    add_data(_space_from_name, num_objects);

    hyperdex_admin_returncode status;
    if (_ad->migrate_data(_space_from_name, _space_to_name, &status) < 0) {
        FAIL("migration failure: " << status);
    }

    std::cout << "Sleeping for 20 seconds for the migration to complete...\n";
    usleep(20 * 1000 * 1000);

    read_and_verify_data(_space_to_name, num_objects);

    remove_space();
    success();
}
