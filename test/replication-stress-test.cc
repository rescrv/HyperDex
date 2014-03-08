// Copyright (c) 2011-2012, Cornell University
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

// C
#include <cstdlib>

// STL
#include <map>

// po6
#include <po6/error.h>

// e
#include <e/compat.h>
#include <e/endian.h>
#include <e/guard.h>
#include <e/slice.h>

// HyperDex
#include <hyperdex/admin.hpp>
#include <hyperdex/client.hpp>
#include "tools/common.h"

static bool _continuous = false;
static long _partitions = 4;
const char* _space = "replication";

static bool _quiet = false;
static int _testno = 0;
static hyperdex::Client* _cl = NULL;
static std::map<int64_t, e::compat::shared_ptr<hyperdex_client_returncode> > _incompleteops;

static void
wipe();
static void
test0();
static void
test1();
static void
test2();
static void
test3();
static void
test4();

int
main(int argc, const char* argv[])
{
    hyperdex::connect_opts conn;
    e::argparser st;
    st.arg().name('c', "continuous")
            .description("run the test continuously (default: no)")
            .set_true(&_continuous);
    st.arg().name('n', "partitions")
            .description("assume each subspace is partitioned into N pieces (default: 4)")
            .metavar("N").as_long(&_partitions);
    st.arg().name('s', "space")
            .description("perform all operations on the specified space (default: \"replication\")")
            .metavar("space").as_string(&_space);
    st.arg().name('q', "quiet")
            .description("silence all output")
            .set_true(&_quiet);

    e::argparser ap;
    ap.autohelp();
    ap.add("Connect to a cluster:", conn.parser());
    ap.add("Replication stress test:", st);

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (!conn.validate())
    {
        std::cerr << "invalid host:port specification\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (ap.args_sz() != 0)
    {
        std::cerr << "command takes no arguments" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    try
    {
        hyperdex::Client cl(conn.host(), conn.port());
        _cl = &cl;

        do
        {
            wipe();
            test0();
            test1();
            test2();
            test3();
            test4();
        }
        while (_continuous);

        return EXIT_SUCCESS;
    }
    catch (po6::error& e)
    {
        std::cerr << "system error:  " << e.what() << std::endl;
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

static uint64_t
generate_attr(size_t idx)
{
    uint64_t partition_size = UINT64_MAX / _partitions;
    return partition_size * idx + partition_size / 2;
}

static void
put(uint64_t A, uint64_t B, uint64_t C)
{
    char buf[3 * sizeof(uint64_t)];
    e::pack64le(A, buf + 0 * sizeof(uint64_t));
    e::pack64le(B, buf + 1 * sizeof(uint64_t));
    e::pack64le(C, buf + 2 * sizeof(uint64_t));
    hyperdex_client_attribute attrs[2];
    attrs[0].attr = "B";
    attrs[0].value = buf + 1 * sizeof(uint64_t);
    attrs[0].value_sz = sizeof(uint64_t);
    attrs[0].datatype = HYPERDATATYPE_INT64;
    attrs[1].attr = "C";
    attrs[1].value = buf + 2 * sizeof(uint64_t);
    attrs[1].value_sz = sizeof(uint64_t);
    attrs[1].datatype = HYPERDATATYPE_INT64;
    e::compat::shared_ptr<hyperdex_client_returncode> status(new hyperdex_client_returncode());
    int64_t id = _cl->put(_space, buf, sizeof(uint64_t), attrs, 2, status.get());

    if (id < 0)
    {
        FAIL("put encountered error " << *status);
    }

    std::pair<std::map<int64_t, e::compat::shared_ptr<hyperdex_client_returncode> >::iterator, bool> res;
    res = _incompleteops.insert(std::make_pair(id, status));

    if (res.second != true)
    {
        FAIL("put could not insert into incompleteops");
    }
}

static void
del(uint64_t A)
{
    char buf[sizeof(uint64_t)];
    e::pack64le(A, buf);
    e::compat::shared_ptr<hyperdex_client_returncode> status(new hyperdex_client_returncode());
    int64_t id = _cl->del(_space, buf, sizeof(uint64_t), status.get());

    if (id < 0)
    {
        FAIL("del encountered error " << status);
    }

    std::pair<std::map<int64_t, e::compat::shared_ptr<hyperdex_client_returncode> >::iterator, bool> res;
    res = _incompleteops.insert(std::make_pair(id, status));

    if (res.second != true)
    {
        FAIL("del could not insert into incompleteops");
    }
}

static void
flush()
{
    while (!_incompleteops.empty())
    {
        hyperdex_client_returncode status;
        int64_t id = _cl->loop(10000, &status);

        if (id < 0)
        {
            FAIL("loop returned error " << status);
        }
        else
        {
            std::map<int64_t, e::compat::shared_ptr<hyperdex_client_returncode> >::iterator incomplete;
            incomplete = _incompleteops.find(id);

            if (incomplete == _incompleteops.end())
            {
                FAIL("loop returned unknown id " << id);
            }

            if (*(incomplete->second) != HYPERDEX_CLIENT_SUCCESS)
            {
                FAIL("operation " << id << " returned " << *(incomplete->second));
            }

            _incompleteops.erase(incomplete);
        }
    }
}

static void
present(uint64_t A, uint64_t B, uint64_t C)
{
    flush();

    char buf[3 * sizeof(uint64_t)];
    e::pack64le(A, buf + 0 * sizeof(uint64_t));
    e::pack64le(B, buf + 1 * sizeof(uint64_t));
    e::pack64le(C, buf + 2 * sizeof(uint64_t));
    hyperdex_client_returncode gstatus;
    hyperdex_client_returncode lstatus;
    const hyperdex_client_attribute* attrs;
    size_t attrs_sz;
    int64_t gid = _cl->get(_space, buf, sizeof(uint64_t), &gstatus, &attrs, &attrs_sz);

    if (gid < 0)
    {
        FAIL("get encountered error " << gstatus);
    }

    int64_t lid = _cl->loop(10000, &lstatus);

    if (lid < 0)
    {
        FAIL("loop encountered error " << lstatus);
    }

    if (gid != lid)
    {
        FAIL("loop id (" << lid << ") does not match get id (" << gid << ")");
    }

    if (gstatus != HYPERDEX_CLIENT_SUCCESS)
    {
        FAIL("operation " << gid << " (a presence check) returned " << gstatus);
    }

    if (attrs_sz != 2)
    {
        FAIL("presence check: " << attrs_sz << " attributes instead of 2 attributes");
    }

    if (strcmp(attrs[0].attr, "B") != 0)
    {
        FAIL("presence check: first attribute is \"" << attrs[0].attr << "\" instead of \"B\"");
    }

    if (attrs[0].datatype != HYPERDATATYPE_INT64)
    {
        FAIL("presence check: first attribute is not of datatype \"int\"");
    }

    if (e::slice(attrs[0].value, attrs[0].value_sz) != e::slice(buf + sizeof(uint64_t), sizeof(uint64_t)))
    {
        FAIL("presence check: first attribute does not match " << B
             << " (" << e::slice(attrs[0].value, attrs[0].value_sz).hex() << " vs. "
             << e::slice(buf + sizeof(uint64_t), sizeof(uint64_t)).hex() << ")");
    }

    if (strcmp(attrs[1].attr, "C") != 0)
    {
        FAIL("presence check: second attribute is \"" << attrs[1].attr << "\" instead of \"C\"");
    }

    if (attrs[1].datatype != HYPERDATATYPE_INT64)
    {
        FAIL("presence check: second attribute is not of datatype \"int\"");
    }

    if (e::slice(attrs[1].value, attrs[1].value_sz) != e::slice(buf + 2 * sizeof(uint64_t), sizeof(uint64_t)))
    {
        FAIL("presence check: second attribute does not match " << C
             << " (" << e::slice(attrs[1].value, attrs[1].value_sz).hex() << " vs. "
             << e::slice(buf + 2 * sizeof(uint64_t), sizeof(uint64_t)).hex() << ")");
    }

    hyperdex_client_destroy_attrs(attrs, attrs_sz);
}

static void
absent(uint64_t A)
{
    flush();

    char buf[sizeof(uint64_t)];
    e::pack64le(A, buf);
    hyperdex_client_returncode gstatus;
    hyperdex_client_returncode lstatus;
    const hyperdex_client_attribute* attrs = NULL;
    size_t attrs_sz = 0;
    int64_t gid = _cl->get(_space, buf, sizeof(uint64_t), &gstatus, &attrs, &attrs_sz);

    if (gid < 0)
    {
        FAIL("get encountered error " << gstatus);
    }

    int64_t lid = _cl->loop(10000, &lstatus);

    if (lid < 0)
    {
        FAIL("loop returned error " << lstatus);
    }

    if (gid != lid)
    {
        FAIL("loop id (" << lid << ") does not match get id (" << gid << ")");
    }

    if (gstatus != HYPERDEX_CLIENT_NOTFOUND)
    {
        FAIL("operation " << gid << " (an absence check) returned " << gstatus);
    }

    assert(!attrs && !attrs_sz);
}

void
wipe()
{
    for (long i = 0; i < _partitions; ++i)
    {
        uint64_t A = generate_attr(i);
        put(A, A, A);
        del(A);
        flush();
    }
}

// This test continually puts keys, ensuring that every way in which one could
// select regions from the subspaces is chosen.
void
test0()
{
    _testno = 0;
    uint64_t A;
    uint64_t B;
    uint64_t C;

    for (long i = 0; i < _partitions; ++i)
    {
        A = generate_attr(i);
        absent(A);

        for (long j = 0; j < _partitions; ++j)
        {
            B = generate_attr(j);

            for (long k = 0; k < _partitions; ++k)
            {
                C = generate_attr(k);
                put(A, B, C);
            }
        }

        present(A, B, C);
        del(A);
        absent(A);
    }

    success();
}

// This test continually puts/deletes keys, ensuring that a [PUT, DEL] sequence
// of operations is run through every way in which one could select regions from
// the subspaces.  This tests that delete plays nicely with the fresh bit.
void
test1()
{
    _testno = 1;
    uint64_t A;
    uint64_t B;
    uint64_t C;

    for (long i = 0; i < _partitions; ++i)
    {
        A = generate_attr(i);
        absent(A);

        for (long j = 0; j < _partitions; ++j)
        {
            B = generate_attr(j);

            for (long k = 0; k < _partitions; ++k)
            {
                C = generate_attr(k);
                put(A, B, C);
                del(A);
            }
        }

        absent(A);
    }

    success();
}

// This test puts keys such that A and B are fixed, but every choice of C for
// a fixed A, B is exercised.  Keys are deleted when changing A or B.  This
// tests that CHAIN_SUBSPACE messages work.  This tests that CHAIN_SUBSPACE
// messages work correctly.
void
test2()
{
    _testno = 2;
    uint64_t A = 0;
    uint64_t B = 0;
    uint64_t C = 0;

    for (long i = 0; i < _partitions; ++i)
    {
        A = generate_attr(i);

        for (long j = 0; j < _partitions; ++j)
        {
            B = generate_attr(j);
            absent(A);

            for (long k = 0; k < _partitions; ++k)
            {
                C = generate_attr(k);
                put(A, B, C);
            }

            present(A, B, C);
            del(A);
            absent(A);
        }
    }

    success();
}

// This is similar to test2 except that columns A and C are fixed.
void
test3()
{
    _testno = 3;
    uint64_t A = 0;
    uint64_t B = 0;
    uint64_t C = 0;

    for (long i = 0; i < _partitions; ++i)
    {
        A = generate_attr(i);

        for (long j = 0; j < _partitions; ++j)
        {
            C = generate_attr(j);
            absent(A);

            for (long k = 0; k < _partitions; ++k)
            {
                B = generate_attr(k);
                put(A, B, C);
            }

            present(A, B, C);
            del(A);
            absent(A);
        }
    }

    success();
}

// This test stresses the interaction of CHAIN_SUBSPACE with DELETE messages.
// For each A, B, C pair, it creates a point.  It then issues a PUT which causes
// B and C to jump to another subspace.  It then issues a DEL to try to create a
// deferred update.
void
test4()
{
    _testno = 4;
    uint64_t A;
    uint64_t B;
    uint64_t BP;
    uint64_t C;
    uint64_t CP;

    for (long h = 0; h < _partitions; ++h)
    {
        A = generate_attr(h);

        for (long i = 0; i < _partitions; ++i)
        {
            B = generate_attr(i);

            for (long j = 0; j < _partitions; ++j)
            {
                C = generate_attr(j);

                for (long k = 0; k < _partitions; ++k)
                {
                    BP = generate_attr(k);

                    for (long l = 0; l < _partitions; ++l)
                    {
                        CP = generate_attr(l);
                        put(A, B, C);
                        put(A, BP, CP);
                        del(A);
                    }
                }

                absent(A);
            }
        }
    }

    success();
}
