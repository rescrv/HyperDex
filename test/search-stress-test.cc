// Copyright (c) 2012, Cornell University
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

// STL
#include <vector>

// e
#include <e/endian.h>
#include <e/popt.h>

// HyperDex
#include <hyperdex/client.hpp>
#include "tools/common.h"

const char* _colnames[] = {"bit01", "bit02", "bit03", "bit04", "bit05", "bit06",
                           "bit07", "bit08", "bit09", "bit10", "bit11", "bit12",
                           "bit13", "bit14", "bit15", "bit16", "bit17", "bit18",
                           "bit19", "bit20", "bit21", "bit22", "bit23", "bit24",
                           "bit25", "bit26", "bit27", "bit28", "bit29", "bit30",
                           "bit31", "bit32"};
static bool _quiet = false;
static const char* _space = "search";
static unsigned long _random_iters = 16;
static const char* _key_type_s = "int";
static hyperdatatype _key_type = HYPERDATATYPE_INT64;

static int
test(hyperdex::Client* cl);

int
main(int argc, const char* argv[])
{
    hyperdex::connect_opts conn;
    e::argparser st;
    st.arg().name('s', "space")
            .description("perform all operations on the specified space (default: \"search\")")
            .metavar("space").as_string(&_space);
    st.arg().name('k', "key-type")
            .description("one of \"int\", \"string\" (default: \"int\")")
            .metavar("type").as_string(&_key_type_s);
    st.arg().name('q', "quiet")
            .description("silence all output")
            .set_true(&_quiet);

    e::argparser ap;
    ap.autohelp();
    ap.add("Connect to a cluster:", conn.parser());
    ap.add("Search stress test:", st);

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

    if (strcmp(_key_type_s, "int") == 0)
    {
        _key_type = HYPERDATATYPE_INT64;
    }
    else if (strcmp(_key_type_s, "string") == 0)
    {
        _key_type = HYPERDATATYPE_STRING;
    }
    else
    {
        std::cerr << "unsupported key type \"" << _key_type_s << "\"" << std::endl;
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
        return test(&cl);
    }
    catch (std::exception& e)
    {
        std::cerr << "error:  " << e.what();
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

#define HYPERDEX_TEST_SUCCESS(TESTNO) \
    do { \
        if (!_quiet) std::cout << "Test " << TESTNO << ":  [\x1b[32mOK\x1b[0m]" << std::endl; \
    } while (0)

#define HYPERDEX_TEST_FAIL(TESTNO, REASON) \
    do { \
        if (!_quiet) std::cout << "Test " << TESTNO << ":  [\x1b[31mFAIL\x1b[0m]\n" \
                  << "location: " << __FILE__ << ":" << __LINE__ << "\n" \
                  << "reason:  " << REASON << std::endl; \
    abort(); \
    } while (0)

#define SEARCH_STRESS_TIMEOUT(testno) (25000 * (1 + testno))

static void
empty(size_t testno,
      hyperdex::Client* cl);

static void
populate(size_t testno,
         hyperdex::Client* cl);

static void
search(size_t testno,
       hyperdex::Client* cl,
       const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
       const std::vector<bool>& expected);

static void
sorted_search(size_t testno,
              hyperdex::Client* cl,
              const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
              const std::vector<bool>& expected);

static void
group_del(size_t testno,
          hyperdex::Client* cl,
          const struct hyperdex_client_attribute_check* checks, size_t checks_sz);

static void
count(size_t testno,
      hyperdex::Client* cl,
      const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
      size_t expected);

static void
all_search_tests(size_t testno,
                 hyperdex::Client* cl,
                 const std::vector<bool>& expecting);

int
test(hyperdex::Client* cl)
{
    group_del(16, cl, NULL, 0); // clear everything
    srand(0xdeadbeef); // yes, I know rand+mod is suboptimal
    hyperdex_client_attribute_check chk;
    chk.datatype = HYPERDATATYPE_STRING;
    chk.predicate = HYPERPREDICATE_EQUALS;

    for (size_t testno = 0; testno < 10; ++testno)
    {
        // Check that we start out clean
        empty(testno, cl);
        // Populate with all items we expect
        populate(testno, cl);
        std::vector<bool> expecting(1ULL << testno, true);
        all_search_tests(testno, cl, expecting);
        // group_del everything that is even
        chk.attr = _colnames[0];
        chk.value = "0";
        chk.value_sz = 1;
        group_del(testno, cl, &chk, 1);

        for (size_t i = 0; i < expecting.size(); ++i)
        {
            if (!(i & 1))
            {
                expecting[i] = false;
            }
        }

        all_search_tests(testno, cl, expecting);
        // take out the upper half of the objects
        chk.attr = _colnames[testno > 0 ? testno - 1 : 0];
        chk.value = "1";
        chk.value_sz = 1;
        group_del(testno, cl, &chk, 1);

        for (size_t i = expecting.size() / 2; i < expecting.size(); ++i)
        {
            expecting[i] = false;
        }

        all_search_tests(testno, cl, expecting);
        // take out the lower quarter of the objects
        chk.attr = _colnames[testno > 1 ? testno - 2 : 0];
        chk.value = "0";
        chk.value_sz = 1;
        group_del(testno, cl, &chk, 1);

        for (size_t i = 0; i < expecting.size() / 4; ++i)
        {
            expecting[i] = false;
        }

        all_search_tests(testno, cl, expecting);
        // Blow it all away
        group_del(testno, cl, NULL, 0);
        // Check to make sure everything is gone.
        empty(testno, cl);
        HYPERDEX_TEST_SUCCESS(testno);
    }

    return 0;
}

void
empty(size_t testno,
      hyperdex::Client* cl)
{
    count(testno, cl, NULL, 0, 0);
    std::vector<bool> expected(1ULL << testno, false);
    search(testno, cl, NULL, 0, expected);
    sorted_search(testno, cl, NULL, 0, expected);
}

static void
populate(size_t testno,
         hyperdex::Client* cl)
{
    for (int64_t number = 0; number < (1LL << testno); ++number)
    {
        hyperdex_client_attribute attrs[32];

        for (size_t i = 0; i < 32; ++i)
        {
            attrs[i].attr = _colnames[i];
            attrs[i].datatype = HYPERDATATYPE_STRING;

            if ((number & (1ULL << i)))
            {
                attrs[i].value = "1";
                attrs[i].value_sz = 1;
            }
            else
            {
                attrs[i].value = "0";
                attrs[i].value_sz = 1;
            }
        }

        char buf[sizeof(uint64_t)];

        if (_key_type == HYPERDATATYPE_STRING)
        {
            e::pack64be(number, buf);
        }
        else if (_key_type == HYPERDATATYPE_INT64)
        {
            e::pack64le(number, buf);
        }
        else
        {
            HYPERDEX_TEST_FAIL(testno, "unknown key datatype");
        }

        hyperdex_client_returncode pstatus;
        int64_t pid = cl->put(_space, buf, sizeof(uint64_t), attrs, 32, &pstatus);

        if (pid < 0)
        {
            HYPERDEX_TEST_FAIL(testno, "put encountered error " << pstatus << " (pid=" << pid << "): " << cl->error_message() << " at " << cl->error_location());
        }

        hyperdex_client_returncode lstatus;
        int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT(testno), &lstatus);

        if (lid < 0)
        {
            HYPERDEX_TEST_FAIL(testno, "loop returned error " << lstatus << ": " << cl->error_message() << " at " << cl->error_location());
        }

        if (pid != lid)
        {
            HYPERDEX_TEST_FAIL(testno, "loop id (" << lid << ") does not match put id (" << pid << ")");
        }

        if (pstatus != HYPERDEX_CLIENT_SUCCESS)
        {
            HYPERDEX_TEST_FAIL(testno, "operation " << pid << " (populate) returned " << pstatus << ": " << cl->error_message() << " at " << cl->error_location());
        }
    }
}

void
search(size_t testno,
       hyperdex::Client* cl,
       const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
       const std::vector<bool>& expected)
{
    std::vector<bool> seen(expected.size(), false);
    const hyperdex_client_attribute* attrs;
    size_t attrs_sz;
    hyperdex_client_returncode sstatus = HYPERDEX_CLIENT_SUCCESS;
    int64_t sid = cl->search(_space, checks, checks_sz, &sstatus, &attrs, &attrs_sz);

    if (sid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "search encountered error " << sstatus << ": " << cl->error_message() << " at " << cl->error_location());
    }

    while (true)
    {
        hyperdex_client_returncode lstatus;
        int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT(testno), &lstatus);

        if (lid < 0)
        {
            HYPERDEX_TEST_FAIL(testno, "loop returned error " << lstatus << ": " << cl->error_message() << " at " << cl->error_location());
        }

        if (sid != lid)
        {
            HYPERDEX_TEST_FAIL(testno, "loop id (" << lid << ") does not match search id (" << sid << ")");
        }

        if (sstatus != HYPERDEX_CLIENT_SUCCESS && sstatus != HYPERDEX_CLIENT_SEARCHDONE)
        {
            HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (search) returned " << sstatus << ": " << cl->error_message() << " at " << cl->error_location());
        }

        if (sstatus == HYPERDEX_CLIENT_SUCCESS)
        {
            if (attrs_sz != 33)
            {
                HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (search) returned " << attrs_sz
                                           << " attributes instead of 33");
            }

            if (strcmp(attrs[0].attr, "number") != 0)
            {
                HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (search) key was field \""
                                           << attrs[0].attr << "\" but should have been field \"number\"");
            }

            if (attrs[0].value_sz != sizeof(uint64_t))
            {
                HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (search) key has size "
                                           << attrs[0].value_sz << " bytes, but should be sizeof(int64_t)");
            }

            int64_t num = 0;

            if (_key_type == HYPERDATATYPE_STRING)
            {
                e::unpack64be(attrs[0].value, &num);
            }
            else if (_key_type == HYPERDATATYPE_INT64)
            {
                e::unpack64le(attrs[0].value, &num);
            }
            else
            {
                HYPERDEX_TEST_FAIL(testno, "unknown key datatype");
            }

            if (num >= (1LL << testno))
            {
                HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (search) number " << num << " outside range");
            }

            if (seen[num])
            {
                HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (search) number " << num << " already seen");
            }

            seen[num] = true;

            if (!expected[num])
            {
                HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (search) number " << num << " not expected");
            }

            for (size_t i = 0; i < 32; ++i)
            {
                if (strcmp(attrs[i + 1].attr, _colnames[i]) != 0)
                {
                    HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (search) key was field \""
                                               << attrs[i + 1].attr << "\" but should have been field \""
                                               << _colnames[i] << "\"");
                }

                if (attrs[i + 1].value_sz != 1)
                {
                    HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (search) attribute has size "
                                               << attrs[i + 1].value_sz << " bytes, but should be 1");
                }

                if (*attrs[i + 1].value == '1')
                {
                    if (!(num & (1ULL << i)))
                    {
                        HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (search) bit " << i
                                                   << " not set for number " << num);
                    }
                }
                else if (*attrs[i + 1].value == '0')
                {
                    if ((num & (1ULL << i)))
                    {
                        HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (search) bit " << i
                                                   << " set for number " << num);
                    }
                }
                else
                {
                    HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (search) bit not '0' or '1' (is " << (int)*attrs[i + 1].value << "d)");
                }
            }
        }

        if (sstatus == HYPERDEX_CLIENT_SEARCHDONE)
        {
            break;
        }
    }

    for (size_t i = 0; i < expected.size(); ++i)
    {
        if (expected[i] && !seen[i])
        {
            HYPERDEX_TEST_FAIL(testno, "search should have returned " << i << " but did not");
        }
    }
}

void
sorted_search(size_t testno,
              hyperdex::Client* cl,
              const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
              const std::vector<bool>& expected)
{
    std::vector<bool> seen(expected.size(), false);
    const hyperdex_client_attribute* attrs;
    size_t attrs_sz;
    hyperdex_client_returncode sstatus = HYPERDEX_CLIENT_SUCCESS;
    int64_t sid = cl->sorted_search(_space, checks, checks_sz, "number", 1ULL << testno, false, &sstatus, &attrs, &attrs_sz);

    if (sid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "sorted_search encountered error " << sstatus << ": " << cl->error_message() << " at " << cl->error_location());
    }

    int64_t old_num = 0;

    while (true)
    {
        hyperdex_client_returncode lstatus;
        int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT(testno), &lstatus);

        if (lid < 0)
        {
            HYPERDEX_TEST_FAIL(testno, "loop returned error " << lstatus << ": " << cl->error_message() << " at " << cl->error_location());
        }

        if (sid != lid)
        {
            HYPERDEX_TEST_FAIL(testno, "loop id (" << lid << ") does not match sorted_search id (" << sid << ")");
        }

        if (sstatus != HYPERDEX_CLIENT_SUCCESS && sstatus != HYPERDEX_CLIENT_SEARCHDONE)
        {
            HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) returned " << sstatus << ": " << cl->error_message() << " at " << cl->error_location());
        }

        if (sstatus == HYPERDEX_CLIENT_SUCCESS)
        {
            if (attrs_sz != 33)
            {
                HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) returned " << attrs_sz
                                           << " attributes instead of 33");
            }

            if (strcmp(attrs[0].attr, "number") != 0)
            {
                HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) key was field \""
                                           << attrs[0].attr << "\" but should have been field \"number\"");
            }

            if (attrs[0].value_sz != sizeof(uint64_t))
            {
                HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) key has size "
                                           << attrs[0].value_sz << " bytes, but should be sizeof(int64_t)");
            }

            int64_t num = 0;

            if (_key_type == HYPERDATATYPE_STRING)
            {
                e::unpack64be(attrs[0].value, &num);
            }
            else if (_key_type == HYPERDATATYPE_INT64)
            {
                e::unpack64le(attrs[0].value, &num);
            }
            else
            {
                HYPERDEX_TEST_FAIL(testno, "unknown key datatype");
            }

            if (num >= (1LL << testno))
            {
                HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) number " << num << " outside range");
            }

            if (num < old_num)
            {
                HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) number " << num << " out of order (prev was " << old_num << ")");
            }

            old_num = num;

            if (seen[num])
            {
                HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) number " << num << " already seen");
            }

            seen[num] = true;

            if (!expected[num])
            {
                HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) number " << num << " not expected");
            }

            for (size_t i = 0; i < 32; ++i)
            {
                if (strcmp(attrs[i + 1].attr, _colnames[i]) != 0)
                {
                    HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) key was field \""
                                               << attrs[i + 1].attr << "\" but should have been field \""
                                               << _colnames[i] << "\"");
                }

                if (attrs[i + 1].value_sz != 1)
                {
                    HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) attribute has size "
                                               << attrs[i + 1].value_sz << " bytes, but should be 1");
                }

                if (*attrs[i + 1].value == '1')
                {
                    if (!(num & (1ULL << i)))
                    {
                        HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) bit " << i
                                                   << " not set for number " << num);
                    }
                }
                else if (*attrs[i + 1].value == '0')
                {
                    if ((num & (1ULL << i)))
                    {
                        HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) bit " << i
                                                   << " set for number " << num);
                    }
                }
                else
                {
                    HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) bit not '0' or '1' (is " << (int)*attrs[i + 1].value << "d)");
                }
            }
        }

        if (sstatus == HYPERDEX_CLIENT_SEARCHDONE)
        {
            break;
        }
    }

    for (size_t i = 0; i < expected.size(); ++i)
    {
        if (expected[i] && !seen[i])
        {
            HYPERDEX_TEST_FAIL(testno, "sorted_search should have returned " << i << " but did not");
        }
    }
}

static void
group_del(size_t testno,
          hyperdex::Client* cl,
          const struct hyperdex_client_attribute_check* checks, size_t checks_sz)
{
    hyperdex_client_returncode gstatus;
    uint64_t count;
    int64_t gid = cl->group_del(_space, checks, checks_sz, &gstatus, &count);

    if (gid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "group_del encountered error " << gstatus << ": " << cl->error_message() << " at " << cl->error_location());
    }

    hyperdex_client_returncode lstatus;
    int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT(testno), &lstatus);

    if (lid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "loop returned error " << lstatus << ": " << cl->error_message() << " at " << cl->error_location());
    }

    if (gid != lid)
    {
        HYPERDEX_TEST_FAIL(testno, "loop id (" << lid << ") does not match group_del id (" << gid << ")");
    }

    if (gstatus != HYPERDEX_CLIENT_SUCCESS)
    {
        HYPERDEX_TEST_FAIL(testno, "operation " << gid << " (group_del) returned " << gstatus << ": " << cl->error_message() << " at " << cl->error_location());
    }

    uint64_t sleep = 10000000ULL;
    sleep *= (1 + testno);
    struct timespec ts;
    ts.tv_sec = sleep / 1000000000ULL;
    ts.tv_nsec = sleep % 1000000000ULL;
    nanosleep(&ts, NULL);
}

static void
count(size_t testno,
      hyperdex::Client* cl,
      const struct hyperdex_client_attribute_check* checks, size_t checks_sz,
      size_t expected)
{
    uint64_t c = 0;
    hyperdex_client_returncode cstatus;
    int64_t cid = cl->count(_space, checks, checks_sz, &cstatus, &c);

    if (cid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "count encountered error " << cstatus << ": " << cl->error_message() << " at " << cl->error_location());
    }

    hyperdex_client_returncode lstatus;
    int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT(testno), &lstatus);

    if (lid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "loop returned error " << lstatus << ": " << cl->error_message() << " at " << cl->error_location());
    }

    if (cid != lid)
    {
        HYPERDEX_TEST_FAIL(testno, "loop id (" << lid << ") does not match count id (" << cid << ")");
    }

    if (cstatus != HYPERDEX_CLIENT_SUCCESS)
    {
        HYPERDEX_TEST_FAIL(testno, "operation " << cid << " (count) returned " << cstatus << ": " << cl->error_message() << " at " << cl->error_location());
    }

    if (c != expected)
    {
        HYPERDEX_TEST_FAIL(testno, "counted " << c << " objects when we should have counted " << expected << " objects");
    }
}

static void
setup_random_search(size_t,
                    hyperdex_client_attribute_check* chks,
                    size_t* chks_sz,
                    std::vector<bool>* expecting)
{
    uint32_t mask = rand();
    uint32_t bits = rand();
    *chks_sz = 0;

    for (size_t i = 0; i < 32; ++i)
    {
        if ((mask & (1ULL << i)))
        {
            chks[*chks_sz].attr = _colnames[i];

            if ((bits & (1ULL << i)))
            {
                chks[*chks_sz].value = "1";
            }
            else
            {
                chks[*chks_sz].value = "0";
            }

            chks[*chks_sz].value_sz = 1;
            chks[*chks_sz].datatype = HYPERDATATYPE_STRING;
            chks[*chks_sz].predicate = HYPERPREDICATE_EQUALS;
            ++(*chks_sz);
        }
    }

    for (uint32_t i = 0; i < expecting->size(); ++i)
    {
        if ((mask & i) != (mask & bits))
        {
            (*expecting)[i] = false;
        }
    }
}

void
all_search_tests(size_t testno,
                 hyperdex::Client* cl,
                 const std::vector<bool>& expecting)
{
    struct timespec ts;
    ts.tv_sec = 0;
    ts.tv_nsec = 100 * 1000ULL * 1000ULL;
    nanosleep(&ts, NULL);
    search(testno, cl, NULL, 0, expecting);
    sorted_search(testno, cl, NULL, 0, expecting);
    size_t num = 0;

    for (size_t i = 0; i < expecting.size(); ++i)
    {
        if (expecting[i])
        {
            ++num;
        }
    }

    count(testno, cl, NULL, 0, num);
    hyperdex_client_attribute_check chks[32];

    for (size_t i = 0; i < _random_iters; ++i)
    {
        size_t chks_sz;
        std::vector<bool> subset_expecting(expecting);
        setup_random_search(testno, chks, &chks_sz, &subset_expecting);
        search(testno, cl, chks, chks_sz, subset_expecting);
        sorted_search(testno, cl, chks, chks_sz, subset_expecting);
        num = 0;

        for (size_t j = 0; j < subset_expecting.size(); ++j)
        {
            if (subset_expecting[j])
            {
                ++num;
            }
        }

        count(testno, cl, chks, chks_sz, num);
    }
}
