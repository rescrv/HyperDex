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

// Popt
#include <popt.h>

// STL
#include <vector>

// po6
#include <po6/error.h>
#include <po6/net/ipaddr.h>

// e
#include <e/endian.h>
#include <e/guard.h>

// HyperDex
#include "hyperdex.h"
#include "client/hyperclient.hpp"

const char* hyperdex_test_space = NULL;
const char* hyperdex_test_host = "127.0.0.1";
long hyperdex_test_port = 1234;

extern "C"
{

struct poptOption hyperdex_test_popts[] = {
    {"space", 's', POPT_ARG_STRING, &hyperdex_test_space, 's',
        "the HyperDex space to use",
        "space"},
    {"host", 'h', POPT_ARG_STRING, &hyperdex_test_host, 'h',
        "connect to an IP address or hostname (default: 127.0.0.1)",
        "addr"},
    {"port", 'p', POPT_ARG_LONG, &hyperdex_test_port, 'p',
        "connect to an alternative port (default: 1982)",
        "port"},
    POPT_TABLEEND
};

} // extern "C"

#define HYPERDEX_TEST_TABLE {NULL, 0, POPT_ARG_INCLUDE_TABLE, hyperdex_test_popts, 0, "Connect to a cluster:", NULL},

#define HYPERDEX_TEST_POPT_SWITCH \
    case 's': \
        break; \
    case 'h': \
        try \
        { \
            po6::net::ipaddr tst(hyperdex_test_host); \
        } \
        catch (po6::error& e) \
        { \
            std::cerr << "cannot parse coordinator address" << std::endl; \
            return EXIT_FAILURE; \
        } \
        catch (std::invalid_argument& e) \
        { \
            std::cerr << "cannot parse coordinator address" << std::endl; \
            return EXIT_FAILURE; \
        } \
        break; \
    case 'p': \
        if (hyperdex_test_port < 0 || hyperdex_test_port >= (1 << 16)) \
        { \
            std::cerr << "port number out of range for TCP" << std::endl; \
            return EXIT_FAILURE; \
        } \
        break; \
    case POPT_ERROR_NOARG: \
    case POPT_ERROR_BADOPT: \
    case POPT_ERROR_BADNUMBER: \
    case POPT_ERROR_OVERFLOW: \
        std::cerr << poptStrerror(rc) << " " << poptBadOption(poptcon, 0) << std::endl; \
        return EXIT_FAILURE; \
    case POPT_ERROR_OPTSTOODEEP: \
    case POPT_ERROR_BADQUOTE: \
    case POPT_ERROR_ERRNO: \
    default: \
        std::cerr << "logic error in argument parsing" << std::endl; \
        return EXIT_FAILURE;

#define HYPERDEX_TEST_SUCCESS(TESTNO) \
    do { \
        std::cout << "Test " << TESTNO << ":  [\x1b[32mOK\x1b[0m]\n"; \
    } while (0)

#define HYPERDEX_TEST_FAIL(TESTNO, REASON) \
    do { \
        std::cout << "Test " << TESTNO << ":  [\x1b[31mFAIL\x1b[0m]\n" \
                  << "location: " << __FILE__ << ":" << __LINE__ << "\n" \
                  << "reason:  " << REASON << "\n"; \
    abort(); \
    } while (0)

#define SEARCH_STRESS_TIMEOUT(TESTNO) (10000)

const char* colnames[] = {"bit01", "bit02", "bit03", "bit04", "bit05", "bit06",
                          "bit07", "bit08", "bit09", "bit10", "bit11", "bit12",
                          "bit13", "bit14", "bit15", "bit16", "bit17", "bit18",
                          "bit19", "bit20", "bit21", "bit22", "bit23", "bit24",
                          "bit25", "bit26", "bit27", "bit28", "bit29", "bit30",
                          "bit31", "bit32"};

static unsigned long random_iters = 16;
static hyperdatatype key_type = HYPERDATATYPE_INT64;

extern "C"
{

static struct poptOption popts[] = {
    POPT_AUTOHELP
    HYPERDEX_TEST_TABLE
    POPT_TABLEEND
};

} // extern "C"

static void
test(HyperClient* cl);

int
main(int argc, const char* argv[])
{
    poptContext poptcon;
    poptcon = poptGetContext(NULL, argc, argv, popts, POPT_CONTEXT_POSIXMEHARDER);
    e::guard g = e::makeguard(poptFreeContext, poptcon);
    g.use_variable();
    int rc;

    while ((rc = poptGetNextOpt(poptcon)) != -1)
    {
        switch (rc)
        {
            HYPERDEX_TEST_POPT_SWITCH
        }
    }

    try
    {
        HyperClient cl(hyperdex_test_host, hyperdex_test_port);

        if (!hyperdex_test_space)
        {
            hyperdex_test_space = "search";
        }

        test(&cl);
    }
    catch (po6::error& e)
    {
        std::cerr << "There was a system error:  " << e.what();
        return EXIT_FAILURE;
    }
    catch (std::runtime_error& e)
    {
        std::cerr << "There was a runtime error:  " << e.what();
        return EXIT_FAILURE;
    }
    catch (std::bad_alloc& e)
    {
        std::cerr << "There was a memory allocation error:  " << e.what();
        return EXIT_FAILURE;
    }
    catch (std::exception& e)
    {
        std::cerr << "There was a generic error:  " << e.what();
        return EXIT_FAILURE;
    }

    return EXIT_SUCCESS;
}

static void
empty(size_t testno,
      HyperClient* cl);

static void
populate(size_t testno,
         HyperClient* cl);

static void
search(size_t testno,
       HyperClient* cl,
       const struct hyperclient_attribute_check* checks, size_t checks_sz,
       const std::vector<bool>& expected);

static void
sorted_search(size_t testno,
              HyperClient* cl,
              const struct hyperclient_attribute_check* checks, size_t checks_sz,
              const std::vector<bool>& expected);

static void
group_del(size_t testno,
          HyperClient* cl,
          const struct hyperclient_attribute_check* checks, size_t checks_sz);

static void
count(size_t testno,
      HyperClient* cl,
      const struct hyperclient_attribute_check* checks, size_t checks_sz,
      size_t expected);

static void
all_search_tests(size_t testno,
                 HyperClient* cl,
                 const std::vector<bool>& expecting);

void
test(HyperClient* cl)
{
    group_del(16, cl, NULL, 0); // clear everything
    srand(0xdeadbeef); // yes, I know rand+mod is bad
    hyperclient_attribute_check chk;
    chk.datatype = HYPERDATATYPE_STRING;
    chk.predicate = HYPERPREDICATE_EQUALS;

    // Until busybee is fixed, we cannot do tests 12+
    for (size_t testno = 0; testno < 10; ++testno)
    {
        // Check that we start out clean
        empty(testno, cl);
        // Populate with all items we expect
        populate(testno, cl);
        std::vector<bool> expecting(1ULL << testno, true);
        all_search_tests(testno, cl, expecting);
        // group_del everything that is even
        chk.attr = colnames[0];
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
        chk.attr = colnames[testno > 0 ? testno - 1 : 0];
        chk.value = "1";
        chk.value_sz = 1;
        group_del(testno, cl, &chk, 1);

        for (size_t i = expecting.size() / 2; i < expecting.size(); ++i)
        {
            expecting[i] = false;
        }

        all_search_tests(testno, cl, expecting);
        // take out the lower quarter of the objects
        chk.attr = colnames[testno > 1 ? testno - 2 : 0];
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
}

void
empty(size_t testno,
      HyperClient* cl)
{
    count(testno, cl, NULL, 0, 0);
    std::vector<bool> expected(1ULL << testno, false);
    search(testno, cl, NULL, 0, expected);
    sorted_search(testno, cl, NULL, 0, expected);
}

static void
populate(size_t testno,
         HyperClient* cl)
{
    for (uint64_t number = 0; number < (1ULL << testno); ++number)
    {
        hyperclient_attribute attrs[32];

        for (size_t i = 0; i < 32; ++i)
        {
            attrs[i].attr = colnames[i];
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

        if (key_type == HYPERDATATYPE_STRING)
        {
            e::pack64be(number, buf);
        }
        else if (key_type == HYPERDATATYPE_INT64)
        {
            e::pack64le(number, buf);
        }
        else if (key_type == HYPERDATATYPE_FLOAT)
        {
            double d = number;
            e::packdoublele(d, buf);
        }
        else
        {
            HYPERDEX_TEST_FAIL(testno, "unknown key datatype");
        }

        hyperclient_returncode pstatus;
        int64_t pid = cl->put(hyperdex_test_space, buf, sizeof(uint64_t), attrs, 32, &pstatus);

        if (pid < 0)
        {
            HYPERDEX_TEST_FAIL(testno, "put encountered error " << pstatus << " (pid=" << pid << ")");
        }

        hyperclient_returncode lstatus;
        int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT(testno), &lstatus);

        if (lid < 0)
        {
            HYPERDEX_TEST_FAIL(testno, "loop returned error " << lstatus);
        }

        if (pid != lid)
        {
            HYPERDEX_TEST_FAIL(testno, "loop id (" << lid << ") does not match put id (" << pid << ")");
        }

        if (pstatus != HYPERCLIENT_SUCCESS)
        {
            HYPERDEX_TEST_FAIL(testno, "operation " << pid << " (populate) returned " << pstatus);
        }
    }
}

void
search(size_t testno,
       HyperClient* cl,
       const struct hyperclient_attribute_check* checks, size_t checks_sz,
       const std::vector<bool>& expected)
{
    std::vector<bool> seen(expected.size(), false);
    hyperclient_attribute* attrs;
    size_t attrs_sz;
    hyperclient_returncode sstatus = HYPERCLIENT_SUCCESS;
    int64_t sid = cl->search(hyperdex_test_space, checks, checks_sz, &sstatus, &attrs, &attrs_sz);

    if (sid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "search encountered error " << sstatus);
    }

    while (true)
    {
        hyperclient_returncode lstatus;
        int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT(testno), &lstatus);

        if (lid < 0)
        {
            HYPERDEX_TEST_FAIL(testno, "loop returned error " << lstatus);
        }

        if (sid != lid)
        {
            HYPERDEX_TEST_FAIL(testno, "loop id (" << lid << ") does not match search id (" << sid << ")");
        }

        if (sstatus != HYPERCLIENT_SUCCESS && sstatus != HYPERCLIENT_SEARCHDONE)
        {
            HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (search) returned " << sstatus);
        }

        if (sstatus == HYPERCLIENT_SUCCESS)
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

            if (key_type == HYPERDATATYPE_STRING)
            {
                e::unpack64be(attrs[0].value, &num);
            }
            else if (key_type == HYPERDATATYPE_INT64)
            {
                e::unpack64le(attrs[0].value, &num);
            }
            else if (key_type == HYPERDATATYPE_FLOAT)
            {
                double d;
                e::unpackdoublele(attrs[0].value, &d);
                num = d;
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
                if (strcmp(attrs[i + 1].attr, colnames[i]) != 0)
                {
                    HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (search) key was field \""
                                               << attrs[i + 1].attr << "\" but should have been field \""
                                               << colnames[i] << "\"");
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

        if (sstatus == HYPERCLIENT_SEARCHDONE)
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
              HyperClient* cl,
              const struct hyperclient_attribute_check* checks, size_t checks_sz,
              const std::vector<bool>& expected)
{
    std::vector<bool> seen(expected.size(), false);
    hyperclient_attribute* attrs;
    size_t attrs_sz;
    hyperclient_returncode sstatus = HYPERCLIENT_SUCCESS;
    int64_t sid = cl->sorted_search(hyperdex_test_space, checks, checks_sz, "number", 1ULL << testno, false, &sstatus, &attrs, &attrs_sz);

    if (sid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "sorted_search encountered error " << sstatus);
    }

    int64_t old_num = 0;

    while (true)
    {
        hyperclient_returncode lstatus;
        int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT(testno), &lstatus);

        if (lid < 0)
        {
            HYPERDEX_TEST_FAIL(testno, "loop returned error " << lstatus);
        }

        if (sid != lid)
        {
            HYPERDEX_TEST_FAIL(testno, "loop id (" << lid << ") does not match sorted_search id (" << sid << ")");
        }

        if (sstatus != HYPERCLIENT_SUCCESS && sstatus != HYPERCLIENT_SEARCHDONE)
        {
            HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) returned " << sstatus);
        }

        if (sstatus == HYPERCLIENT_SUCCESS)
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

            if (key_type == HYPERDATATYPE_STRING)
            {
                e::unpack64be(attrs[0].value, &num);
            }
            else if (key_type == HYPERDATATYPE_INT64)
            {
                e::unpack64le(attrs[0].value, &num);
            }
            else if (key_type == HYPERDATATYPE_FLOAT)
            {
                double d;
                e::unpackdoublele(attrs[0].value, &d);
                num = d;
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
                if (strcmp(attrs[i + 1].attr, colnames[i]) != 0)
                {
                    HYPERDEX_TEST_FAIL(testno, "operation " << sid << " (sorted_search) key was field \""
                                               << attrs[i + 1].attr << "\" but should have been field \""
                                               << colnames[i] << "\"");
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

        if (sstatus == HYPERCLIENT_SEARCHDONE)
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
          HyperClient* cl,
          const struct hyperclient_attribute_check* checks, size_t checks_sz)
{
    hyperclient_returncode gstatus;
    int64_t gid = cl->group_del(hyperdex_test_space, checks, checks_sz, &gstatus);

    if (gid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "group_del encountered error " << gstatus);
    }

    hyperclient_returncode lstatus;
    int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT(testno), &lstatus);

    if (lid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "loop returned error " << lstatus);
    }

    if (gid != lid)
    {
        HYPERDEX_TEST_FAIL(testno, "loop id (" << lid << ") does not match group_del id (" << gid << ")");
    }

    if (gstatus != HYPERCLIENT_SUCCESS)
    {
        HYPERDEX_TEST_FAIL(testno, "operation " << gid << " (group_del) returned " << gstatus);
    }
}

static void
count(size_t testno,
      HyperClient* cl,
      const struct hyperclient_attribute_check* checks, size_t checks_sz,
      size_t expected)
{
    uint64_t c = 0;
    hyperclient_returncode cstatus;
    int64_t cid = cl->count(hyperdex_test_space, checks, checks_sz, &cstatus, &c);

    if (cid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "count encountered error " << cstatus);
    }

    hyperclient_returncode lstatus;
    int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT(testno), &lstatus);

    if (lid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "loop returned error " << lstatus);
    }

    if (cid != lid)
    {
        HYPERDEX_TEST_FAIL(testno, "loop id (" << lid << ") does not match count id (" << cid << ")");
    }

    if (cstatus != HYPERCLIENT_SUCCESS)
    {
        HYPERDEX_TEST_FAIL(testno, "operation " << cid << " (count) returned " << cstatus);
    }

    if (c != expected)
    {
        HYPERDEX_TEST_FAIL(testno, "counted " << c << " objects when we should have counted " << expected << " objects");
    }
}

static void
setup_random_search(size_t,
                    hyperclient_attribute_check* chks,
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
            chks[*chks_sz].attr = colnames[i];

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
                 HyperClient* cl,
                 const std::vector<bool>& expecting)
{
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
    hyperclient_attribute_check chks[32];

    for (size_t i = 0; i < random_iters; ++i)
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
