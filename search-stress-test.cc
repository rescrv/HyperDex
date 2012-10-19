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

// e
#include <e/endian.h>

// HyperDex
#include "test/common.h"
#include "hyperclient/hyperclient.h"

#define SEARCH_STRESS_TIMEOUT 100000

const char* colnames[] = {"bit01", "bit02", "bit03", "bit04", "bit05", "bit06",
                          "bit07", "bit08", "bit09", "bit10", "bit11", "bit12",
                          "bit13", "bit14", "bit15", "bit16", "bit17", "bit18",
                          "bit19", "bit20", "bit21", "bit22", "bit23", "bit24",
                          "bit25", "bit26", "bit27", "bit28", "bit29", "bit30",
                          "bit31", "bit32"};

extern "C"
{

static struct poptOption popts[] = {
    POPT_AUTOHELP
    HYPERDEX_TEST_TABLE
    POPT_TABLEEND
};

} // extern "C"

static void
test(hyperclient* cl);

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
        hyperclient cl(hyperdex_test_host, hyperdex_test_port);

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
      hyperclient* cl);

static void
populate(size_t testno,
         hyperclient* cl);

static void
search(size_t testno,
       hyperclient* cl,
       const struct hyperclient_attribute_check* checks, size_t checks_sz,
       const std::vector<bool>& expected);

static void
sorted_search(size_t testno,
              hyperclient* cl,
              const struct hyperclient_attribute_check* checks, size_t checks_sz,
              const std::vector<bool>& expected);

static void
group_del(size_t testno,
          hyperclient* cl,
          const struct hyperclient_attribute_check* checks, size_t checks_sz);

static void
count(size_t testno,
      hyperclient* cl,
      const struct hyperclient_attribute_check* checks, size_t checks_sz,
      size_t expected);

void
test(hyperclient* cl)
{
    for (size_t i = 0; i < 16; ++i)
    {
        empty(i, cl);
        populate(i, cl);
        //count(i, cl, NULL, 0, 1ULL << i);
        empty(i, cl);
        HYPERDEX_TEST_SUCCESS(i);
    }
}

void
empty(size_t testno,
      hyperclient* cl)
{
    count(testno, cl, NULL, 0, 0);
    std::vector<bool> expected(1ULL << testno, false);
    search(testno, cl, NULL, 0, expected);
    sorted_search(testno, cl, NULL, 0, expected);
}

static void
populate(size_t testno,
         hyperclient* cl)
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

        char bufle[sizeof(uint64_t)];
        e::pack64le(number, bufle);
        hyperclient_returncode pstatus;
        int64_t pid = cl->put(hyperdex_test_space, bufle, sizeof(uint64_t), attrs, 32, &pstatus);

        if (pid < 0)
        {
            HYPERDEX_TEST_FAIL(testno, "put encountered error " << pstatus << " (pid=" << pid << ")");
        }

        hyperclient_returncode lstatus;
        int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT, &lstatus);

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
       hyperclient* cl,
       const struct hyperclient_attribute_check* checks, size_t checks_sz,
       const std::vector<bool>& expected)
{
    const std::vector<bool> seen(expected.size(), false);
    hyperclient_attribute* attr;
    size_t attr_sz;
    hyperclient_returncode sstatus = HYPERCLIENT_SUCCESS;
    int64_t sid = cl->search(hyperdex_test_space, checks, checks_sz, &sstatus, &attr, &attr_sz);

    if (sid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "search encountered error " << sstatus);
    }

    while (true)
    {
        hyperclient_returncode lstatus;
        int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT, &lstatus);

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
            // XXX check that we've not yet seen it
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
              hyperclient* cl,
              const struct hyperclient_attribute_check* checks, size_t checks_sz,
              const std::vector<bool>& expected)
{
    const std::vector<bool> seen(expected.size(), false);
    hyperclient_attribute* attr;
    size_t attr_sz;
    hyperclient_returncode sstatus = HYPERCLIENT_SUCCESS;
    int64_t sid = cl->sorted_search(hyperdex_test_space, checks, checks_sz, "number", 1ULL << testno, true, &sstatus, &attr, &attr_sz);

    if (sid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "sorted_search encountered error " << sstatus);
    }

    while (true)
    {
        hyperclient_returncode lstatus;
        int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT, &lstatus);

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
            // XXX check that we've not yet seen it
            // XXX check that it is in order
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
          hyperclient* cl,
          const struct hyperclient_attribute_check* checks, size_t checks_sz)
{
    hyperclient_returncode gstatus;
    int64_t gid = cl->group_del(hyperdex_test_space, checks, checks_sz, &gstatus);

    if (gid < 0)
    {
        HYPERDEX_TEST_FAIL(testno, "group_del encountered error " << gstatus);
    }

    hyperclient_returncode lstatus;
    int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT, &lstatus);

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
      hyperclient* cl,
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
    int64_t lid = cl->loop(SEARCH_STRESS_TIMEOUT, &lstatus);

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
        HYPERDEX_TEST_FAIL(testno, "counted " << c << " objects when we should have counted " << expected << "objects");
    }
}
