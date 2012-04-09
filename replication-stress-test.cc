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

// The purpose of this test is to expose errors in the replication logic.
//
// The test is designed to be run on a space with dimensions A, B, C.
// Dimension A is the key while dimensions B and C are meant to be separate
// subspaces.  The code intentionally creates certain scenarios which will
// fail with high probability if the code is not correct.
//
// The prefix provided as a commandline argument is used to efficiently prune
// out duplicate results.  It should match P in "auto P R" used for each
// subspace for basic tests.  It is important to test this both on a single
// daemon instance, and on several daemon instances.  The former stresses code
// with lightening quick responses while the latter introduces delay and
// non-determinism.

// C
#include <cstdio>

// Popt
#include <popt.h>

// STL
#include <iomanip>
#include <map>
#include <memory>
#include <string>
#include <tr1/functional>

// po6
#include <po6/net/ipaddr.h>
#include <po6/net/location.h>

// e
#include <e/bitfield.h>
#include <e/convert.h>

// HyperspaceHashing
#include "hyperspacehashing/hyperspacehashing/hashes.h"
#include "hyperspacehashing/hyperspacehashing/prefix.h"

// HyperDex
#include "hyperclient/hyperclient.h"

// These 32-bit values all hash (using hyperspacehashing) to be have the same
// high-order byte as their index.  E.g. index 0 has a hash of
// 0x00??????????????, while index 255 has a hash of 0xff??????????????.
static uint32_t nums[256];

// These are all the incomplete operations
static std::map<int64_t, hyperclient_returncode*> incompleteops;

static const char* space = "replication";
static const char* host = "127.0.0.1";
static po6::net::ipaddr coord(host);
static long port = 1234;
static long prefix = 3;

static void
find_hashes();

static void
test0(hyperclient* cl);
static void
test1(hyperclient* cl);
static void
test2(hyperclient* cl);
static void
test3(hyperclient* cl);
static void
test4(hyperclient* cl);

class generator
{
    public:
        static uint32_t end()
        {
            bool looped = false;
            uint32_t ret = 0;

            for (generator i; i.has_next(); i.next())
            {
                looped = true;
                ret = i;
            }

            assert(looped);
            return ret;
        }

    public:
        generator() : m_i(0) {}

    public:
        bool has_next() const { return m_i < 256; }

    public:
        void next()
        {
            uint32_t current = (m_i >> (8 - prefix));

            while (m_i < 256 && current == (m_i >> (8 - prefix)))
            {
                ++ m_i;
            }
        }

    public:
        operator uint32_t () const { return nums[m_i]; }

    private:
        uint32_t m_i;
};

extern "C"
{

static struct poptOption popts[] = {
    POPT_AUTOHELP
    {"prefix", 'P', POPT_ARG_LONG, &prefix, 'P',
        "the size of the prefix used when creating the space",
        "number"},
    {"space", 's', POPT_ARG_STRING, &space, 's',
        "the HyperDex space to use",
        "space"},
    {"host", 'h', POPT_ARG_STRING, &host, 'h',
        "the IP address of the coordinator",
        "IP"},
    {"port", 'p', POPT_ARG_LONG, &port, 'p',
        "the port number of the coordinator",
        "port"},
    POPT_TABLEEND
};

} // extern "C"

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
            case 'P':
                if (prefix < 0)
                {
                    std::cerr << "prefix must be >= 0" << std::endl;
                    return EXIT_FAILURE;
                }

                if (prefix > 64)
                {
                    std::cerr << "prefix must be <= 64" << std::endl;
                    return EXIT_FAILURE;
                }

                break;
            case 's':
                break;
            case 'h':
                try
                {
                    coord = po6::net::ipaddr(host);
                }
                catch (po6::error& e)
                {
                    std::cerr << "cannot parse coordinator address" << std::endl;
                    return EXIT_FAILURE;
                }
                catch (std::invalid_argument& e)
                {
                    std::cerr << "cannot parse coordinator address" << std::endl;
                    return EXIT_FAILURE;
                }

                break;
            case 'p':
                if (port < 0 || port >= (1 << 16))
                {
                    std::cerr << "port number out of range for TCP" << std::endl;
                    return EXIT_FAILURE;
                }

                break;
            case POPT_ERROR_NOARG:
            case POPT_ERROR_BADOPT:
            case POPT_ERROR_BADNUMBER:
            case POPT_ERROR_OVERFLOW:
                std::cerr << poptStrerror(rc) << " " << poptBadOption(poptcon, 0) << std::endl;
                return EXIT_FAILURE;
            case POPT_ERROR_OPTSTOODEEP:
            case POPT_ERROR_BADQUOTE:
            case POPT_ERROR_ERRNO:
            default:
                std::cerr << "logic error in argument parsing" << std::endl;
                return EXIT_FAILURE;
        }
    }

    find_hashes();

    try
    {
        hyperclient cl(host, port);

        test0(&cl);
        test1(&cl);
        test2(&cl);
        test3(&cl);
        test4(&cl);
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
find_hashes()
{
    bool found[256];
    size_t complete = 0;

    for (size_t i = 0; i < 256; ++i)
    {
        found[i] = false;
    }

    std::vector<hyperspacehashing::hash_t> hashes(1, hyperspacehashing::EQUALITY);
    hyperspacehashing::prefix::hasher hasher(hashes);

    for (uint32_t value = 0; complete < 256; ++value)
    {
        e::slice key(&value, sizeof(uint32_t));
        uint64_t hash = hasher.hash(key).point;
        unsigned high_byte = (hash >> 56) & 0xff;

        if (!found[high_byte])
        {
            found[high_byte] = true;
            nums[high_byte] = value;
            ++complete;
        }
    }

    const char* terminator = "";
    const char* sep;
    std::cout << "Hashes turn out to be:\n";

    for (size_t i = 0; i < 256; ++i)
    {
        if (i % 16 == 0)
        {
            std::cout << terminator << std::setw(4) << i;
            sep = " [";
            terminator = "]\n";
        }

        std::cout << sep << std::setw(4) << nums[i];
        sep = ", ";
    }

    std::cout << "]\n" << std::endl;
}

static void
success(int testno)
{
    std::cout << "Test " << testno << ":  [\x1b[32mOK\x1b[0m]\n";
}

#define FAIL(TESTNO, REASON) \
    do { \
    std::cout << "Test " << TESTNO << ":  [\x1b[31mFAIL\x1b[0m]\n" \
              << "location: " << __FILE__ << ":" << __LINE__ << "\n" \
              << "reason:  " << REASON << "\n"; \
    abort(); \
    } while (0)

static void
put(int testno,
    hyperclient* cl,
    uint32_t A, uint32_t B, uint32_t C)
{
    hyperclient_attribute attrs[2];
    attrs[0].attr = "B";
    attrs[0].value = reinterpret_cast<const char*>(&B);
    attrs[0].value_sz = sizeof(uint32_t);
    attrs[0].datatype = HYPERDATATYPE_STRING;
    attrs[1].attr = "C";
    attrs[1].value = reinterpret_cast<const char*>(&C);
    attrs[1].value_sz = sizeof(uint32_t);
    attrs[1].datatype = HYPERDATATYPE_STRING;
    hyperclient_returncode* status = new hyperclient_returncode();
    int64_t id = cl->put(space, reinterpret_cast<const char*>(&A), sizeof(uint32_t), attrs, 2, status);

    if (id < 0)
    {
        FAIL(testno, "put encountered error " << status);
    }

    std::pair<std::map<int64_t, hyperclient_returncode*>::iterator, bool> res;
    res = incompleteops.insert(std::make_pair(id, status));

    if (res.second != true)
    {
        FAIL(testno, "put could not insert into incompleteops");
    }
}

static void
del(int testno,
    hyperclient* cl,
    uint32_t A)
{
    hyperclient_returncode* status = new hyperclient_returncode();
    int64_t id = cl->del(space, reinterpret_cast<const char*>(&A), sizeof(uint32_t), status);

    if (id < 0)
    {
        FAIL(testno, "del encountered error " << status);
    }

    std::pair<std::map<int64_t, hyperclient_returncode*>::iterator, bool> res;
    res = incompleteops.insert(std::make_pair(id, status));

    if (res.second != true)
    {
        FAIL(testno, "del could not insert into incompleteops");
    }
}

static void
flush(int testno,
      hyperclient* cl)
{
    while (!incompleteops.empty())
    {
        hyperclient_returncode status;
        int64_t id = cl->loop(10000, &status);

        if (id < 0)
        {
            FAIL(testno, "loop returned error " << status);
        }
        else
        {
            std::map<int64_t, hyperclient_returncode*>::iterator incomplete;
            incomplete = incompleteops.find(id);

            if (incomplete == incompleteops.end())
            {
                FAIL(testno, "loop returned unknown id " << id);
            }

            if (*(incomplete->second) != HYPERCLIENT_SUCCESS)
            {
                FAIL(testno, "operation " << id << " returned " << *(incomplete->second));
            }

            delete incomplete->second;
            incompleteops.erase(incomplete);
        }
    }
}

static void
present(int testno,
        hyperclient* cl,
        uint32_t A, uint32_t B, uint32_t C)
{
    flush(testno, cl);

    hyperclient_returncode gstatus;
    hyperclient_returncode lstatus;
    hyperclient_attribute* attrs;
    size_t attrs_sz;
    int64_t gid = cl->get(space, reinterpret_cast<char*>(&A), sizeof(uint32_t), &gstatus, &attrs, &attrs_sz);

    if (gid < 0)
    {
        FAIL(testno, "get encountered error " << gstatus);
    }

    int64_t lid = cl->loop(10000, &lstatus);

    if (lid < 0)
    {
        FAIL(testno, "loop encountered error " << lstatus);
    }

    if (gid != lid)
    {
        FAIL(testno, "loop id (" << lid << ") does not match get id (" << gid << ")");
    }

    if (gstatus != HYPERCLIENT_SUCCESS)
    {
        FAIL(testno, "operation " << gid << " (a presence check) returned " << gstatus);
    }

    if (attrs_sz != 2)
    {
        FAIL(testno, "presence check: " << attrs_sz << " attributes instead of 2 attributes");
    }

    if (strcmp(attrs[0].attr, "B") != 0)
    {
        FAIL(testno, "presence check: first attribute is \"" << attrs[0].attr << "\" instead of \"B\"");
    }

    if (attrs[0].datatype != HYPERDATATYPE_STRING)
    {
        FAIL(testno, "presence check: first attribute is not of datatype \"string\"");
    }

    if (e::slice(attrs[0].value, attrs[0].value_sz) != e::slice(&B, sizeof(uint32_t)))
    {
        FAIL(testno, "presence check: first attribute does not match " << B);
    }

    if (strcmp(attrs[1].attr, "C") != 0)
    {
        FAIL(testno, "presence check: second attribute is \"" << attrs[1].attr << "\" instead of \"C\"");
    }

    if (attrs[1].datatype != HYPERDATATYPE_STRING)
    {
        FAIL(testno, "presence check: second attribute is not of datatype \"string\"");
    }

    if (e::slice(attrs[1].value, attrs[1].value_sz) != e::slice(&C, sizeof(uint32_t)))
    {
        FAIL(testno, "presence check: second attribute does not match " << C);
    }

    free(attrs);
}

static void
absent(int testno,
       hyperclient* cl,
       uint32_t A)
{
    flush(testno, cl);

    hyperclient_returncode gstatus;
    hyperclient_returncode lstatus;
    hyperclient_attribute* attrs = NULL;
    size_t attrs_sz = 0;
    int64_t gid = cl->get(space, reinterpret_cast<char*>(&A), sizeof(uint32_t), &gstatus, &attrs, &attrs_sz);

    if (gid < 0)
    {
        FAIL(testno, "get encountered error " << gstatus);
    }

    int64_t lid = cl->loop(10000, &lstatus);

    if (lid < 0)
    {
        FAIL(testno, "loop returned error " << lstatus);
    }

    if (gid != lid)
    {
        FAIL(testno, "loop id (" << lid << ") does not match get id (" << gid << ")");
    }

    if (gstatus != HYPERCLIENT_NOTFOUND)
    {
        FAIL(testno, "operation " << gid << " (an absence check) returned " << gstatus);
    }

    assert(!attrs && !attrs_sz);
}

// This test continually puts keys, ensuring that every way in which one could
// select regions from the subspaces is chosen.
void
test0(hyperclient* cl)
{
    for (generator A; A.has_next(); A.next())
    {
        absent(0, cl, A);

        for (generator B; B.has_next(); B.next())
        {
            for (generator C; C.has_next(); C.next())
            {
                put(0, cl, A, B, C);
            }
        }

        present(0, cl, A, generator::end(), generator::end());
        del(0, cl, A);
        absent(0, cl, A);
    }

    success(0);
}

// This test continually puts/deletes keys, ensuring that a [PUT, DEL] sequence
// of operations is run through every way in which one could select regions from
// the subspaces.  6 * 2**prefix requests will be performed.  This tests that
// delete plays nicely with the fresh bit.
void
test1(hyperclient* cl)
{
    for (generator A; A.has_next(); A.next())
    {
        absent(1, cl, A);

        for (generator B; B.has_next(); B.next())
        {
            for (generator C; C.has_next(); C.next())
            {
                put(1, cl, A, B, C);
                del(1, cl, A);
            }
        }

        absent(1, cl, A);
    }

    success(1);
}

// This test puts keys such that A and B are fixed, but every choice of C for
// a fixed A, B is exercised.  Keys are deleted when changing A or B.  This
// tests that CHAIN_SUBSPACE messages work.  This tests that CHAIN_SUBSPACE
// messages work correctly.
void
test2(hyperclient* cl)
{
    for (generator A; A.has_next(); A.next())
    {
        for (generator B; B.has_next(); B.next())
        {
            absent(2, cl, A);

            for (generator C; C.has_next(); C.next())
            {
                put(2, cl, A, B, C);
            }

            present(2, cl, A, B, generator::end());
            del(2, cl, A);
            absent(2, cl, A);
        }
    }

    success(2);
}

// This is isomorphic to test2 except that columns A and C are fixed.
void
test3(hyperclient* cl)
{
    for (generator A; A.has_next(); A.next())
    {
        for (generator C; C.has_next(); C.next())
        {
            absent(3, cl, A);

            for (generator B; B.has_next(); B.next())
            {
                put(3, cl, A, B, C);
            }

            present(3, cl, A, generator::end(), C);
            del(3, cl, A);
            absent(3, cl, A);
        }
    }

    success(3);
}

// This test stresses the interaction of CHAIN_SUBSPACE with DELETE messages.
// For each A, B, C pair, it creates a point.  It then issues a PUT which causes
// B and C to jump to another subspace.  It then issues a DEL to try to create a
// deferred update.
void
test4(hyperclient* cl)
{
    for (generator A; A.has_next(); A.next())
    {
        for (generator B; B.has_next(); B.next())
        {
            for (generator C; C.has_next(); C.next())
            {
                absent(4, cl, A);

                for (generator BP; BP.has_next(); BP.next())
                {
                    for (generator CP; CP.has_next(); CP.next())
                    {
                        if (B != BP && C != CP)
                        {
                            put(4, cl, A, B, C);
                            put(4, cl, A, BP, CP);
                            del(4, cl, A);
                        }
                    }
                }

                absent(4, cl, A);
            }
        }
    }

    success(4);
}
