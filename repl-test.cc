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

// The purpose of this test is to expose errors in the replication logic.
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

// POSIX
#include <signal.h>

// STL
#include <string>

// Google CityHash
#include <city.h>

// po6
#include <po6/net/ipaddr.h>
#include <po6/net/location.h>

#include <glog/logging.h>

// e
#include <e/convert.h>

// HyperDex
#include <hyperclient/client.h>

// These 32-bit values all hash (using CityHash) to be have the same high-order
// byte as their index.  E.g. index 0 has a hash of 0x00??????????????, while
// index 255 has a hash of 0xff??????????????.
static uint32_t nums[256];

std::string space;
size_t prefix;
bool conditionals = true;

static void
sigalarm(int) {}

static int
usage();

static void
find_hashes();

static void
fail(int testno, const char* reason, unsigned lineno);
static void
success(int testno);

static bool
put(int testno,
    unsigned lineno,
    hyperclient::client* cl,
    size_t A, size_t B, size_t C);
static bool
del(int testno,
    unsigned lineno,
    hyperclient::client* cl,
    size_t A);
static bool
check(int testno,
      unsigned lineno,
      hyperclient::client* cl,
      size_t A, size_t B, size_t C);
static bool
absent(int testno,
       unsigned lineno,
       hyperclient::client* cl,
       size_t A);
static bool
cond_check(int testno,
           unsigned lineno,
           hyperclient::client* cl,
           size_t A, size_t B, size_t C)
{ return conditionals ? check(testno, lineno, cl, A, B, C) : true; }
static bool
cond_absent(int testno,
            unsigned lineno,
            hyperclient::client* cl,
            size_t A)
{ return conditionals ? absent(testno, lineno, cl, A) : true; }

static void
test1(hyperclient::client* cl);
static void
test2(hyperclient::client* cl);
static void
test3(hyperclient::client* cl);
static void
test4(hyperclient::client* cl);

class generator
{
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
        operator int () const { return nums[m_i]; }

    private:
        uint32_t m_i;
};

int
main(int argc, char* argv[])
{
    if (argc != 5)
    {
        return usage();
    }

    find_hashes();

    po6::net::ipaddr ip;
    uint16_t port;
    space = argv[3];

    try
    {
        ip = argv[1];
    }
    catch (std::invalid_argument& e)
    {
        std::cerr << "The IP address must be an IPv4 or IPv6 address." << std::endl;
        return EXIT_FAILURE;
    }

    try
    {
        port = e::convert::to_uint16_t(argv[2]);
    }
    catch (std::domain_error& e)
    {
        std::cerr << "The port number must be an integer." << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::out_of_range& e)
    {
        std::cerr << "The port number must be suitably small." << std::endl;
        return EXIT_FAILURE;
    }

    try
    {
        prefix = e::convert::to_uint8_t(argv[4]);
    }
    catch (std::domain_error& e)
    {
        std::cerr << "The prefix must be an integer." << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::out_of_range& e)
    {
        std::cerr << "The prefix must be suitably small." << std::endl;
        return EXIT_FAILURE;
    }

    try
    {
        // Set SIGALARM to not crash us.
        struct sigaction sa;
        sa.sa_handler = sigalarm;
        sigfillset(&sa.sa_mask);
        sa.sa_flags = 0;
        sigaction(SIGALRM, &sa, NULL);

        hyperclient::client cl(po6::net::location(ip, port));
        cl.connect();

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

int
usage()
{
    std::cerr << "Usage:  repl-tester <coordinator ip> <coordinator port> <space name> <prefix>"
              << std::endl;
    return EXIT_FAILURE;
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

    for (uint32_t value = 0; complete < 256; ++value)
    {
        e::buffer key;
        key.pack() << value;
        uint64_t high_byte = CityHash64(reinterpret_cast<char*>(&value), sizeof(value)) >> 56;

        if (!found[high_byte])
        {
            found[high_byte] = true;
            nums[high_byte] = value;
            ++ complete;
        }
    }
}

void
fail(int testno, const char* reason, unsigned lineno)
{
    printf("Test %2i:  [\x1b[31mFAIL\x1b[0m]\n", testno);
    printf("failure on line %u:  %s\n", lineno, reason);
}

void
success(int testno)
{
    printf("Test %2i:  [\x1b[32mOK\x1b[0m]\n", testno);
}

#define str(X) #X
#define xstr(X) str(X)

#define PUTFAILCASE(X) \
    case hyperclient::X: \
        snprintf(buf, 2048, "point = <%u %u %u> :: PUT returned %s.", \
                static_cast<unsigned int>(A), \
                static_cast<unsigned int>(B), \
                static_cast<unsigned int>(C), \
                xstr(X)); \
        fail(testno, buf, lineno); \
        return false;

static bool
put(int testno,
    unsigned lineno,
    hyperclient::client* cl,
    size_t A, size_t B, size_t C)
{
    e::buffer key;
    std::vector<e::buffer> value(2);
    key.pack() << A;
    value[0].pack() << B;
    value[1].pack() << C;
    char buf[2048];
    alarm(10);

    switch (cl->put(space, key, value))
    {
        case hyperclient::SUCCESS:
            return true;
        PUTFAILCASE(NOTFOUND)
        PUTFAILCASE(WRONGARITY)
        PUTFAILCASE(NOTASPACE)
        PUTFAILCASE(BADSEARCH)
        PUTFAILCASE(COORDFAIL)
        PUTFAILCASE(SERVERERROR)
        PUTFAILCASE(CONNECTFAIL)
        PUTFAILCASE(DISCONNECT)
        PUTFAILCASE(RECONFIGURE)
        PUTFAILCASE(LOGICERROR)
        default:
            return false;
    }
}

#define DELFAILCASE(X) \
    case hyperclient::X: \
        snprintf(buf, 2048, "key = %u :: DEL returned %s.", \
                static_cast<unsigned int>(A), \
                xstr(X)); \
        fail(testno, buf, lineno); \
        return false;

static bool
del(int testno,
    unsigned lineno,
    hyperclient::client* cl,
    size_t A)
{
    e::buffer key;
    key.pack() << A;

    char buf[2048];
    alarm(10);

    switch (cl->del(space, key))
    {
        case hyperclient::SUCCESS:
            return true;
        DELFAILCASE(NOTFOUND)
        DELFAILCASE(WRONGARITY)
        DELFAILCASE(NOTASPACE)
        DELFAILCASE(BADSEARCH)
        DELFAILCASE(COORDFAIL)
        DELFAILCASE(SERVERERROR)
        DELFAILCASE(CONNECTFAIL)
        DELFAILCASE(DISCONNECT)
        DELFAILCASE(RECONFIGURE)
        DELFAILCASE(LOGICERROR)
        default:
            return false;
    }
}

static bool
check(int testno,
      unsigned lineno,
      hyperclient::client* cl,
      size_t A, size_t B, size_t C)
{
    e::buffer key;
    std::vector<e::buffer> value(2);
    std::vector<e::buffer> gotten_value;
    key.pack() << A;
    value[0].pack() << B;
    value[1].pack() << C;
    alarm(10);

    switch (cl->get(space, key, &gotten_value))
    {
        case hyperclient::SUCCESS:
            return true;
        case hyperclient::NOTFOUND:
            fail(testno, "check returned NOTFOUND.", lineno);
            return false;
        case hyperclient::WRONGARITY:
            fail(testno, "check returned WRONGARITY.", lineno);
            return false;
        case hyperclient::NOTASPACE:
            fail(testno, "check returned NOTASPACE.", lineno);
            return false;
        case hyperclient::BADSEARCH:
            fail(testno, "check returned BADSEARCH.", lineno);
            return false;
        case hyperclient::COORDFAIL:
            fail(testno, "check returned COORDFAIL.", lineno);
            return false;
        case hyperclient::SERVERERROR:
            fail(testno, "check returned SERVERERROR.", lineno);
            return false;
        case hyperclient::CONNECTFAIL:
            fail(testno, "check returned CONNECTFAIL.", lineno);
            return false;
        case hyperclient::DISCONNECT:
            fail(testno, "check returned DISCONNECT.", lineno);
            return false;
        case hyperclient::RECONFIGURE:
            fail(testno, "check returned RECONFIGURE.", lineno);
            return false;
        case hyperclient::LOGICERROR:
            fail(testno, "check returned LOGICERROR (timeout?).", lineno);
            return false;
        default:
            return false;
    }

    return value != gotten_value;

#if 0
    std::map<std::string, e::buffer> terms;
    terms.insert(std::make_pair("B", e::buffer(&B, sizeof(uint32_t))));
    terms.insert(std::make_pair("C", e::buffer(&C, sizeof(uint32_t))));
    hyperclient::search_results sr;
    alarm(10);

    switch (cl->search(space, terms, &sr))
    {
        case hyperclient::SUCCESS:
            return true;
        case hyperclient::NOTFOUND:
            fail(testno, "check returned NOTFOUND.", lineno);
            return false;
        case hyperclient::WRONGARITY:
            fail(testno, "check returned WRONGARITY.", lineno);
            return false;
        case hyperclient::NOTASPACE:
            fail(testno, "check returned NOTASPACE.", lineno);
            return false;
        case hyperclient::BADSEARCH:
            fail(testno, "check returned BADSEARCH.", lineno);
            return false;
        case hyperclient::COORDFAIL:
            fail(testno, "check returned COORDFAIL.", lineno);
            return false;
        case hyperclient::SERVERERROR:
            fail(testno, "check returned SERVERERROR.", lineno);
            return false;
        case hyperclient::CONNECTFAIL:
            fail(testno, "check returned CONNECTFAIL.", lineno);
            return false;
        case hyperclient::DISCONNECT:
            fail(testno, "check returned DISCONNECT.", lineno);
            return false;
        case hyperclient::RECONFIGURE:
            fail(testno, "check returned RECONFIGURE.", lineno);
            return false;
        case hyperclient::LOGICERROR:
            fail(testno, "check returned LOGICERROR (timeout?).", lineno);
            return false;
        default:
            return false;
    }

    alarm(10);
    return value != gotten_value &&
           sr.valid() && sr.key() == key && sr.value() == value &&
           sr.next() == hyperclient::SUCCESS && !sr.valid();
#endif
}

static bool
absent(int testno,
       unsigned lineno,
       hyperclient::client* cl,
       size_t A)
{
    e::buffer key;
    std::vector<e::buffer> gotten_value;
    key.pack() << A;
    alarm(10);

    switch (cl->get(space, key, &gotten_value))
    {
        case hyperclient::SUCCESS:
            fail(testno, "absent returned NOTFOUND.", lineno);
            return false;
        case hyperclient::NOTFOUND:
            return true;
        case hyperclient::WRONGARITY:
            fail(testno, "absent returned WRONGARITY.", lineno);
            return false;
        case hyperclient::NOTASPACE:
            fail(testno, "absent returned NOTASPACE.", lineno);
            return false;
        case hyperclient::BADSEARCH:
            fail(testno, "absent returned BADSEARCH.", lineno);
            return false;
        case hyperclient::COORDFAIL:
            fail(testno, "absent returned COORDFAIL.", lineno);
            return false;
        case hyperclient::SERVERERROR:
            fail(testno, "absent returned SERVERERROR.", lineno);
            return false;
        case hyperclient::CONNECTFAIL:
            fail(testno, "absent returned CONNECTFAIL.", lineno);
            return false;
        case hyperclient::DISCONNECT:
            fail(testno, "absent returned DISCONNECT.", lineno);
            return false;
        case hyperclient::RECONFIGURE:
            fail(testno, "absent returned RECONFIGURE.", lineno);
            return false;
        case hyperclient::LOGICERROR:
            fail(testno, "absent returned LOGICERROR (timeout?).", lineno);
            return false;
        default:
            return false;
    }
}

// This test continually puts/deletes keys, ensuring that a [PUT, DEL] sequence
// of operations is run through every way in which one could select regions from
// the subspaces.  6 * 2**prefix requests will be performed.  This tests that
// delete plays nicely with the fresh bit.
void
test1(hyperclient::client* cl)
{
    for (generator A; A.has_next(); A.next())
    {
        for (generator B; B.has_next(); B.next())
        {
            for (generator C; C.has_next(); C.next())
            {
                if (!put(1, __LINE__, cl, A, B, C)) return;
                if (!cond_check(1, __LINE__, cl, A, B, C)) return;
                if (!del(1, __LINE__, cl, A)) return;
                if (!cond_absent(1, __LINE__, cl, A)) return;
            }
        }

        if (!absent(1, __LINE__, cl, A)) return;
    }

    success(1);
}

// This test puts keys such that A and B are fixed, but every choice of C for
// a fixed A, B is exercised.  Keys are deleted when changing A or B.  This
// tests that CHAIN_SUBSPACE messages work.  This tests that CHAIN_SUBSPACE
// messages work correctly.
void
test2(hyperclient::client* cl)
{
    for (generator A; A.has_next(); A.next())
    {
        for (generator B; B.has_next(); B.next())
        {
            if (!absent(2, __LINE__, cl, A)) return;

            for (generator C; C.has_next(); C.next())
            {
                if (!put(2, __LINE__, cl, A, B, C)) return;
                if (!cond_check(2, __LINE__, cl, A, B, C)) return;
            }

            if (!del(2, __LINE__, cl, A)) return;
            if (!absent(2, __LINE__, cl, A)) return;
        }
    }

    success(2);
}

// This is isomorphic to test2 except that columns A and C are fixed.
void
test3(hyperclient::client* cl)
{
    for (generator A; A.has_next(); A.next())
    {
        for (generator C; C.has_next(); C.next())
        {
            if (!absent(3, __LINE__, cl, A)) return;

            for (generator B; B.has_next(); B.next())
            {
                if (!put(3, __LINE__, cl, A, B, C)) return;
                if (!cond_check(3, __LINE__, cl, A, B, C)) return;
            }

            if (!del(3, __LINE__, cl, A)) return;
            if (!absent(3, __LINE__, cl, A)) return;
        }
    }

    success(3);
}

// This test stresses the interaction of CHAIN_SUBSPACE with DELETE messages.
// For each A, B, C pair, it creates a point.  It then issues a PUT which causes
// B and C to jump to another subspace.  It then issues a DEL to try to create a
// deferred update.
void
test4(hyperclient::client* cl)
{
    for (generator A; A.has_next(); A.next())
    {
        for (generator B; B.has_next(); B.next())
        {
            for (generator C; C.has_next(); C.next())
            {
                for (generator BP; BP.has_next(); BP.next())
                {
                    for (generator CP; CP.has_next(); CP.next())
                    {
                        if (B != BP && C != CP)
                        {
                            if (!put(4, __LINE__, cl, A, B, C)) return;
                            if (!cond_check(4, __LINE__, cl, A, B, C)) return;
                            if (!put(4, __LINE__, cl, A, BP, CP)) return;
                            if (!del(4, __LINE__, cl, A)) return;
                            if (!cond_absent(4, __LINE__, cl, A)) return;
                        }
                    }

                    if (!absent(4, __LINE__, cl, A)) return;
                }
            }
        }
    }

    success(4);
}
