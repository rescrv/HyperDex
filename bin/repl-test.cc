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

// The purpose of this test is to expose any errors in the replication logic.
// It creates a simple space of three dimensions with three subspaces.  Each
// dimension is used to construct one of the subspaces.  Specially-crafted
// requests are sent to test patterns in which updates could change points in
// the space.
//
// The tests are described in pseudocode.  The following utilties are defined in
// the tests:
//
// check(A, B, C):
//      [A, B, C] == get(A) &&
//      [[A, B, C]] == search({"B": B}) &&
//      [[A, B, C]] == search({"C": C})
//
// absent(A, B, C):
//      NOTFOUND == get(A) &&
//      NOTFOUND == search({"B": B}) &&
//      NOTFOUND == search({"C": C})
//
//
// Test 1:  PUT/DEL.
//  for A in range(256):
//      for B in range(256):
//          for C in range(256):
//              SUCCESS == put([A, B, C])
//              check(A, B, C)
//              SUCCESS == del(A)
//              sleep 100us
//              absent(A, B, C)
//
// Test 3:  Interleave PUT and DEL operations in a way that the two PUTs on
// either side of the DEL map to the same region in B.
//
//  for A in range(256):
//      for B in range(256):
//          for C in range(256):
//              SUCCESS == put([A, B, C])
//              check(A, B, C)
//              SUCCESS == del(A)
//              sleep 100us
//              absent(A, B, C)
//          SUCCESS == del(A) // reset
//
// Test 4:  Interleave PUT and DEL operations in a way that the two PUTs on
// either side of the DEL map to the same region in C.
//
//  for A in range(256):
//      for C in range(256):
//          for B in range(256):
//              SUCCESS == put([A, B, C])
//              check(A, B, C)
//              SUCCESS == del(A)
//              sleep 100us
//              absent(A, B, C)
//          SUCCESS == del(A) // reset
//
// Test 5:  Interleave PUT and DEL operations in a way that the two PUTs on
// either side of the DEL do not map to the same hosts in B or C.
//
//  for A in range(256):
//      for i in range(256):
//          SUCCESS == put([A, i, i])
//          check(A, i, i)
//          SUCCESS == del(A)
//          sleep 100us
//          absent(A, i, i)
//      SUCCESS == del(A)
//      for i in range(256):
//          SUCCESS == put([A, i, 256 - i - 1])
//          check(A, i, 256 - i - 1)
//          SUCCESS == del(A)
//          sleep 100us
//          absent(A, i, 256 - i - 1)
//      SUCCESS == del(A)
//
// Test 6:  Send successive PUT operations in a way that A, B are fixed for any
// given point, but C is cycled through all options.
//
//  for A in range(256):
//      for B in range(256):
//          for C in range(256):
//              SUCCESS == put([A, B, C])
//              check(A, B, C)
//          SUCCESS == del(A) // reset
//          absent(A, B, C)
//
// Test 7:  Send successive PUT operations in a way that A, C are fixed for any
// given point, but B is cycled through all regions.
//
//  for A in range(256):
//      for C in range(256):
//          for B in range(256):
//              SUCCESS == put([A, B, C])
//              check(A, B, C)
//          SUCCESS == del(A) // reset
//          absent(A, B, C)
//
// Test 8:  Send successive PUT operations in a way that A is fixed for any
// given point, but each point changes to a different B and a different C on
// each update.
//
//  for A in range(256):
//      for i in range(256):
//          SUCCESS == put([A, i, i])
//          check(A, i, i)
//      for i in range(256):
//          SUCCESS == put([A, i, 256 - i - 1])
//          check(A, i, 256 - i - 1)
//      SUCCESS == del(A)

// C
#include <cstdio>

// POSIX
#include <signal.h>

// STL
#include <string>

// po6
#include <po6/net/ipaddr.h>
#include <po6/net/location.h>

#include <glog/logging.h>

// e
#include <e/convert.h>

// HyperDex
#include <hyperclient/client.h>

#define MAX_A 256
#define MAX_B 256
#define MAX_C 256
#define MAX_I 256
#define INC_A 1
#define INC_B 1
#define INC_C 1
#define INC_I 1

// These 32-bit values all hash (using CityHash) to be have the same high-order
// byte as their index.  E.g. index 0 has a hash of 0x00??????????????, while
// index 255 has a hash of 0xff??????????????.
static uint32_t nums[] = {8, 67, 535, 199, 381, 136, 1, 501, 260, 60, 386, 42,
    250, 22, 221, 1000, 139, 559, 84, 113, 230, 726, 143, 31, 88, 257, 315, 478,
    394, 108, 207, 5, 50, 85, 124, 142, 533, 659, 58, 213, 118, 321, 240, 96,
    335, 480, 116, 101, 132, 681, 342, 309, 127, 419, 26, 248, 482, 87, 111, 10,
    4, 355, 137, 112, 637, 448, 229, 370, 194, 224, 148, 220, 959, 125, 325, 98,
    177, 208, 645, 188, 47, 470, 1237, 19, 68, 0, 52, 6, 66, 79, 105, 214, 21,
    491, 459, 483, 238, 388, 474, 129, 15, 158, 23, 92, 59, 2, 1234, 333, 1064,
    502, 431, 53, 126, 77, 150, 175, 9, 32, 36, 305, 382, 73, 521, 655, 222,
    279, 376, 270, 164, 563, 162, 682, 508, 278, 43, 14, 25, 242, 433, 86, 963,
    204, 29, 13, 138, 202, 33, 119, 430, 78, 154, 195, 146, 169, 301, 76, 255,
    45, 49, 1120, 275, 435, 495, 30, 379, 402, 160, 225, 569, 344, 415, 746,
    1221, 95, 41, 441, 580, 51, 165, 764, 83, 40, 348, 651, 120, 184, 11, 331,
    56, 539, 271, 65, 316, 244, 3, 635, 467, 28, 211, 436, 261, 135, 7, 578, 81,
    54, 471, 141, 300, 12, 813, 82, 445, 64, 284, 527, 44, 306, 456, 272, 24,
    55, 167, 354, 16, 48, 156, 35, 317, 149, 191, 253, 273, 157, 17, 605, 170,
    20, 74, 773, 63, 662, 330, 134, 122, 246, 27, 131, 39, 71, 109, 189, 396,
    364, 46, 163};

std::string space;
size_t prefix;

static void
sigalarm(int) {}

static int
usage();

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
       size_t A, size_t B, size_t C);

static void
test1(hyperclient::client* cl);
static void
test3(hyperclient::client* cl);
static void
test4(hyperclient::client* cl);
static void
test5(hyperclient::client* cl);
static void
test6(hyperclient::client* cl);
static void
test7(hyperclient::client* cl);
static void
test8(hyperclient::client* cl);

int
main(int argc, char* argv[])
{
    if (argc != 5)
    {
        return usage();
    }

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
        struct sigaction sa;
        sa.sa_handler = sigalarm;
        sigfillset(&sa.sa_mask);
        sa.sa_flags = 0;
        sigaction(SIGALRM, &sa, NULL);

        hyperclient::client cl(po6::net::location(ip, port));
        cl.connect();

        test1(&cl);
        test3(&cl);
        test4(&cl);
        test5(&cl);
        test6(&cl);
        test7(&cl);
        test8(&cl);
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
    key.pack() << nums[A];
    value[0].pack() << nums[B];
    value[1].pack() << nums[C];
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
    key.pack() << nums[A];
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
    key.pack() << nums[A];
    value[0].pack() << nums[B];
    value[1].pack() << nums[C];
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
}

static bool
absent(int testno,
       unsigned lineno,
       hyperclient::client* cl,
       size_t A, size_t B, size_t C)
{
    e::buffer key;
    std::vector<e::buffer> gotten_value;
    key.pack() << nums[A];
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

void
test1(hyperclient::client* cl)
{
    for (size_t A = 0; A < MAX_A; A += INC_A)
    {
        for (size_t B = 0; B < MAX_B; B += INC_B)
        {
            for (size_t C = 0; C < MAX_C; C += INC_C)
            {
                if (!put(1, __LINE__, cl, A, B, C))
                {
                    return;
                }

                if (!check(1, __LINE__, cl, A, B, C))
                {
                    return;
                }

                if (!del(1, __LINE__, cl, A))
                {
                    return;
                }

                if (!absent(1, __LINE__, cl, A, B, C))
                {
                    return;
                }
            }
        }
    }

    success(1);
}

void
test3(hyperclient::client* cl)
{
    for (size_t A = 0; A < MAX_A; A += INC_A)
    {
        for (size_t B = 0; B < MAX_B; B += INC_B)
        {
            for (size_t C = 0; C < MAX_C; C += INC_C)
            {
                if (!put(3, __LINE__, cl, A, B, C))
                {
                    return;
                }

                if (!check(3, __LINE__, cl, A, B, C))
                {
                    return;
                }

                if (!del(3, __LINE__, cl, A))
                {
                    return;
                }

                if (!absent(3, __LINE__, cl, A, B, C))
                {
                    return;
                }
            }
        }
    }

    success(3);
}

void
test4(hyperclient::client* cl)
{
    for (size_t A = 0; A < MAX_A; A += INC_A)
    {
        for (size_t C = 0; C < MAX_C; C += INC_C)
        {
            for (size_t B = 0; B < MAX_B; B += INC_B)
            {
                if (!put(4, __LINE__, cl, A, B, C))
                {
                    return;
                }

                if (!check(4, __LINE__, cl, A, B, C))
                {
                    return;
                }

                if (!del(4, __LINE__, cl, A))
                {
                    return;
                }

                if (!absent(4, __LINE__, cl, A, B, C))
                {
                    return;
                }
            }
        }
    }

    success(4);
}

void
test5(hyperclient::client* cl)
{
    for (size_t A = 0; A < MAX_A; A += INC_A)
    {
        for (size_t I = 0; I < MAX_I; I += INC_I)
        {
            if (!put(5, __LINE__, cl, A, I, I))
            {
                return;
            }

            if (!check(5, __LINE__, cl, A, I, I))
            {
                return;
            }

            if (!del(5, __LINE__, cl, A))
            {
                return;
            }

            if (!absent(5, __LINE__, cl, A, I, I))
            {
                return;
            }
        }

        for (size_t I = 0; I < MAX_I; I += INC_I)
        {
            if (!put(5, __LINE__, cl, A, I, 256 - I - 1))
            {
                return;
            }

            if (!check(5, __LINE__, cl, A, I, 256 - I - 1))
            {
                return;
            }

            if (!del(5, __LINE__, cl, A))
            {
                return;
            }

            if (!absent(5, __LINE__, cl, A, I, 256 - I - 1))
            {
                return;
            }
        }
    }

    success(5);
}

void
test6(hyperclient::client* cl)
{
    for (size_t A = 0; A < MAX_A; A += INC_A)
    {
        for (size_t B = 0; B < MAX_B; B += INC_B)
        {
            for (size_t C = 0; C < MAX_C; C += INC_C)
            {
                if (!put(6, __LINE__, cl, A, B, C))
                {
                    return;
                }

                if (!check(6, __LINE__, cl, A, B, C))
                {
                    return;
                }
            }

            if (!del(6, __LINE__, cl, A))
            {
                return;
            }
        }
    }

    success(6);
}

void
test7(hyperclient::client* cl)
{
    for (size_t A = 0; A < MAX_A; A += INC_A)
    {
        for (size_t C = 0; C < MAX_C; C += INC_C)
        {
            for (size_t B = 0; B < MAX_B; B += INC_B)
            {
                if (!put(7, __LINE__, cl, A, B, C))
                {
                    return;
                }

                if (!check(7, __LINE__, cl, A, B, C))
                {
                    return;
                }
            }

            if (!del(7, __LINE__, cl, A))
            {
                return;
            }
        }
    }

    success(7);
}

void
test8(hyperclient::client* cl)
{
    for (size_t A = 0; A < MAX_A; A += INC_A)
    {
        for (size_t I = 0; I < MAX_I; I += INC_I)
        {
            if (!put(8, __LINE__, cl, A, I, I))
            {
                return;
            }

            if (!check(8, __LINE__, cl, A, I, I))
            {
                return;
            }
        }

        for (size_t I = 0; I < MAX_I; I += INC_I)
        {
            if (!put(8, __LINE__, cl, A, I, 256 - I - 1))
            {
                return;
            }

            if (!check(8, __LINE__, cl, A, I, 256 - I - 1))
            {
                return;
            }
        }
    }

    success(8);
}
