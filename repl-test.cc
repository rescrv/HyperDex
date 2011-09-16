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

// STL
#include <tr1/functional>
#include <memory>
#include <string>

// po6
#include <po6/net/ipaddr.h>
#include <po6/net/location.h>

#include <glog/logging.h>

// e
#include <e/bitfield.h>
#include <e/convert.h>

// HyperspaceHashing
#include <hyperspacehashing/hashes.h>
#include <hyperspacehashing/prefix.h>

// HyperDex
#include <hyperclient/client.h>

// These 32-bit values all hash (using CityHash) to be have the same high-order
// byte as their index.  E.g. index 0 has a hash of 0x00??????????????, while
// index 255 has a hash of 0xff??????????????.
static uint32_t nums[256];

std::string space;
size_t prefix;
bool failed = false;

static int
usage();

static void
find_hashes();

static void
test0(hyperclient::client* cl);
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
        hyperclient::client cl(po6::net::location(ip, port));
        cl.connect();

        if (!failed) test0(&cl);
        if (!failed) test1(&cl);
        if (!failed) test2(&cl);
        if (!failed) test3(&cl);
        if (!failed) test4(&cl);
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

    e::bitfield dims(1);
    dims.set(0);
    std::vector<hyperspacehashing::hash_t> hashes(1, hyperspacehashing::CITYHASH);
    hyperspacehashing::prefix::hasher hasher(dims, hashes);

    for (uint32_t value = 0; complete < 256; ++value)
    {
        e::buffer key;
        key.pack() << value;
        uint64_t hash = hasher.hash(key).point;
        int high_byte = (hash >> 56) & 0xff;

        if (!found[high_byte])
        {
            found[high_byte] = true;
            nums[high_byte] = value;
            ++ complete;
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
fail(int testno, const char* reason)
{
    failed = true;
    printf("Test %2i:  [\x1b[31mFAIL\x1b[0m]\n", testno);
    printf("failure:  %s\n", reason);
}

static void
success(int testno)
{
    printf("Test %2i:  [\x1b[32mOK\x1b[0m]\n", testno);
}

static size_t
reverse(size_t num)
{
    size_t i = 0;

    for (; i < 256; ++i)
    {
        if (nums[i] == num)
        {
            return i;
        }
    }

    assert(false);
}

static void
handle_put(int testno, bool* succeeded, size_t A, size_t B, size_t C, hyperclient::returncode ret)
{
    if (ret == hyperclient::SUCCESS)
    {
        return;
    }

    if (*succeeded)
    {
        *succeeded = false;
        std::ostringstream ostr;
        ostr << "key = " << reverse(A) << " ; value = <" << reverse(B) << ", " << reverse(C) << "> :: PUT returned " << ret;
        fail(testno, ostr.str().c_str());
    }
}

static void
put(int testno,
    hyperclient::client* cl,
    bool* succeeded,
    size_t A, size_t B, size_t C)
{
    e::buffer key;
    std::vector<e::buffer> value(2);
    key.pack() << A;
    value[0].pack() << B;
    value[1].pack() << C;
    using std::tr1::bind;
    using std::tr1::placeholders::_1;
    cl->put(space, key, value, std::tr1::bind(handle_put, testno, succeeded, A, B, C, _1));
}

static void
handle_del(int testno, bool* succeeded, size_t A, hyperclient::returncode ret)
{
    if (ret == hyperclient::SUCCESS)
    {
        return;
    }

    if (*succeeded)
    {
        *succeeded = false;
        std::ostringstream ostr;
        ostr << "key = " << reverse(A) << " :: DEL returned " << ret;
        fail(testno, ostr.str().c_str());
    }
}

static void
del(int testno,
    hyperclient::client* cl,
    bool* succeeded,
    size_t A)
{
    e::buffer key;
    key.pack() << A;
    using std::tr1::bind;
    using std::tr1::placeholders::_1;
    cl->del(space, key, std::tr1::bind(handle_del, testno, succeeded, A, _1));
}

void
handle_get(hyperclient::returncode* store_ret, std::vector<e::buffer>* store_value,
           hyperclient::returncode ret, const std::vector<e::buffer>& value)
{
    *store_ret = ret;
    *store_value = value;
}


static bool
check(int testno,
      hyperclient::client* cl,
      bool* succeeded,
      size_t A, size_t B, size_t C)
{
    cl->flush((1 << prefix) * 1000);

    if (!*succeeded)
    {
        return false;
    }

    e::buffer key;
    key.pack() << A;
    hyperclient::returncode ret;
    std::vector<e::buffer> ret_value(2);
    std::vector<e::buffer> exp_value(2);
    exp_value[0].pack() << B;
    exp_value[1].pack() << C;
    using std::tr1::bind;
    using std::tr1::placeholders::_1;
    using std::tr1::placeholders::_2;
    cl->get(space, key, bind(handle_get, &ret, &ret_value, _1, _2));
    cl->flush(1000);

    if (ret != hyperclient::SUCCESS)
    {
        std::ostringstream ostr;
        ostr << "key = " << reverse(A) << " :: CHECK returned " << ret;
        fail(testno, ostr.str().c_str());
        return false;
    }

    if (ret_value != exp_value)
    {
        std::ostringstream ostr;
        ostr << "key = " << reverse(A) << " ; value = <"
             << reverse(B) << ", " << reverse(C)
             << ">\t:: CHECK returned wrong value exp<"
             << exp_value[0].hex() << ", " << exp_value[1].hex() << "> ret<"
             << ret_value[0].hex() << ", " << ret_value[1].hex() << ">";
        fail(testno, ostr.str().c_str());
        return false;
    }

    // XXX Do an empty search in each subspace.
    return true;
}

static bool
absent(int testno,
       hyperclient::client* cl,
       bool* succeeded,
       size_t A)
{
    cl->flush((1 << prefix) * 1000);

    if (!*succeeded)
    {
        return false;
    }

    e::buffer key;
    key.pack() << A;
    hyperclient::returncode ret;
    std::vector<e::buffer> ret_value(2);
    using std::tr1::bind;
    using std::tr1::placeholders::_1;
    using std::tr1::placeholders::_2;
    cl->get(space, key, bind(handle_get, &ret, &ret_value, _1, _2));
    cl->flush(1000);

    if (ret == hyperclient::SUCCESS)
    {
        std::ostringstream ostr;
        ostr << "key = " << reverse(A) << "\t:: ABSENT returned <"
             << ret_value[0].hex() << ", " << ret_value[1].hex() << ">";
        fail(testno, ostr.str().c_str());
        return false;
    }

    if (ret != hyperclient::NOTFOUND)
    {
        std::ostringstream ostr;
        ostr << "key = " << reverse(A) << " :: ABSENT returned " << ret;
        fail(testno, ostr.str().c_str());
        return false;
    }

    // XXX Do an empty search in each subspace.
    return true;
}

// This test continually puts keys, ensuring that every way in which one could
// select regions from the subspaces is chosen.
void
test0(hyperclient::client* cl)
{
    bool succeeded = true;

    for (generator A; A.has_next(); A.next())
    {
        if (!absent(0, cl, &succeeded, A)) return;

        for (generator B; B.has_next(); B.next())
        {
            for (generator C; C.has_next(); C.next())
            {
                put(0, cl, &succeeded, A, B, C);
            }
        }

        if (!check(0, cl, &succeeded, A, generator::end(), generator::end())) return;
        del(0, cl, &succeeded, A);
        if (!absent(0, cl, &succeeded, A)) return;
    }

    success(0);
}

// This test continually puts/deletes keys, ensuring that a [PUT, DEL] sequence
// of operations is run through every way in which one could select regions from
// the subspaces.  6 * 2**prefix requests will be performed.  This tests that
// delete plays nicely with the fresh bit.
void
test1(hyperclient::client* cl)
{
    bool succeeded = true;

    for (generator A; A.has_next(); A.next())
    {
        if (!absent(1, cl, &succeeded, A)) return;

        for (generator B; B.has_next(); B.next())
        {
            for (generator C; C.has_next(); C.next())
            {
                put(1, cl, &succeeded, A, C, C);
                del(1, cl, &succeeded, A);
            }
        }

        if (!absent(1, cl, &succeeded, A)) return;
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
    bool succeeded = true;

    for (generator A; A.has_next(); A.next())
    {
        for (generator B; B.has_next(); B.next())
        {
            if (!absent(2, cl, &succeeded, A)) return;

            for (generator C; C.has_next(); C.next())
            {
                put(2, cl, &succeeded, A, B, C);
            }

            if (!check(2, cl, &succeeded, A, B, generator::end())) return;
            del(2, cl, &succeeded, A);
            if (!absent(2, cl, &succeeded, A)) return;
        }
    }

    success(2);
}

// This is isomorphic to test2 except that columns A and C are fixed.
void
test3(hyperclient::client* cl)
{
    bool succeeded = true;

    for (generator A; A.has_next(); A.next())
    {
        for (generator C; C.has_next(); C.next())
        {
            if (!absent(3, cl, &succeeded, A)) return;

            for (generator B; B.has_next(); B.next())
            {
                put(3, cl, &succeeded, A, B, C);
            }

            if (!check(3, cl, &succeeded, A, generator::end(), C)) return;
            del(3, cl, &succeeded, A);
            if (!absent(3, cl, &succeeded, A)) return;
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
    bool succeeded = true;

    for (generator A; A.has_next(); A.next())
    {
        for (generator B; B.has_next(); B.next())
        {
            for (generator C; C.has_next(); C.next())
            {
                if (!absent(4, cl, &succeeded, A)) return;

                for (generator BP; BP.has_next(); BP.next())
                {
                    for (generator CP; CP.has_next(); CP.next())
                    {
                        if (B != BP && C != CP)
                        {
                            put(4, cl, &succeeded, A, B, C);
                            put(4, cl, &succeeded, A, BP, CP);
                            del(4, cl, &succeeded, A);
                        }
                    }
                }

                if (!absent(4, cl, &succeeded, A)) return;
            }
        }
    }

    success(4);
}
