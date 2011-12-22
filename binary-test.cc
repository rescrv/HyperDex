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

// STL
#include <memory>
#include <string>

// po6
#include <po6/net/ipaddr.h>
#include <po6/net/location.h>

// e
#include <e/convert.h>
#include <e/timer.h>

// HyperDex
#include <hyperclient/client.h>

const char* colnames[] = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
                          "11", "12", "13", "14", "15", "16", "17", "18", "19",
                          "20", "21", "22", "23", "24", "25", "26", "27", "28",
                          "29", "30", "31", "32"};

static int
usage();

void
handle_put(hyperclient::returncode ret)
{
    if (ret != hyperclient::SUCCESS)
    {
        std::cerr << "Put returned " << ret << "." << std::endl;
    }
}

void
handle_search(size_t* count,
              const e::buffer& expected_key,
              hyperclient::returncode ret,
              const e::buffer& key,
              const std::vector<e::buffer>& value)
{
    if (ret != hyperclient::SUCCESS)
    {
        std::cerr << "Equality returned !SUCCESS:  " << ret << std::endl;
        return;
    }

    ++*count;

    if (expected_key != key)
    {
        std::cerr << "Equality returned unexpected key:  "
                  << expected_key.size() << ":" << expected_key.hex()
                  << " != " << key.size() << ":" << key.hex() << std::endl;
    }
}

void
handle_range(size_t* count,
             const e::buffer& expected_key,
             hyperclient::returncode ret,
             const e::buffer& key,
             const std::vector<e::buffer>& value)
{
    if (ret != hyperclient::SUCCESS)
    {
        std::cerr << "Range returned !SUCCESS:  " << ret << std::endl;
        return;
    }

    ++*count;

    if (expected_key != key)
    {
        std::cerr << "Range returned unexpected key:  "
                  << expected_key.size() << ":" << expected_key.hex()
                  << " != " << key.size() << ":" << key.hex() << std::endl;
    }
}

int
main(int argc, char* argv[])
{
    if (argc != 5)
    {
        return usage();
    }

    po6::net::ipaddr ip;
    uint16_t port;
    std::string space = argv[3];
    uint32_t numbers;

    try
    {
        ip = po6::net::ipaddr(argv[1]);
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
        numbers = e::convert::to_uint32_t(argv[4]);
    }
    catch (std::domain_error& e)
    {
        std::cerr << "The number must be an integer." << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::out_of_range& e)
    {
        std::cerr << "The number must be suitably small." << std::endl;
        return EXIT_FAILURE;
    }

    try
    {
        hyperclient::client cl(po6::net::location(ip, port));
        cl.connect();
        e::buffer one("one", 3);
        e::buffer zero("zero", 4);

        for (uint32_t num = 0; num < numbers; ++num)
        {
            e::buffer key;
            key.pack() << num;
            std::vector<e::buffer> value;

            for (size_t i = 0; i < 32; ++i)
            {
                value.push_back(e::buffer());

                if ((num & (1 << i)))
                {
                    value.back() = one;
                }
                else
                {
                    value.back() = zero;
                }
            }

            char buf[4];
            memmove(buf, &num, sizeof(4));
            value.push_back(e::buffer(buf, 4));

            cl.put(space, key, value, handle_put);

            if (cl.outstanding() > 1000)
            {
                cl.flush(-1);
            }
        }

        cl.flush(-1);
        e::sleep_ms(1, 0);
        std::cerr << "Starting searches." << std::endl;
        timespec start;
        timespec end;

        clock_gettime(CLOCK_REALTIME, &start);

        for (uint32_t num = 0; num < numbers; ++num)
        {
            e::buffer key;
            key.pack() << num;
            std::map<std::string, e::buffer> search;

            for (size_t i = 0; i < 32; ++i)
            {
                if ((num & (1 << i)))
                {
                    search.insert(std::make_pair(colnames[i], one));
                }
                else
                {
                    search.insert(std::make_pair(colnames[i], zero));
                }
            }

            using std::tr1::bind;
            using std::tr1::placeholders::_1;
            using std::tr1::placeholders::_2;
            using std::tr1::placeholders::_3;
            size_t count = 0;
            cl.search(argv[3], search, std::tr1::bind(handle_search, &count, key, _1, _2, _3));
            cl.flush(-1);

            if (count < 1)
            {
                std::cerr << "Equality returned less than 1 result." << std::endl;
            }

            if (count > 1)
            {
                std::cerr << "Equality returned more than 1 result." << std::endl;
            }

            count = 0;
            std::map<std::string, std::pair<uint64_t, uint64_t> > range;
            range.insert(std::make_pair("range", std::make_pair(num, num + 1)));
            cl.search(argv[3], range, std::tr1::bind(handle_range, &count, key, _1, _2, _3));
            cl.flush(-1);

            if (count < 1)
            {
                std::cerr << "Range returned less than 1 result." << std::endl;
            }

            if (count > 1)
            {
                std::cerr << "Range returned more than 1 result." << std::endl;
            }
        }

        clock_gettime(CLOCK_REALTIME, &end);
        timespec diff;

        if ((end.tv_nsec < start.tv_nsec) < 0)
        {
            diff.tv_sec = end.tv_sec - start.tv_sec - 1;
            diff.tv_nsec = 1000000000 + end.tv_nsec - start.tv_nsec;
        }
        else
        {
            diff.tv_sec = end.tv_sec - start.tv_sec;
            diff.tv_nsec = end.tv_nsec - start.tv_nsec;
        }

        uint64_t nanosecs = diff.tv_sec * 1000000000 + diff.tv_nsec;
        std::cerr << "test took " << nanosecs << " nanoseconds for " << numbers << " searches" << std::endl;
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
    std::cerr << "Usage:  binary <coordinator ip> <coordinator port> <space name> <numbers>\n"
              << "This will create <numbers> points whose key is a number [0, <numbers>) and "
              << "then perform searches over the bits of the number.  The space should have 32 "
              << "secondary dimensions so that all bits of a number may be stored."
              << std::endl;
    return EXIT_FAILURE;
}
