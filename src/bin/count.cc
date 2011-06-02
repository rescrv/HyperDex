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
#include <string>

// po6
#include <po6/net/ipaddr.h>
#include <po6/net/location.h>

// e
#include <e/convert.h>

// HyperDex
#include <hyperdex/client.h>

static int
usage();

int
main(int argc, char* argv[])
{
    if (argc != 6)
    {
        return usage();
    }

    po6::net::ipaddr ip;
    uint16_t port;
    std::string space = argv[3];
    uint32_t loopiter;
    std::string payload;

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
        loopiter = e::convert::to_uint32_t(argv[4]);
    }
    catch (std::domain_error& e)
    {
        std::cerr << "The loop iteration count must be an integer." << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::out_of_range& e)
    {
        std::cerr << "The loop iteration count must be suitably small." << std::endl;
        return EXIT_FAILURE;
    }

    try
    {
        payload = std::string(e::convert::to_uint32_t(argv[5]), 'A');
    }
    catch (std::domain_error& e)
    {
        std::cerr << "The payload size must be an integer." << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::out_of_range& e)
    {
        std::cerr << "The payload size must be suitably small." << std::endl;
        return EXIT_FAILURE;
    }

    try
    {
        hyperdex::client cl(po6::net::location(ip, port));
        timespec start;
        timespec end;

        clock_gettime(CLOCK_REALTIME, &start);

        for (uint64_t i = 1; i <= loopiter; ++i)
        {
            e::buffer key;
            key.pack() << i;
            std::vector<e::buffer> val;
            val.push_back(e::buffer(payload.c_str(), payload.size()));

            switch (cl.put(space, key, val))
            {
                case hyperdex::SUCCESS:
                    break;
                case hyperdex::NOTFOUND:
                    std::cerr << "Put returned NOTFOUND." << std::endl;
                    break;
                case hyperdex::INVALID:
                    std::cerr << "Put returned INVALID." << std::endl;
                    break;
                case hyperdex::ERROR:
                    std::cerr << "Put returned ERROR." << std::endl;
                    break;
                default:
                    std::cerr << "Put returned unknown status." << std::endl;
                    break;
            }

            val.clear();

            switch (cl.get(space, key, &val))
            {
                case hyperdex::SUCCESS:
                    break;
                case hyperdex::NOTFOUND:
                    std::cerr << "Get returned NOTFOUND." << std::endl;
                    break;
                case hyperdex::INVALID:
                    std::cerr << "Get returned INVALID." << std::endl;
                    break;
                case hyperdex::ERROR:
                    std::cerr << "Get returned ERROR." << std::endl;
                    break;
                default:
                    std::cerr << "Get returned unknown status." << std::endl;
                    break;
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
        std::cerr << "test took " << nanosecs << " nanoseconds for " << loopiter << " PUT/GET pairs" << std::endl;
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
    std::cerr << "Usage:  count <coordinator ip> <coordinator port> <space name> <loop iter> <payload size (b)>"
              << std::endl;
    return EXIT_FAILURE;
}
