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
#include <e/endian.h>
#include <e/timer.h>

// HyperDex
#include "hyperclient/hyperclient.h"

const char* colnames[] = {"1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
                          "11", "12", "13", "14", "15", "16", "17", "18", "19",
                          "20", "21", "22", "23", "24", "25", "26", "27", "28",
                          "29", "30", "31", "32"};

static int
usage();

static void
validate_search(hyperclient* cl,
                uint32_t num,
                const char* space,
                const struct hyperclient_attribute* eq,
                size_t eq_sz,
                const struct hyperclient_range_query* rn,
                size_t rn_sz,
                const char* type);

int
main(int argc, char* argv[])
{
    if (argc != 5)
    {
        return usage();
    }

    const char* ip;
    uint16_t port;
    const char* space = argv[3];
    uint32_t numbers;

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
        hyperclient cl(ip, port);

        for (uint32_t num = 0; num < numbers; ++num)
        {
            hyperclient_attribute attrs[32];

            for (size_t i = 0; i < 32; ++i)
            {
                attrs[i].attr = colnames[i];
                attrs[i].datatype = HYPERDATATYPE_STRING;

                if ((num & (1 << i)))
                {
                    attrs[i].value = "ONE";
                    attrs[i].value_sz = 3;
                }
                else
                {
                    attrs[i].value = "ZERO";
                    attrs[i].value_sz = 4;
                }
            }

            uint8_t bufle[sizeof(uint64_t)];
            e::pack64le(num, bufle);
            hyperclient_returncode pstatus;
            int64_t pid = cl.put(space, reinterpret_cast<const char*>(bufle), sizeof(uint32_t), attrs, 32, &pstatus);

            if (pid < 0)
            {
                std::cerr << "put error " << pstatus << " " << (-1 - pid) << std::endl;
                return EXIT_FAILURE;
            }

            hyperclient_returncode lstatus;
            int64_t lid = cl.loop(-1, &lstatus);

            if (pid != lid)
            {
                std::cerr << "loop error " << pstatus << std::endl;
                return EXIT_FAILURE;
            }
        }

        std::cerr << "Starting searches." << std::endl;
        e::stopwatch stopw;
        stopw.start();

        for (uint32_t num = 0; num < numbers; ++num)
        {
            hyperclient_attribute attrs[32];

            for (size_t i = 0; i < 32; ++i)
            {
                attrs[i].attr = colnames[i];
                attrs[i].datatype = HYPERDATATYPE_STRING;

                if ((num & (1 << i)))
                {
                    attrs[i].value = "ONE";
                    attrs[i].value_sz = 3;
                }
                else
                {
                    attrs[i].value = "ZERO";
                    attrs[i].value_sz = 4;
                }
            }

            validate_search(&cl, num, space, attrs, 32, NULL, 0, "equality");

            hyperclient_range_query rn;
            rn.attr = "num";
            rn.lower = num;
            rn.upper = num + 1;

            validate_search(&cl, num, space, NULL, 0, &rn, 1, "range");
        }

        std::cerr << "test took " << stopw.peek() << " nanoseconds for " << numbers << " searches" << std::endl;
    }
    catch (po6::error& e)
    {
        std::cerr << "There was a system error:  " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::runtime_error& e)
    {
        std::cerr << "There was a runtime error:  " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::bad_alloc& e)
    {
        std::cerr << "There was a memory allocation error:  " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::exception& e)
    {
        std::cerr << "There was a generic error:  " << e.what() << std::endl;
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

void
validate_search(hyperclient* cl,
                uint32_t num,
                const char* space,
                const struct hyperclient_attribute* eq,
                size_t eq_sz,
                const struct hyperclient_range_query* rn,
                size_t rn_sz,
                const char* type)
{
    hyperclient_returncode sstatus;
    hyperclient_attribute* attrs;
    size_t attrs_sz;
    int64_t sid = cl->search(space, eq, eq_sz, rn, rn_sz, &sstatus, &attrs, &attrs_sz);

    if (sid < 0)
    {
        std::cerr << "invalid " << type << " search returns(" << sid << ", " << sstatus << ")" << std::endl;
        abort();
    }

    hyperclient_returncode lstatus;
    int64_t lid = cl->loop(-1, &lstatus);

    if (lid < 0)
    {
        std::cerr << type << " search's loop returns(" << lid << ", " << lstatus << ")" << std::endl;
        abort();
    }

    if (lid != sid)
    {
        std::cerr << "for the " << type << " search, the ids do not match " << sid << " " << lid << std::endl;
        abort();
    }

    if (sstatus != HYPERCLIENT_SUCCESS)
    {
        std::cerr << "for the " << type << " search, the first loop did not return a result." << std::endl;
        abort();
    }

    if (attrs_sz != 33)
    {
        std::cerr << "for the " << type << " search, there are not 1+32 attributes" << std::endl;
        abort();
    }

    if (strcmp(attrs[0].attr, "num") != 0)
    {
        std::cerr << "for the " << type << " search, attribute 0 (the key) is not named num" << std::endl;
        abort();
    }

    if (attrs[0].value_sz != sizeof(num)
            || memcmp(&num, attrs[0].value, sizeof(num)))
    {
        std::cerr << "for the " << type << " search, attribute 0 (the key) != " << num << std::endl;
        abort();
    }

    if (memcmp(&num, attrs[0].value, std::min(sizeof(num), attrs[0].value_sz)) != 0)
    {
        std::cerr << "for the " << type << " search, the result's key does not match" << std::endl;
        abort();
    }

    for (size_t i = 0; i < 32; ++i)
    {
        if (strcmp(attrs[i + 1].attr, colnames[i]) != 0)
        {
            std::cerr << "for the " << type << " search, secondary attribute " << i << " is not named " << colnames[i] << std::endl;
            abort();
        }

        if ((num & (1 << i))
                && !(attrs[i + 1].value_sz == 3 && memcmp(attrs[i + 1].value, "ONE", 3) == 0))
        {
            std::cerr << "for the " << type << " search, secondary attribute " << i << " != ONE" << std::endl;
            abort();
        }
        else if (!(num & (1 << i))
                && !(attrs[i + 1].value_sz == 4 && memcmp(attrs[i + 1].value, "ZERO", 4) == 0))
        {
            std::cerr << "for the " << type << " search, secondary attribute " << i << " != ZERO" << std::endl;
            abort();
        }
    }

    hyperclient_destroy_attrs(attrs, attrs_sz);

    lid = cl->loop(-1, &lstatus);

    if (lid < 0)
    {
        std::cerr << type << " search's 2nd loop returns(" << lid << ", " << lstatus << ")" << std::endl;
        abort();
    }

    if (lid != sid)
    {
        std::cerr << "for the " << type << " search's 2nd loop, the ids do not match " << sid << " " << lid << std::endl;
        abort();
    }

    if (sstatus != HYPERCLIENT_SEARCHDONE)
    {
        std::cerr << type << " search's 2nd loop is not a SEARCHDONE message";
        abort();
    }
}
