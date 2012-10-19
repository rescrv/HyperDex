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

#ifndef hyperdex_test_common_h_
#define hyperdex_test_common_h_

// Popt
#include <popt.h>

extern const char* hyperdex_test_space;
extern const char* hyperdex_test_host;
extern long hyperdex_test_port;

extern "C"
{

extern struct poptOption hyperdex_test_popts[];

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

#endif // hyperdex_test_common_h_
