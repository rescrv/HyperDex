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
//     * Neither the name of Replicant nor the names of its contributors may be
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

// C
#include <cstdlib>

// po6
#include <po6/error.h>
#include <po6/io/fd.h>
#include <po6/pathname.h>

// e
#include <e/guard.h>

// HyperDex
#include "client/coordinator_link.h"
#include "client/hyperclient.hpp"
#include "tools/old.common.h"

static struct poptOption popts[] = {
    POPT_AUTOHELP
    CONNECT_TABLE
    POPT_TABLEEND
};

int
main(int argc, const char* argv[])
{
    poptContext poptcon;
    poptcon = poptGetContext(NULL, argc, argv, popts, POPT_CONTEXT_POSIXMEHARDER);
    e::guard g = e::makeguard(poptFreeContext, poptcon); g.use_variable();
    poptSetOtherOptionHelp(poptcon, "[OPTIONS]");
    int rc;

    while ((rc = poptGetNextOpt(poptcon)) != -1)
    {
        switch (rc)
        {
            case 'h':
                if (!check_host())
                {
                    return EXIT_FAILURE;
                }
                break;
            case 'p':
                if (!check_port())
                {
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

    const char** args = poptGetArgs(poptcon);
    size_t num_args = 0;

    while (args && args[num_args])
    {
        ++num_args;
    }

    if (num_args > 1)
    {
        std::cerr << "command takes at most one argument" << std::endl;
        poptPrintUsage(poptcon, stderr, 0);
        return EXIT_FAILURE;
    }

    std::vector<po6::pathname> paths;

    if (num_args)
    {
        for (size_t i = 0; i < num_args; ++i)
        {
            paths.push_back(po6::pathname(args[i]));
        }
    }
    else
    {
        paths.push_back(po6::pathname(HYPERDEX_COORD_LIB ".so.0"));
        paths.push_back(po6::pathname(HYPERDEX_COORD_LIB ".so"));
        paths.push_back(po6::pathname(HYPERDEX_COORD_LIB ".dylib"));
        paths.push_back(po6::join(po6::pathname(argv[0]).dirname(), "libhypercoordinator.so"));
        paths.push_back(po6::join(po6::pathname(argv[0]).dirname(), "libhypercoordinator.dylib"));
    }

    try
    {
        uint64_t token;
        po6::io::fd sysrand(open("/dev/urandom", O_RDONLY));

        if (sysrand.get() < 0 ||
            sysrand.read(&token, sizeof(token)) != sizeof(token))
        {
            std::cerr << "could not generate random token for cluster" << std::endl;
            return EXIT_FAILURE;
        }

        hyperdex::coordinator_link cl(po6::net::hostname(_connect_host, _connect_port));

        for (size_t i = 0; i < paths.size(); ++i)
        {
            hyperclient_returncode e = cl.initialize_cluster(token, paths[i].get());

            if (e == HYPERCLIENT_NOTFOUND && errno == ENOENT)
            {
                continue;
            }
            else if (e == HYPERCLIENT_DUPLICATE)
            {
                std::cerr << "cluster already initialized" << std::endl;
                return EXIT_FAILURE;
            }
            else if (e == HYPERCLIENT_COORD_LOGGED)
            {
                std::cerr << "could not initialize cluster: see the replicant error log for details" << std::endl;
                return EXIT_FAILURE;
            }
            else if (e != HYPERCLIENT_SUCCESS)
            {
                std::cerr << "could not initialize cluster: " << e << std::endl;
                return EXIT_FAILURE;
            }

            return EXIT_SUCCESS;
        }

        std::cerr << "could not find libhypercoordinator library" << std::endl;
        std::cerr << "provide a path to the library as a command-line argument" << std::endl;
        std::cerr << "try specifying \".libs/libhypercoordinator.so\" if you did not run \"make install\"" << std::endl;
        return EXIT_FAILURE;
    }
    catch (po6::error& e)
    {
        std::cerr << "system error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
    catch (std::exception& e)
    {
        std::cerr << "error: " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
