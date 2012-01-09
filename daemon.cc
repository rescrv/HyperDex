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

// C
#include <cstdio>

// POSIX
#include <signal.h>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/error.h>

// HyperDex
#include "hyperdaemon/hyperdaemon/daemon.h"

int
usage();

int
main(int argc, char* argv[])
{
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    if (argc != 4 && argc != 5)
    {
        return usage();
    }

    // Run the daemon.
    try
    {
        po6::net::location coordinator = po6::net::location(argv[1], atoi(argv[2]));
        po6::net::ipaddr bind_to = po6::net::ipaddr(argv[3]);
        po6::pathname base = ".";

        if (argc == 5)
        {
            base = argv[4];
        }

        return hyperdaemon::daemon(base, coordinator, 2, bind_to, 0, 0);
    }
    catch (std::exception& e)
    {
        LOG(ERROR) << "Uncaught exception:  " << e.what();
    }

    return EXIT_SUCCESS;
}

int
usage()
{
    std::cerr << "Usage:  hyperdexd <coordinator ip> <coordinator port> <bind to> [<datadir>]"
              << std::endl;
    return EXIT_FAILURE;
}
