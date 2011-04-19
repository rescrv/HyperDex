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
#include <hyperdex/hyperdexd.h>

void
handle_signal(int signum);

hyperdex::hyperdexd hyperdexd;

int
main(int argc, char* argv[])
{
    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    // Install signal handlers
    struct sigaction sa;
    sa.sa_handler = handle_signal;
    sa.sa_flags = SA_RESTART;
    sa.sa_restorer = NULL;

    if (sigemptyset(&sa.sa_mask) < 0)
    {
        perror("sigemptyset");
        return EXIT_FAILURE;
    }

    if (sigaction(SIGHUP, &sa, NULL) < 0)
    {
        perror("sigaction(SIGHUP)");
        return EXIT_FAILURE;
    }

    if (sigaction(SIGINT, &sa, NULL) < 0)
    {
        perror("sigaction(SIGINT)");
        return EXIT_FAILURE;
    }

    if (sigaction(SIGQUIT, &sa, NULL) < 0)
    {
        perror("sigaction(SIGQUIT)");
        return EXIT_FAILURE;
    }

    if (sigaction(SIGTERM, &sa, NULL) < 0)
    {
        perror("sigaction(SIGTERM)");
        return EXIT_FAILURE;
    }

    if (sigaction(SIGUSR1, &sa, NULL) < 0)
    {
        perror("sigaction(SIGUSR1)");
        return EXIT_FAILURE;
    }

    if (sigaction(SIGUSR2, &sa, NULL) < 0)
    {
        perror("sigaction(SIGUSR2)");
        return EXIT_FAILURE;
    }

    // Add arguments to the daemon.
    // TODO:  These are constant for now.
    hyperdexd.set_location(po6::net::location("127.0.0.1", 6970));

    // Run the daemon.
    try
    {
        hyperdexd.run();
    }
    catch (po6::error& e)
    {
        LOG(ERROR) << "Uncaught system error:  " << e.what();
    }
    catch (std::bad_alloc& ba)
    {
        LOG(ERROR) << "Out of memory:  " << ba.what();
    }
    catch (std::exception& e)
    {
        LOG(ERROR) << "Uncaught exception:  " << e.what();
    }

    return EXIT_SUCCESS;
}

void
handle_signal(int signum)
{
    switch (signum)
    {
        case SIGHUP:
            hyperdexd.HUP();
            break;
        case SIGINT:
            hyperdexd.INT();
            break;
        case SIGQUIT:
            hyperdexd.QUIT();
            break;
        case SIGTERM:
            hyperdexd.TERM();
            break;
        case SIGUSR1:
            hyperdexd.USR1();
            break;
        case SIGUSR2:
            hyperdexd.USR2();
            break;
        default:
            break;
    }
}
