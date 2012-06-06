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
#include <sys/stat.h>

// popt
#include <popt.h>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/error.h>

// e
#include <e/guard.h>

// HyperDex
#include "hyperdaemon/daemon.h"

static const char* data = ".";
static const char* host = "127.0.0.1";
static po6::net::ipaddr coord(host);
static long port = 1234;
static long threads = 1;
static const char* bindto = "127.0.0.1";
static po6::net::ipaddr local(bindto);
static long port_in = 0;
static long port_out = 0;
static bool daemonize = true;

extern "C"
{

static struct poptOption popts[] = {
    POPT_AUTOHELP
    {"daemon", 'd', POPT_ARG_NONE, NULL, 'd',
        "run HyperDex in the background",
        '\0'},
    {"foreground", 'f', POPT_ARG_NONE, NULL, 'f',
        "run HyperDex in the foreground",
        '\0'},
    {"data", 'D', POPT_ARG_STRING, &data, 'D',
        "the local directory in which data will be stored",
        "D"},
    {"host", 'h', POPT_ARG_STRING, &host, 'h',
        "the IP address of the coordinator",
        "IP"},
    {"port", 'p', POPT_ARG_LONG, &port, 'p',
        "the port number of the coordinator",
        "port"},
    {"threads", 't', POPT_ARG_LONG, &threads, 't',
        "the number of threads which will handle network traffic",
        "N"},
    {"bind-to", 'b', POPT_ARG_STRING, &bindto, 'b',
        "the local IP address that all sockets should bind to",
        "IP"},
    {"incoming-port", 'i', POPT_ARG_LONG, &port_in, 'i',
        "the port to listen on for incoming connections",
        "port"},
    {"outgoing-port", 'o', POPT_ARG_LONG, &port_out, 'o',
        "the port to use for outgoing connections",
        "port"},
    POPT_TABLEEND
};

} // extern "C"

int
main(int argc, const char* argv[])
{
    poptContext poptcon;
    poptcon = poptGetContext(NULL, argc, argv, popts, POPT_CONTEXT_POSIXMEHARDER);
    e::guard g = e::makeguard(poptFreeContext, poptcon);
    g.use_variable();
    int rc;
    struct stat stbuf;

    while ((rc = poptGetNextOpt(poptcon)) != -1)
    {
        switch (rc)
        {
            case 'd':
                daemonize = true;
                break;
            case 'f':
                daemonize = false;
                break;
            case 'D':
                if (stat(data, &stbuf) < 0)
                {
                    std::cerr << "could not access data directory:  " << strerror(errno) << std::endl;
                    return EXIT_FAILURE;
                }

                break;
            case 'h':
                try
                {
                    coord = po6::net::ipaddr(host);
                }
                catch (po6::error& e)
                {
                    std::cerr << "cannot parse coordinator address" << std::endl;
                    return EXIT_FAILURE;
                }
                catch (std::invalid_argument& e)
                {
                    std::cerr << "cannot parse coordinator address" << std::endl;
                    return EXIT_FAILURE;
                }

                break;
            case 'p':
                if (port < 0 || port >= (1 << 16))
                {
                    std::cerr << "coordinator port number out of range for TCP" << std::endl;
                    return EXIT_FAILURE;
                }

                break;
            case 't':
                if (threads <= 0)
                {
                    std::cerr << "cannot create a non-positive number of threads" << std::endl;
                    return EXIT_FAILURE;
                }
                else if (threads > 512)
                {
                    std::cerr << "refusing to create more than 512 threads" << std::endl;
                    return EXIT_FAILURE;
                }

                break;
            case 'b':
                try
                {
                    local = po6::net::ipaddr(bindto);
                }
                catch (po6::error& e)
                {
                    std::cerr << "cannot parse bind-to as IP address" << std::endl;
                    return EXIT_FAILURE;
                }
                catch (std::invalid_argument& e)
                {
                    std::cerr << "cannot parse bind-to as IP address" << std::endl;
                    return EXIT_FAILURE;
                }

                break;
            case 'i':
                if (port_in < 0 || port_in >= (1 << 16))
                {
                    std::cerr << "coordinator port number out of range for TCP" << std::endl;
                    return EXIT_FAILURE;
                }

                break;
            case 'o':
                if (port_out < 0 || port_out >= (1 << 16))
                {
                    std::cerr << "coordinator port number out of range for TCP" << std::endl;
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

    // Run the daemon.
    try
    {
        po6::pathname datadir(data);
        return hyperdaemon::daemon(argv[0], daemonize, datadir, po6::net::location(coord, port),
                                   threads, local, port_in, port_out);
    }
    catch (std::exception& e)
    {
        LOG(ERROR) << "Uncaught exception:  " << e.what();
    }

    return EXIT_SUCCESS;
}
