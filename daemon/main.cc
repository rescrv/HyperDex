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

// Popt
#include <popt.h>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/net/ipaddr.h>
#include <po6/net/hostname.h>

// e
#include <e/guard.h>

// HyperDex
#include "daemon/daemon.h"

static bool _daemonize = true;
static const char* _data = ".";
static const char* _listen_host = "auto";
static unsigned long _listen_port = 2012;
static po6::net::ipaddr _listen_ip;
static bool _listen = false;
static const char* _coordinator_host = "127.0.0.1";
static unsigned long _coordinator_port = 1982;
static bool _coordinator = false;
static long _threads = 0;

extern "C"
{

static struct poptOption popts[] = {
    POPT_AUTOHELP
    {"daemon", 'd', POPT_ARG_NONE, NULL, 'd',
     "run HyperDex in the background", 0},
    {"foreground", 'f', POPT_ARG_NONE, NULL, 'f',
     "run replicant in the foreground", 0},
    {"data", 'D', POPT_ARG_STRING, &_data, 'D',
     "store persistent state in this directory (default: .)",
     "dir"},
    {"listen", 'l', POPT_ARG_STRING, &_listen_host, 'l',
     "listen on a specific IP address (default: auto)",
     "IP"},
    {"listen-port", 'p', POPT_ARG_LONG, &_listen_port, 'L',
     "listen on an alternative port (default: 2012)",
     "port"},
    {"coordinator", 'c', POPT_ARG_STRING, &_coordinator_host, 'c',
     "join an existing HyperDex cluster through IP address or hostname",
     "addr"},
    {"coordinator-port", 'P', POPT_ARG_LONG, &_coordinator_port, 'C',
     "connect to an alternative port on the coordinator (default: 2013)",
     "port"},
    {"threads", 't', POPT_ARG_LONG, &_threads, 't',
     "the number of threads which will handle network traffic",
     "N"},
    POPT_TABLEEND
};

} // extern "C"

int
main(int argc, const char* argv[])
{
    poptContext poptcon;
    poptcon = poptGetContext(NULL, argc, argv, popts, POPT_CONTEXT_POSIXMEHARDER);
    e::guard g = e::makeguard(poptFreeContext, poptcon); g.use_variable();
    int rc;

    while ((rc = poptGetNextOpt(poptcon)) != -1)
    {
        switch (rc)
        {
            case 'd':
                _daemonize = true;
                break;
            case 'f':
                _daemonize = false;
                break;
            case 'D':
                break;
            case 'l':
                try
                {
                    if (strcmp(_listen_host, "auto") == 0)
                    {
                        break;
                    }

                    _listen_ip = po6::net::ipaddr(_listen_host);
                }
                catch (po6::error& e)
                {
                    std::cerr << "cannot parse listen address" << std::endl;
                    return EXIT_FAILURE;
                }
                catch (std::invalid_argument& e)
                {
                    std::cerr << "cannot parse listen address" << std::endl;
                    return EXIT_FAILURE;
                }

                _listen = true;
                break;
            case 'L':
                if (_listen_port >= (1 << 16))
                {
                    std::cerr << "port number to listen on is out of range" << std::endl;
                    return EXIT_FAILURE;
                }

                _listen = true;
                break;
            case 'c':
                _coordinator = true;
                break;
            case 'C':
                if (_coordinator_port >= (1 << 16))
                {
                    std::cerr << "port number to coordinator to is out of range" << std::endl;
                    return EXIT_FAILURE;
                }

                _coordinator = true;
                break;
            case 't':
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

    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    try
    {
        hyperdex::daemon d;

        if (strcmp(_listen_host, "auto") == 0)
        {
            if (!busybee_mta::discover(&_listen_ip))
            {
                std::cerr << "cannot automatically discover local address; specify one manually" << std::endl;
                return EXIT_FAILURE;
            }
        }

        po6::pathname data(_data);
        po6::net::location bind_to(_listen_ip, _listen_port);
        po6::net::hostname coord(_coordinator_host, _coordinator_port);

        if (_threads <= 0)
        {
            _threads += sysconf(_SC_NPROCESSORS_ONLN);

            if (_threads <= 0)
            {
                std::cerr << "cannot create a non-positive number of threads" << std::endl;
                return EXIT_FAILURE;
            }
        }
        else if (_threads > 512)
        {
            std::cerr << "refusing to create more than 512 threads" << std::endl;
            return EXIT_FAILURE;
        }

        return d.run(_daemonize, data, _listen, bind_to, _coordinator, coord, _threads);
    }
    catch (po6::error& e)
    {
        std::cerr << "system error:  " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
