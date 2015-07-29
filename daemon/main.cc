// Copyright (c) 2011-2014, Cornell University
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

#define __STDC_LIMIT_MACROS

// STL
#include <stdexcept>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/net/ipaddr.h>
#include <po6/net/hostname.h>

// e
#include <e/popt.h>

// BusyBee
#include <busybee_utils.h>

// HyperDex
#include "daemon/daemon.h"

int
main(int argc, const char* argv[])
{
    bool daemonize = true;
    const char* data = ".";
    const char* log = NULL;
    const char* pidfile = "";
    bool has_pidfile = false;
    bool listen = false;
    const char* listen_host = "auto";
    long listen_port = 2012;
    bool coordinator = false;
    const char* coordinator_host = "127.0.0.1";
    long coordinator_port = 1982;
    long threads = 0;
    bool log_immediate = false;

    e::argparser ap;
    ap.autohelp();
    ap.arg().name('d', "daemon")
            .description("run in the background")
            .set_true(&daemonize);
    ap.arg().name('f', "foreground")
            .description("run in the foreground")
            .set_false(&daemonize);
    ap.arg().name('D', "data")
            .description("store persistent state in this directory (default: .)")
            .metavar("dir").as_string(&data);
    ap.arg().name('L', "log")
            .description("store logs in this directory (default: --data)")
            .metavar("dir").as_string(&log);
    ap.arg().long_name("pidfile")
            .description("write the PID to a file (default: don't)")
            .metavar("file").as_string(&pidfile).set_true(&has_pidfile);
    ap.arg().name('l', "listen")
            .description("listen on a specific IP address (default: auto)")
            .metavar("IP").as_string(&listen_host).set_true(&listen);
    ap.arg().name('p', "listen-port")
            .description("listen on an alternative port (default: 1982)")
            .metavar("port").as_long(&listen_port).set_true(&listen);
    ap.arg().name('c', "coordinator")
            .description("join an existing HyperDex cluster through IP address or hostname")
            .metavar("addr").as_string(&coordinator_host).set_true(&coordinator);
    ap.arg().name('P', "coordinator-port")
            .description("connect to an alternative port on the coordinator (default: 1982)")
            .metavar("port").as_long(&coordinator_port).set_true(&coordinator);
    ap.arg().name('t', "threads")
            .description("the number of threads which will handle network traffic")
            .metavar("N").as_long(&threads);
    ap.arg().long_name("log-immediate")
            .description("immediately flush all log output")
            .set_true(&log_immediate).hidden();

    if (!ap.parse(argc, argv))
    {
        return EXIT_FAILURE;
    }

    if (ap.args_sz() != 0)
    {
        std::cerr << "command takes no positional arguments\n" << std::endl;
        ap.usage();
        return EXIT_FAILURE;
    }

    if (listen_port >= (1 << 16))
    {
        std::cerr << "listen-port is out of range" << std::endl;
        return EXIT_FAILURE;
    }

    if (coordinator_port >= (1 << 16))
    {
        std::cerr << "coordinator-port is out of range" << std::endl;
        return EXIT_FAILURE;
    }

    po6::net::ipaddr listen_ip;
    po6::net::location bind_to;

    if (strcmp(listen_host, "auto") == 0)
    {
        if (!busybee_discover(&listen_ip))
        {
            std::cerr << "cannot automatically discover local address; specify one manually" << std::endl;
            return EXIT_FAILURE;
        }

        bind_to = po6::net::location(listen_ip, listen_port);
    }
    else
    {
        try
        {
            listen_ip = po6::net::ipaddr(listen_host);
            bind_to = po6::net::location(listen_ip, listen_port);
        }
        catch (std::invalid_argument& e)
        {
            // fallthrough
        }

        if (bind_to == po6::net::location())
        {
            bind_to = po6::net::hostname(listen_host, 0).lookup(AF_UNSPEC, IPPROTO_TCP);
            bind_to.port = listen_port;
        }
    }

    if (bind_to == po6::net::location())
    {
        std::cerr << "cannot interpret listen address as hostname or IP address" << std::endl;
        return EXIT_FAILURE;
    }

    if (bind_to.address == po6::net::ipaddr("0.0.0.0"))
    {
        std::cerr << "cannot bind to " << bind_to << " because it is not routable" << std::endl;
        return EXIT_FAILURE;
    }

    google::InitGoogleLogging(argv[0]);
    google::InstallFailureSignalHandler();

    if (log_immediate)
    {
        FLAGS_logbufsecs = 0;
    }

    try
    {
        hyperdex::daemon d;

        if (threads <= 0)
        {
            threads += sysconf(_SC_NPROCESSORS_ONLN);

            if (threads <= 0)
            {
                std::cerr << "cannot create a non-positive number of threads" << std::endl;
                return EXIT_FAILURE;
            }
        }
        else if (threads > 512)
        {
            std::cerr << "refusing to create more than 512 threads" << std::endl;
            return EXIT_FAILURE;
        }

        return d.run(daemonize,
                     std::string(data),
                     std::string(log ? log : data),
                     std::string(pidfile), has_pidfile,
                     listen, bind_to,
                     coordinator, po6::net::hostname(coordinator_host, coordinator_port),
                     threads);
    }
    catch (std::exception& e)
    {
        std::cerr << "error:  " << e.what() << std::endl;
        return EXIT_FAILURE;
    }
}
