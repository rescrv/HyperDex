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

// POSIX
#include <signal.h>

// STL
#include <tr1/memory>

// Google Log
#include <glog/logging.h>

// e
#include <e/timer.h>

// HyperDex
#include <hyperdex/coordinatorlink.h>

// HyperDaemon
#include "../include/hyperdaemon/daemon.h"
#include "datalayer.h"
#include "logical.h"
#include "network_worker.h"
#include "replication_manager.h"
#include "searches.h"

// Configuration

typedef std::tr1::shared_ptr<po6::threads::thread> thread_ptr;

static bool s_continue = true;

void
sig_handle(int /*signum*/)
{
    LOG(ERROR) << "signal received; triggering exit..";
    s_continue = false;
}

int
hyperdaemon :: daemon(po6::pathname datadir,
                      po6::net::location coordinator,
                      po6::net::ipaddr bind_to,
                      uint16_t num_threads)
{
    // Catch signals.
    struct sigaction handle;
    handle.sa_handler = sig_handle;
    sigfillset(&handle.sa_mask);
    handle.sa_flags = SA_RESTART;
    int ret = 0;
    ret += sigaction(SIGHUP, &handle, NULL);
    ret += sigaction(SIGINT, &handle, NULL);

    if (ret < 0)
    {
        PLOG(INFO) << "could not set signal handlers (sigaction)";
        return EXIT_FAILURE;
    }

    // Setup our link to the coordinator.
    hyperdex::coordinatorlink cl(coordinator);
    // Setup the data component.
    datalayer data(&cl, datadir);
    // Setup the communication component.
    logical comm(&cl, bind_to);
    comm.pause(); // Pause is idempotent.  The first unpause will cancel this one.
    // Create our announce string.
    std::ostringstream announce;
    announce << "instance\t" << comm.inst().inbound << "\t"
                             << comm.inst().outbound;
    cl.set_announce(announce.str());
    // Setup the search component.
    searches ssss(&cl, &data, &comm);
    // Setup the replication component.
    replication_manager repl(&cl, &data, &comm);

    LOG(INFO) << "Connecting to the coordinator.";

    while (s_continue && cl.connect() != hyperdex::coordinatorlink::SUCCESS)
    {
        PLOG(INFO) << "Coordinator connection failed";
    }

    // Start the network workers.
    LOG(INFO) << "Starting network workers.";
    num_threads = s_continue ? num_threads : 0;
    network_worker nw(&data, &comm, &ssss, &repl);
    std::tr1::function<void (network_worker*)> fnw(&network_worker::run);
    std::vector<thread_ptr> threads;

    for (size_t i = 0; i < num_threads; ++i)
    {
        thread_ptr t(new po6::threads::thread(std::tr1::bind(fnw, &nw)));
        t->start();
        threads.push_back(t);
    }

    LOG(INFO) << "Network workers started.";

    while (s_continue)
    {
        if (cl.unacknowledged())
        {
            LOG(INFO) << "Installing new configuration.";
            // Prepare for our new configuration.
            // These operations should assume that there will be network
            // activity, and that the network threads will be in full force..
            comm.prepare(cl.config());
            data.prepare(cl.config(), comm.inst());
            repl.prepare(cl.config(), comm.inst());

            // Protect ourself against exceptions.
            e::guard g1 = e::makeobjguard(comm, &logical::unpause);
            e::guard g2 = e::makeobjguard(comm, &logical::shutdown);

            LOG(INFO) << "Pausing communication for reconfiguration.";

            // Make sure that we wait until everyone is paused.
            comm.pause();

            while (comm.num_paused() < num_threads)
            {
                e::sleep_ms(0, 10);
            }

            // Here is the critical section.  This is is mutually exclusive with the
            // network workers' loop.
            comm.reconfigure(cl.config());
            data.reconfigure(cl.config(), comm.inst());
            repl.reconfigure(cl.config(), comm.inst());
            comm.unpause();
            g1.dismiss();
            g2.dismiss();
            LOG(INFO) << "Reconfiguration complete; unpausing communication.";

            // Cleanup anything not specified by our new configuration.
            // These operations should assume that there will be network
            // activity, and that the network threads will be in full force..
            repl.cleanup(cl.config(), comm.inst());
            data.cleanup(cl.config(), comm.inst());
            comm.cleanup(cl.config());
            cl.acknowledge();
        }

        switch (cl.loop(1, -1))
        {
            case hyperdex::coordinatorlink::SHUTDOWN:
                s_continue = false;
                break;
            case hyperdex::coordinatorlink::SUCCESS:
                break;
            case hyperdex::coordinatorlink::CONNECTFAIL:
            case hyperdex::coordinatorlink::DISCONNECT:
                if (cl.connect() != hyperdex::coordinatorlink::SUCCESS)
                {
                    PLOG(INFO) << "Coordinator connection failed";
                }

                break;
            case hyperdex::coordinatorlink::LOGICERROR:
            default:
                assert(!"Programming error.");
        }
    }

    LOG(INFO) << "Exiting daemon.";

    // Stop replication.
    repl.shutdown();
    // Turn off the network.
    comm.shutdown();
    // Cleanup the network_worker threads.
    nw.shutdown();
    // Cleanup the master thread.
    cl.shutdown();
    // Stop the data layer.
    data.shutdown();

    for (std::vector<thread_ptr>::iterator t = threads.begin();
            t != threads.end(); ++t)
    {
        (*t)->join();
    }

    return EXIT_SUCCESS;
}
