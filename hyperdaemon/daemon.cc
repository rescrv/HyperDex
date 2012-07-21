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
#include <unistd.h>

// C++
#include <sstream>
#include <fstream>

// STL
#include <algorithm>
#include <tr1/memory>

// Google Log
#include <glog/logging.h>
#include <glog/raw_logging.h>

// e
#include <e/timer.h>

// HyperDex
#include "hyperdex/hyperdex/coordinatorlink.h"

// HyperDaemon
#include "hyperdaemon/daemon.h"
#include "hyperdaemon/datalayer.h"
#include "hyperdaemon/logical.h"
#include "hyperdaemon/network_worker.h"
#include "hyperdaemon/ongoing_state_transfers.h"
#include "hyperdaemon/replication_manager.h"
#include "hyperdaemon/searches.h"

// util
#include <util/atomicfile.h>

// Configuration
static const char* DAEMON_TOKEN_FILE = "token.hd";
typedef std::tr1::shared_ptr<po6::threads::thread> thread_ptr;

static bool s_continue = true;

void
sig_handle(int /*signum*/)
{
    RAW_LOG(ERROR, "signal received; triggering exit..");
    s_continue = false;
}

int
hyperdaemon :: daemon(const char* progname,
                      bool daemonize,
                      po6::pathname datadir,
                      po6::net::hostname coordinator,
                      uint16_t num_threads,
                      po6::net::ipaddr bind_to,
                      in_port_t incoming,
                      in_port_t outgoing)
{
    google::InitGoogleLogging(progname);
    google::InstallFailureSignalHandler();

    if (chdir(datadir.get()) < 0)
    {
        PLOG(ERROR) << "could not change to the data directory";
        return EXIT_FAILURE;
    }

    if (daemonize)
    {
        google::SetLogDestination(google::INFO, po6::join(datadir, po6::pathname("HyperDex-")).get());

        if (::daemon(1, 0) < 0)
        {
            PLOG(ERROR) << "could not daemonize";
            return EXIT_FAILURE;
        }
    }
    else
    {
        google::LogToStderr();
    }

    // Catch signals.
    struct sigaction handle;
    handle.sa_handler = sig_handle;
    sigfillset(&handle.sa_mask);
    handle.sa_flags = SA_RESTART;
    int ret = 0;
    ret += sigaction(SIGHUP, &handle, NULL);
    ret += sigaction(SIGINT, &handle, NULL);
    ret += sigaction(SIGTERM, &handle, NULL);

    if (ret < 0)
    {
        PLOG(ERROR) << "could not set signal handlers (sigaction)";
        return EXIT_FAILURE;
    }

    // Token uniquely identifies a daemon. It is generated once on first start and used for the life 
    // of daemon, even after restart or daemon move. 
    std::string token;
    
    po6::pathname token_file = po6::join(datadir, DAEMON_TOKEN_FILE);
    std::ifstream f; 
    f.open(token_file.get());
    if (f)
    {
        // Use the existing token.
        f >> token;
        if (f.fail() || token.length()!=32)
        {
            PLOG(ERROR) << "corrupted token file " << token_file.get() << ", token read " << token;
            return EXIT_FAILURE;
        }
        
        f.close();
    }
    else
    {
        // Generate new token.
        po6::io::fd rand(open("/dev/urandom", O_RDONLY));
        if (rand.get() < 0)
        {
            PLOG(ERROR) << "could not open /dev/urandom for random bytes (open)";
            return EXIT_FAILURE;
        }

        char random_bytes[16];
        if (rand.read(random_bytes, 16) != 16)
        {
            PLOG(ERROR) << "could not read random bytes from /dev/urandom (read)";
            return EXIT_FAILURE;
        }
    
        token = e::slice(random_bytes, 16).hex();
        
        // Save the token into a file (atomically).
        std::ostringstream s;
        s << token << std::endl;
        if (!util::atomicfile::rewrite(datadir.get(), DAEMON_TOKEN_FILE, s.str().c_str()))
        {
            PLOG(ERROR) << "could not write token to file " << token_file.get();
            return EXIT_FAILURE;
        }
    }

    num_threads = s_continue ? num_threads : 0;

    // Setup our link to the coordinator.
    hyperdex::coordinatorlink cl(coordinator);
    // Setup the data component.
    datalayer data(&cl, datadir);
    // Setup the communication component.
    logical comm(&cl, bind_to, incoming, outgoing, num_threads);
    // Create our announce string.
    std::ostringstream announce;
    announce << "instance\t" << comm.inst().address << "\t"
                             << comm.inst().inbound_port << "\t"
                             << comm.inst().outbound_port << "\t"
                             << getpid() << "\t"
                             << token;
    cl.set_announce(announce.str());
    // Setup the search component.
    searches ssss(&cl, &data, &comm);
    // Setup the recovery component.
    ongoing_state_transfers ost(&data, &comm, &cl);
    // Setup the replication component.
    replication_manager repl(&cl, &data, &comm, &ost);
    // Give the ongoing_state_transfers a view into the replication component
    ost.set_replication_manager(&repl);
    // Start the network workers.
    LOG(INFO) << "Starting network workers.";
    network_worker nw(&data, &comm, &ssss, &ost, &repl);
    std::tr1::function<void (network_worker*)> fnw(&network_worker::run);
    std::vector<thread_ptr> threads;

    for (size_t i = 0; i < num_threads; ++i)
    {
        thread_ptr t(new po6::threads::thread(std::tr1::bind(fnw, &nw)));
        t->start();
        threads.push_back(t);
    }

    LOG(INFO) << "Network workers started.";
    uint64_t now;
    uint64_t disconnected_at = e::time();
    uint64_t nanos_to_wait = 5000000;
    // Reconnect right away, then 10ms, then 20ms, .... 1s

    while (s_continue)
    {
        switch (cl.poll(1, -1))
        {
            case hyperdex::coordinatorlink::SUCCESS:
                break;
            case hyperdex::coordinatorlink::CONNECTFAIL:
                now = e::time();
                PLOG(ERROR) << "Coordinator connection failed";

                if (now - disconnected_at < nanos_to_wait)
                {
                    e::sleep_ns(0, nanos_to_wait - (now - disconnected_at));
                }

                disconnected_at = e::time();
                nanos_to_wait = std::min(nanos_to_wait * 2, static_cast<uint64_t>(1000000000));
                continue;;
            default:
                abort();
        }

        if (cl.unacknowledged())
        {
            LOG(INFO) << "Installing new configuration version " << cl.config().version();
            hyperdex::instance newinst = comm.inst();

            // Figure out which versions we were assigned.
            cl.config().instance_versions(&newinst);

            if (newinst.inbound_version == 0 ||
                newinst.outbound_version == 0)
            {
                LOG(ERROR) << "We've been configured to a dummy node.";
            }

            // Prepare for our new configuration.
            // These operations should assume that there will be network
            // activity, and that the network threads will be in full force.
            comm.prepare(cl.config(), newinst);
            data.prepare(cl.config(), newinst);
            repl.prepare(cl.config(), newinst);
            ost.prepare(cl.config(), newinst);
            ssss.prepare(cl.config(), newinst);

            // Protect ourself against exceptions.
            e::guard g1 = e::makeobjguard(comm, &logical::unpause);
            e::guard g2 = e::makeobjguard(comm, &logical::shutdown);

            LOG(INFO) << "Pausing communication for reconfiguration.";

            // Make sure that we wait until everyone is paused.
            comm.pause();

            // Here is the critical section.  This is is mutually exclusive with the
            // network workers' loop.
            comm.reconfigure(cl.config(), newinst);
            data.reconfigure(cl.config(), comm.inst());
            repl.reconfigure(cl.config(), comm.inst());
            ost.reconfigure(cl.config(), comm.inst());
            ssss.reconfigure(cl.config(), comm.inst());
            comm.unpause();
            g1.dismiss();
            g2.dismiss();
            LOG(INFO) << "Reconfiguration complete; unpausing communication.";

            // Cleanup anything not specified by our new configuration.
            // These operations should assume that there will be network
            // activity, and that the network threads will be in full force..
            ssss.prepare(cl.config(), comm.inst());
            ost.cleanup(cl.config(), comm.inst());
            repl.cleanup(cl.config(), comm.inst());
            data.cleanup(cl.config(), comm.inst());
            comm.cleanup(cl.config(), comm.inst());
            cl.acknowledge();
        }
    }

    LOG(INFO) << "Exiting daemon.";

    // Stop replication.
    repl.shutdown();
    // Turn off the network.
    comm.shutdown();
    // Cleanup the network_worker threads.
    nw.shutdown();
    // Stop the data layer.
    data.shutdown();

    for (std::vector<thread_ptr>::iterator t = threads.begin();
            t != threads.end(); ++t)
    {
        (*t)->join();
    }

    return EXIT_SUCCESS;
}
