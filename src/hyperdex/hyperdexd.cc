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

#define __STDC_LIMIT_MACROS

// POSIX
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/sctp.h>

// C++
#include <iostream>

// STL
#include <tr1/functional>
#include <tr1/memory>

// Google Log
#include <glog/logging.h>

// Libev
#include <ev++.h>

// po6
#include <po6/threads/thread.h>

// HyperDex
#include <hyperdex/datalayer.h>
#include <hyperdex/logical.h>
#include <hyperdex/hyperdexd.h>
#include <hyperdex/network_worker.h>

typedef std::tr1::shared_ptr<po6::threads::thread> thread_ptr;

#define NUM_THREADS 64

hyperdex :: hyperdexd :: hyperdexd()
    : m_continue(true)
{
}

hyperdex :: hyperdexd :: ~hyperdexd()
{
}

void
hyperdex :: hyperdexd :: run()
{
    // Setup HyperDex.
    ev::default_loop dl;
    // Catch signals.
    ev::sig sighup(dl);
    sighup.set<hyperdexd, &hyperdexd::HUP>(this);
    sighup.set(SIGHUP);
    sighup.start();
    ev::sig sigint(dl);
    sigint.set<hyperdexd, &hyperdexd::INT>(this);
    sigint.set(SIGINT);
    sigint.start();
    ev::sig sigquit(dl);
    sigquit.set<hyperdexd, &hyperdexd::QUIT>(this);
    sigquit.set(SIGQUIT);
    sigquit.start();
    ev::sig sigterm(dl);
    sigterm.set<hyperdexd, &hyperdexd::TERM>(this);
    sigterm.set(SIGTERM);
    sigterm.start();
    ev::sig sigusr1(dl);
    sigusr1.set<hyperdexd, &hyperdexd::USR1>(this);
    sigusr1.set(SIGUSR1);
    sigusr1.start();
    ev::sig sigusr2(dl);
    sigusr2.set<hyperdexd, &hyperdexd::USR2>(this);
    sigusr2.set(SIGUSR2);
    sigusr2.start();
    // Setup the communications layer.
    hyperdex::logical comm(dl, "127.0.0.1"); // XXX don't hardcode localhost
    // Setup the data layer.
    hyperdex::datalayer data;
    // Start the network_worker threads.
    hyperdex::network_worker nw(&comm, &data);
    std::tr1::function<void (hyperdex::network_worker*)> fnw(&hyperdex::network_worker::run);
    std::vector<thread_ptr> threads;

    for (size_t i = 0; i < NUM_THREADS; ++i)
    {
        thread_ptr t(new po6::threads::thread(std::tr1::bind(fnw, &nw)));
        t->start();
        threads.push_back(t);
    }

    while (m_continue)
    {
        dl.loop(EVLOOP_ONESHOT);
    }

    // Cleanup HyperDex.
    //
    // XXX This is not proper network procedure.  A proper shutdown is:
    //  - The server receives the shutdown notice.
    //  - Every row leader agrees not to accept more updates.
    //  - Every row leader notifies the master when stable.
    //  - All instances flush to disk.
    //  - All instances terminate.
    //
    // Turn off the network.
    comm.shutdown();
    // Cleanup the network_worker threads.
    nw.shutdown();

    for (std::vector<thread_ptr>::iterator t = threads.begin();
            t != threads.end(); ++t)
    {
        (*t)->join();
    }
}

void
hyperdex :: hyperdexd :: HUP(ev::sig&, int)
{
    LOG(ERROR) << "SIGHUP received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexd :: INT(ev::sig&, int)
{
    LOG(ERROR) << "SIGINT received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexd :: QUIT(ev::sig&, int)
{
    LOG(ERROR) << "SIGQUIT received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexd :: TERM(ev::sig&, int)
{
    LOG(ERROR) << "SIGTERM received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexd :: USR1(ev::sig&, int)
{
    LOG(INFO) << "SIGUSR1 received.  This does nothing.";
}

void
hyperdex :: hyperdexd :: USR2(ev::sig&, int)
{
    LOG(INFO) << "SIGUSR2 received.  This does nothing.";
}
