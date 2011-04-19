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

// po6
#include <po6/threads/thread.h>

// HyperDex
#include <hyperdex/hyperdexd.h>
#include <hyperdex/network_worker.h>

typedef std::tr1::shared_ptr<po6::threads::thread> thread_ptr;

#define NUM_THREADS 64

hyperdex :: hyperdexd :: hyperdexd()
    : m_bind_to()
    , m_continue(true)
{
}

hyperdex :: hyperdexd :: ~hyperdexd()
{
}

void
hyperdex :: hyperdexd :: set_location(po6::net::location bind_to)
{
    m_bind_to = bind_to;
}

void
hyperdex :: hyperdexd :: run()
{
    // Actually run HyperDex from within here.
    // Open a listening socket.
    po6::net::socket sock(AF_INET6, SOCK_SEQPACKET, IPPROTO_SCTP);
    sockaddr_in6 sa6;
    sockaddr* sa = reinterpret_cast<sockaddr*>(&sa6);
    socklen_t salen = sizeof(sa6);
    m_bind_to.pack(sa, &salen);

    if (sctp_bindx(sock.get(), sa, 1, SCTP_BINDX_ADD_ADDR) < 0)
    {
        PLOG(ERROR) << "sctp_bindx";
        return;
    }

    sock.listen(1024);

    // Start the network_worker threads.
    hyperdex::network_worker nw(m_bind_to, &sock);
    std::tr1::function<void (hyperdex::network_worker*)> fnw(&hyperdex::network_worker::run);
    std::vector<thread_ptr> threads;

    for (size_t i = 0; i < NUM_THREADS; ++i)
    {
        thread_ptr t(new po6::threads::thread(std::tr1::bind(fnw, &nw)));
        t->start();
        threads.push_back(t);
    }

    // Wait for a signal to interrupt us.
    while (m_continue)
    {
        pause();
    }

    // Cleanup HyperDex.
    // Cleanup the network_worker threads.
    nw.shutdown();

    for (size_t i = 0; i < threads.size(); ++i)
    {
        if (sctp_sendmsg(sock.get(), "A", 2, sa, salen, 0, SCTP_UNORDERED|SCTP_ADDR_OVER, 0, 0, 0) < 0)
        {
            PLOG(ERROR) << "sctp_bindx";
        }
    }

    for (std::vector<thread_ptr>::iterator t = threads.begin();
            t != threads.end(); ++t)
    {
        (*t)->join();
    }

    // Cleanup the listening socket.
    sock.close();
}

void
hyperdex :: hyperdexd :: HUP()
{
    LOG(ERROR) << "SIGHUP received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexd :: INT()
{
    LOG(ERROR) << "SIGINT received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexd :: QUIT()
{
    LOG(ERROR) << "SIGQUIT received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexd :: TERM()
{
    LOG(ERROR) << "SIGTERM received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexd :: USR1()
{
    LOG(INFO) << "SIGUSR1 received.  This does nothing.";
}

void
hyperdex :: hyperdexd :: USR2()
{
    LOG(INFO) << "SIGUSR2 received.  This does nothing.";
}
