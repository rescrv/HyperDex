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
#include <netinet/in.h>
#include <netinet/sctp.h>
#include <signal.h>
#include <sys/socket.h>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/net/location.h>

// HyperDex
#include <hyperdex/limits.h>
#include <hyperdex/network_worker.h>
#include <hyperdex/stream_no.h>

hyperdex :: network_worker :: network_worker(po6::net::location us,
                                             po6::net::socket* sock)
    : m_continue(true)
    , m_us(us)
    , m_sock(sock)
{
}

hyperdex :: network_worker :: ~network_worker()
{
}

void
hyperdex :: network_worker :: run()
{
    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        return;
    }

    if (pthread_sigmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        PLOG(ERROR) << "pthread_sigmask";
        return;
    }

    int ret;
    int flags;
    sockaddr_in6 sa;
    socklen_t salen;
    sctp_sndrcvinfo sinfo;
    std::vector<uint8_t> buf(hyperdex::MESSAGE_SIZE, 0);

    while (m_continue)
    {
        // Receive one message, and dispatch it appropriately.
        salen = sizeof(sa);

        if ((ret = sctp_recvmsg(m_sock->get(), &buf.front(), buf.size(),
                                reinterpret_cast<sockaddr*>(&sa), &salen,
                                &sinfo, &flags)) < 0)
        {
            PLOG(ERROR) << "receiving message";
        }

        switch (sinfo.sinfo_stream)
        {
            case stream_no::NONE:
                break;
            default:
                LOG(INFO) << "receiving message";
        }
    }
}

void
hyperdex :: network_worker :: shutdown()
{
    // TODO:  This is not the proper shutdown method.  Proper shutdown is a
    // two-stage process, and requires global coordination.
    m_continue = false;
}
