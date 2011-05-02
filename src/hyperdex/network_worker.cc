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

hyperdex :: network_worker :: network_worker(hyperdex::logical* comm,
                                             hyperdex::datalayer* data)
    : m_continue(true)
    , m_comm(comm)
    , m_data(data)
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

    hyperdex::entity from;
    hyperdex::entity to;
    uint8_t msg_type;
    std::vector<char> msg;

    while (m_continue && m_comm->recv(&from, &to, &msg_type, &msg))
    {
        hyperdex::stream_no::stream_no_t type;
        type = static_cast<hyperdex::stream_no::stream_no_t>(msg_type);

        switch (type)
        {
            case hyperdex::stream_no::NONE:
                LOG(INFO) << "NONE";
                break;
            case hyperdex::stream_no::GET:
                LOG(INFO) << "GET";
                break;
            case hyperdex::stream_no::PUT:
                LOG(INFO) << "PUT";
                break;
            case hyperdex::stream_no::DEL:
                LOG(INFO) << "DEL";
                break;
            case hyperdex::stream_no::SEARCH:
                LOG(INFO) << "SEARCH";
                break;
            default:
                LOG(INFO) << "unknown message type";
                break;
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
