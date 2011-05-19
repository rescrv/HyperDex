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

// STL
#include <sstream>
#include <tr1/functional>

// Google Log
#include <glog/logging.h>

// po6
#include <po6/threads/thread.h>

// HyperDex
#include <hyperdex/masterlink.h>

hyperdex :: masterlink :: masterlink(const po6::net::location& loc,
                                     hyperdex::datalayer* data,
                                     hyperdex::logical* comm)
    : m_continue(true)
    , m_sock(loc.address.family(), SOCK_STREAM, IPPROTO_TCP)
    , m_thread(std::tr1::bind(std::tr1::function<void (hyperdex::masterlink*)>(&hyperdex::masterlink::run), this))
    , m_dl()
    , m_wake(m_dl)
{
    m_wake.set<masterlink, &masterlink::wake>(this);
    m_wake.start();
    m_sock.connect(loc);
    std::ostringstream ostr;
    ostr << "instance on " << comm->instance().inbound << " "
                           << comm->instance().inbound_version << " "
                           << comm->instance().outbound << " "
                           << comm->instance().outbound_version
                           << "\n";

    if (m_sock.xwrite(ostr.str().c_str(), ostr.str().size()) != ostr.str().size())
    {
        throw po6::error(errno);
    }

    m_thread.start();
}

hyperdex :: masterlink :: ~masterlink()
{
    if (m_continue)
    {
        m_wake.send();
        LOG(INFO) << "Master link object not cleanly shutdown.";
    }

    m_thread.join();
}

void
hyperdex :: masterlink :: shutdown()
{
    m_wake.send();
}

void
hyperdex :: masterlink :: run()
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

    while (m_continue)
    {
        m_dl.loop(EVLOOP_ONESHOT);
    }
}

void
hyperdex :: masterlink :: wake(ev::async&, int)
{
    m_continue = false;
}
