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

// ZooKeeper
#include <zookeeper.h>

// Google Log
#include <glog/logging.h>

// Libev
#include <ev++.h>

// HyperDex
#include <hyperdex/logical.h>
#include <hyperdex/hyperdexm.h>

static void
watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
    LOG(INFO) << "change on " << path;
}


hyperdex :: hyperdexm :: hyperdexm()
    : m_continue(true)
{
}

hyperdex :: hyperdexm :: ~hyperdexm()
{
}

void
hyperdex :: hyperdexm :: run()
{
    // Setup HyperDex.
    ev::default_loop dl;
    // Catch signals.
    ev::sig sighup(dl);
    sighup.set<hyperdexm, &hyperdexm::HUP>(this);
    sighup.set(SIGHUP);
    sighup.start();
    ev::sig sigint(dl);
    sigint.set<hyperdexm, &hyperdexm::INT>(this);
    sigint.set(SIGINT);
    sigint.start();
    ev::sig sigquit(dl);
    sigquit.set<hyperdexm, &hyperdexm::QUIT>(this);
    sigquit.set(SIGQUIT);
    sigquit.start();
    ev::sig sigterm(dl);
    sigterm.set<hyperdexm, &hyperdexm::TERM>(this);
    sigterm.set(SIGTERM);
    sigterm.start();
    ev::sig sigusr1(dl);
    sigusr1.set<hyperdexm, &hyperdexm::USR1>(this);
    sigusr1.set(SIGUSR1);
    sigusr1.start();
    ev::sig sigusr2(dl);
    sigusr2.set<hyperdexm, &hyperdexm::USR2>(this);
    sigusr2.set(SIGUSR2);
    sigusr2.start();
    // Setup the communications layer.
    hyperdex::logical comm(dl, "127.0.0.1"); // XXX don't hardcode localhost
    // Setup the ZooKeeper connection.
    clientid_t ci;
    memset(&ci, 0, sizeof(ci));
    zhandle_t* zk = zookeeper_init("127.0.0.1:2181", watcher, 10000, &ci, NULL, 0);

    while (m_continue)
    {
        dl.loop(EVLOOP_ONESHOT);
    }

    zookeeper_close(zk);
}

void
hyperdex :: hyperdexm :: HUP(ev::sig&, int)
{
    LOG(ERROR) << "SIGHUP received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexm :: INT(ev::sig&, int)
{
    LOG(ERROR) << "SIGINT received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexm :: QUIT(ev::sig&, int)
{
    LOG(ERROR) << "SIGQUIT received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexm :: TERM(ev::sig&, int)
{
    LOG(ERROR) << "SIGTERM received; exiting.";
    m_continue = false;
}

void
hyperdex :: hyperdexm :: USR1(ev::sig&, int)
{
    LOG(INFO) << "SIGUSR1 received.  This does nothing.";
}

void
hyperdex :: hyperdexm :: USR2(ev::sig&, int)
{
    LOG(INFO) << "SIGUSR2 received.  This does nothing.";
}
