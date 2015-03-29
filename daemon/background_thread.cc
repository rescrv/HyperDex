// Copyright (c) 2014, Cornell University
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

// C
#include <cassert>

// POSIX
#include <signal.h>

// Google Log
#include <glog/logging.h>

// HyperDex
#include "daemon/background_thread.h"
#include "daemon/daemon.h"

using po6::threads::make_thread_wrapper;
using hyperdex::background_thread;

background_thread :: background_thread(daemon* d)
    : m_thread(make_thread_wrapper(&background_thread::run, this))
    , m_gc(&d->m_gc)
    , m_protect()
    , m_wakeup_thread(&m_protect)
    , m_wakeup_pauser(&m_protect)
    , m_shutdown(true)
    , m_pause_count(0)
    , m_paused(false)
    , m_offline(false)
{
    po6::threads::mutex::hold hold(&m_protect);
}

background_thread :: ~background_thread() throw ()
{
    shutdown();
}

void
background_thread :: start()
{
    po6::threads::mutex::hold hold(&m_protect);
    m_thread.start();
    m_shutdown = false;
}

void
background_thread :: initiate_pause()
{
    po6::threads::mutex::hold hold(&m_protect);
    ++m_pause_count;
}

void
background_thread :: wait_until_paused()
{
    po6::threads::mutex::hold hold(&m_protect);
    assert(m_pause_count > 0);

    while (!m_paused && !m_offline)
    {
        m_wakeup_pauser.wait();
    }
}

void
background_thread :: unpause()
{
    po6::threads::mutex::hold hold(&m_protect);
    assert(m_pause_count > 0);
    --m_pause_count;
    m_wakeup_thread.broadcast();
}

void
background_thread :: shutdown()
{
    bool already_shutdown;

    {
        po6::threads::mutex::hold hold(&m_protect);
        m_wakeup_thread.broadcast();
        already_shutdown = m_shutdown;
        m_shutdown = true;
    }

    if (!already_shutdown)
    {
        m_thread.join();
    }
}

void
background_thread :: offline()
{
    po6::threads::mutex::hold hold(&m_protect);
    assert(!m_offline);
    m_offline = true;

    if (m_pause_count > 0)
    {
        m_wakeup_pauser.broadcast();
    }
}

void
background_thread :: online()
{
    po6::threads::mutex::hold hold(&m_protect);
    assert(m_offline);
    m_offline = false;

    while (m_pause_count > 0 && !m_shutdown)
    {
        m_paused = true;
        m_wakeup_thread.wait();
        m_paused = false;
    }
}

void
background_thread :: run()
{
    LOG(INFO) << this->thread_name() << " thread started";
    block_signals();
    e::garbage_collector::thread_state ts;
    m_gc->register_thread(&ts);

    while (true)
    {
        {
            m_gc->quiescent_state(&ts);
            po6::threads::mutex::hold hold(&m_protect);

            while ((!this->have_work() && !m_shutdown) || m_pause_count > 0)
            {
                m_paused = true;

                if (m_pause_count > 0)
                {
                    m_wakeup_pauser.signal();
                }

                m_gc->offline(&ts);
                m_wakeup_thread.wait();
                m_gc->online(&ts);
                m_paused = false;
            }

            if (m_shutdown)
            {
                break;
            }

            this->copy_work();
        }

        this->do_work();
    }

    m_gc->deregister_thread(&ts);
    LOG(INFO) << this->thread_name() << " thread stopped";
}

void
background_thread :: block_signals()
{
    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        LOG(ERROR) << "could not successfully block signals; this could result in undefined behavior";
        return;
    }

    sigdelset(&ss, SIGPROF);

    if (pthread_sigmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        PLOG(ERROR) << "could not block signals";
        LOG(ERROR) << "could not successfully block signals; this could result in undefined behavior";
        return;
    }
}
