// Copyright (c) 2015, Cornell University
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
#include <stdint.h>

// POSIX
#include <signal.h>

// STL
#include <queue>

// Google Log
#include <glog/logging.h>

// e
#include <e/endian.h>

// HyperDex
#include "common/coordinator_returncode.h"
#include "common/serialization.h"
#include "daemon/wan_manager.h"
#include "daemon/daemon.h"

using hyperdex::configuration;
using hyperdex::wan_manager;
using po6::threads::make_thread_wrapper;

wan_manager :: wan_manager(daemon* d)
    : m_daemon(d)
    , m_poller(make_thread_wrapper(&wan_manager::background_maintenance, this))
    , m_coord()
    , m_mtx()
    , m_cond(&m_mtx)
    , m_poller_started(false)
    , m_teardown(false)
    , m_deferred()
    , m_locked(false)
    , m_kill(false)
    , m_to_kill()
    , m_waiting(0)
    , m_sleep(1000ULL * 1000ULL)
    , m_online_id(-1)
    , m_shutdown_requested(false)
{
}

wan_manager :: ~wan_manager() throw ()
{
    {
        po6::threads::mutex::hold hold(&m_mtx);

        m_teardown = true;
    }

    if (m_poller_started)
    {
        m_poller.join();
    }
}

void
wan_manager :: set_coordinator_address(const char* host, uint16_t port)
{
    assert(!m_coord.get());
    m_coord.reset(new coordinator_link(host, port));
}

bool
wan_manager :: maintain_link()
{
    enter_critical_section_killable();
    bool exit_status = false;

    if (!m_poller_started)
    {
        m_poller.start();
        m_poller_started = true;
    }

    while (true) {
        int64_t id = -1;
        replicant_returncode status = REPLICANT_GARBAGE;

        if (!m_deferred.empty()) {
            id = m_deferred.front().first;
            status = m_deferred.front().second;
            m_deferred.pop();
        } else {
            id = m_coord->loop(1000, &status);
        }

        if (id == INT64_MAX) {
            exit_status = m_coord->config()->exists(m_daemon->m_us);
            break;
        } else {
            e::error err = m_coord->error();
            LOG(ERROR) << "coordinator error: " << err.msg() << " @" << err.loc();
            do_sleep();
            exit_status = false;
            break;
        }
    }

    exit_critical_section_killable();
    return exit_status;
}

void
wan_manager :: copy_config(configuration* config)
{
    enter_critical_section();
    *config = *m_coord->config();
    exit_critical_section();
}

void
wan_manager :: background_maintenance()
{
    sigset_t ss;

    if (sigfillset(&ss) < 0)
    {
        PLOG(ERROR) << "sigfillset";
        return;
    }

    if (pthread_sigmask(SIG_BLOCK, &ss, NULL) < 0)
    {
        PLOG(ERROR) << "could not block signals";
        return;
    }

    while (true)
    {
        enter_critical_section_background();

        if (m_teardown)
        {
            break;
        }

        replicant_returncode status = REPLICANT_GARBAGE;
        int64_t id = m_coord->loop(1000, &status);

        if (status != REPLICANT_TIMEOUT && status != REPLICANT_INTERRUPTED)
        {
            m_deferred.push(std::make_pair(id, status));
        }

        exit_critical_section_killable();
    }
}

void
wan_manager :: do_sleep()
{
    uint64_t sleep = m_sleep;
    timespec ts;

    while (sleep > 0)
    {
        ts.tv_sec = 0;
        ts.tv_nsec = std::min(static_cast<uint64_t>(10 * 1000ULL * 1000ULL), sleep);
        sigset_t empty_signals;
        sigset_t old_signals;
        sigemptyset(&empty_signals); // should never fail
        pthread_sigmask(SIG_SETMASK, &empty_signals, &old_signals); // should never fail
        nanosleep(&ts, NULL); // nothing to gain by checking output
        pthread_sigmask(SIG_SETMASK, &old_signals, NULL); // should never fail
        sleep -= ts.tv_nsec;
    }

    m_sleep = std::min(static_cast<uint64_t>(1000ULL * 1000ULL * 1000ULL), m_sleep * 2);
}

void
wan_manager :: reset_sleep()
{
    uint64_t start_sleep = 1000ULL * 1000ULL;

    if (m_sleep != start_sleep)
    {
        m_sleep = start_sleep;
        LOG(INFO) << "connection to coordinator reestablished";
    }
}

void
wan_manager :: enter_critical_section()
{
    po6::threads::mutex::hold hold(&m_mtx);

    while (m_locked)
    {
        if (m_kill)
        {
            pthread_kill(m_to_kill, SIGUSR1);
        }

        ++m_waiting;
        m_cond.wait();
        --m_waiting;
    }

    m_locked = true;
    m_kill = false;
}

void
wan_manager :: exit_critical_section()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_locked = false;
    m_kill = false;

    if (m_waiting > 0)
    {
        m_cond.broadcast();
    }
}

void
wan_manager :: enter_critical_section_killable()
{
    po6::threads::mutex::hold hold(&m_mtx);

    while (m_locked)
    {
        if (m_kill)
        {
            pthread_kill(m_to_kill, SIGUSR1);
        }

        ++m_waiting;
        m_cond.wait();
        --m_waiting;
    }

    m_locked = true;
    m_kill = true;
    m_to_kill = pthread_self();
}

void
wan_manager :: exit_critical_section_killable()
{
    po6::threads::mutex::hold hold(&m_mtx);
    m_locked = false;
    m_kill = false;

    if (m_waiting > 0)
    {
        m_cond.broadcast();
    }
}

void
wan_manager :: enter_critical_section_background()
{
    po6::threads::mutex::hold hold(&m_mtx);

    while (m_locked || m_waiting > 0)
    {
        ++m_waiting;
        m_cond.wait();
        --m_waiting;
    }

    m_locked = true;
    m_kill = true;
    m_to_kill = pthread_self();
}
