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

#ifndef hyperdex_daemon_background_thread_h_
#define hyperdex_daemon_background_thread_h_

// po6
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// e
#include <e/garbage_collector.h>

// HyperDex
#include "namespace.h"

BEGIN_HYPERDEX_NAMESPACE
class daemon;

class background_thread
{
    public:
        background_thread(daemon* d);
        virtual ~background_thread() throw ();

    public:
        void start();
        void initiate_pause();
        void wait_until_paused();
        void unpause();
        void shutdown();

    protected:
        virtual const char* thread_name() = 0;
        virtual bool have_work() = 0;
        virtual void copy_work() = 0;
        virtual void do_work() = 0;

    protected:
        // take the thread offline.  pause will appear to work right away
        void offline();
        // bring the thread back online.  will block if paused
        void online();
        bool is_paused() { return m_paused; } // call with lock()
        bool is_shutdown() { return m_shutdown; } // call with lock()
        void lock() { m_protect.lock(); }
        void unlock() { m_protect.unlock(); }
        void wakeup() { m_wakeup_thread.broadcast(); }

    private:
        void run();
        void block_signals();

    private:
        background_thread(const background_thread&);
        background_thread& operator = (const background_thread&);

    private:
        po6::threads::thread m_thread;
        e::garbage_collector* m_gc;
        po6::threads::mutex m_protect;
        po6::threads::cond m_wakeup_thread;
        po6::threads::cond m_wakeup_pauser;
        bool m_shutdown;
        int m_pause_count;
        bool m_paused;
        bool m_offline;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_background_thread_h_
