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

#ifndef hyperdex_daemon_wan_manager_h_
#define hyperdex_daemon_wan_manager_h_

// C
#include <stdint.h>

// po6
#include <po6/net/location.h>
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/thread.h>

// HyperDex
#include "namespace.h"
#include "common/configuration.h"
#include "common/coordinator_link.h"
#include "common/ids.h"

BEGIN_HYPERDEX_NAMESPACE
class daemon;

class wan_manager
{
    public:
        wan_manager(daemon* d);
        ~wan_manager() throw ();

    public:
        void set_coordinator_address(const char* host, uint16_t port);
        bool maintain_link();
        void copy_config(configuration* config);

    private:
        void background_maintenance();
        void do_sleep();
        void reset_sleep();
        void enter_critical_section();
        void exit_critical_section();
        void enter_critical_section_killable();
        void enter_critical_section_background();
        void exit_critical_section_killable();

    private:
        daemon* m_daemon;
        po6::threads::thread m_poller;
        std::auto_ptr<coordinator_link> m_coord;
        po6::threads::mutex m_mtx;
        po6::threads::cond m_cond;
        bool m_poller_started;
        bool m_teardown;
        std::queue<std::pair<int64_t, replicant_returncode> > m_deferred;
        bool m_locked;
        bool m_kill;
        pthread_t m_to_kill;
        uint64_t m_waiting;
        uint64_t m_sleep;
        int64_t m_online_id;
        bool m_shutdown_requested;

    private:
        wan_manager(const wan_manager&);
        wan_manager& operator = (const wan_manager&);
};

END_HYPERDEX_NAMESPACE

#endif /* end of include guard: hyperdex_daemon_wan_manager_h_ */
