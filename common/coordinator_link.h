// Copyright (c) 2013, Cornell University
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

#ifndef hyperdex_common_coordinator_link_h_
#define hyperdex_common_coordinator_link_h_

#define __STDC_LIMIT_MACROS

// C
#include <stdint.h>

// STL
#include <list>

// e
#include <e/error.h>

// Replicant
#include <replicant.h>

// HyperDex
#include "namespace.h"
#include "common/configuration.h"

BEGIN_HYPERDEX_NAMESPACE

class coordinator_link
{
    public:
#ifdef _MSC_VER
        typedef fd_set* poll_fd_t;
#else
        typedef int poll_fd_t;
#endif

    public:
        coordinator_link(const char* coordinator, uint16_t port);
        ~coordinator_link() throw ();

    public:
        const configuration* config() { return &m_config; }
        poll_fd_t poll_fd() { return m_repl.poll_fd(); }
        bool ensure_configuration(replicant_returncode* status);
        int64_t rpc(const char* func,
                    const char* data, size_t data_sz,
                    replicant_returncode* status,
                    const char** output, size_t* output_sz);
        int64_t loop(int timeout, replicant_returncode* status);
        uint64_t queued_responses() { return m_pending_ids.size(); }
        e::error error() { return m_repl.last_error(); }

    private:
        coordinator_link(const coordinator_link&);
        coordinator_link& operator = (const coordinator_link&);

    private:
        // returns true when ensures config one way or the other
        // returns false when further looping is necessary to ensure config
        bool handle_internal_callback(replicant_returncode* status, bool* ensured);
        bool begin_waiting_on_broadcast(replicant_returncode* status);
        bool begin_fetching_config(replicant_returncode* status);

    private:
        replicant_client m_repl;
        configuration m_config;
        int64_t m_id;
        replicant_returncode m_status;
        enum { WAITING_ON_BROADCAST, FETCHING_CONFIG } m_state;
        const char* m_output;
        size_t m_output_sz;
        std::list<int64_t> m_pending_ids;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_common_coordinator_link_h_
