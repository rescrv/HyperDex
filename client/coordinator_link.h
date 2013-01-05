// Copyright (c) 2012, Cornell University
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

#ifndef hyperdex_client_coordinator_link_h_
#define hyperdex_client_coordinator_link_h_

// po6
#include <po6/net/hostname.h>

// Replicant
#include <replicant.h>

// HyperDex
#include "client/hyperclient.h"

namespace hyperdex
{

class coordinator_link
{
    public:
        coordinator_link(const po6::net::hostname& coord);
        ~coordinator_link() throw ();

    public:
        const configuration& config();
        bool wait_for_config(hyperclient_returncode* status);
        bool poll_for_config(hyperclient_returncode* status);
        int poll_fd();
        bool make_rpc(const char* func,
                      const char* data, size_t data_sz,
                      hyperclient_returncode* status,
                      const char** output, size_t* output_sz);

    private:
        coordinator_link(const coordinator_link&);
        coordinator_link& operator = (const coordinator_link&);

    private:
        configuration m_config;
        replicant_client m_repl;
        bool m_need_wait;
        int64_t m_wait_config_id;
        replicant_returncode m_wait_config_status;
        int64_t m_get_config_id;
        replicant_returncode m_get_config_status;
        const char* m_get_config_output;
        size_t m_get_config_output_sz;
};

} // namespace hyperdex

#endif // hyperdex_client_coordinator_link_h_
