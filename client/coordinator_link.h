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
#include "namespace.h"
#include "visibility.h"
#include "common/configuration.h"
#include "client/hyperclient.h"
#include "client/hyperspace_builder.h"

// XXX hide this class
namespace hyperdex
{

class HYPERDEX_API coordinator_link
{
    public:
        HYPERDEX_API coordinator_link(const po6::net::hostname& coord);
        HYPERDEX_API ~coordinator_link() throw ();

    public:
        HYPERDEX_API hyperclient_returncode add_space(hyperspace* space);
        HYPERDEX_API hyperclient_returncode rm_space(const char* space);
        HYPERDEX_API hyperclient_returncode initialize_cluster(uint64_t cluster, const char* path);
        HYPERDEX_API hyperclient_returncode show_config(std::ostream& out);
        HYPERDEX_API hyperclient_returncode kill(uint64_t server_id);
        HYPERDEX_API hyperclient_returncode initiate_transfer(uint64_t region_id, uint64_t server_id);

    public:
        HYPERDEX_API const configuration& config();
        HYPERDEX_API bool wait_for_config(hyperclient_returncode* status);
        HYPERDEX_API bool poll_for_config(hyperclient_returncode* status);
#ifdef _MSC_VER
        fd_set* poll_fd();
#else
        int poll_fd();
#endif
        HYPERDEX_API bool make_rpc(const char* func,
                      const char* data, size_t data_sz,
                      hyperclient_returncode* status,
                      const char** output, size_t* output_sz);
        HYPERDEX_API replicant_client* replicant() { return &m_repl; }

    private:
        bool initiate_wait_for_config(hyperclient_returncode* status);
        bool initiate_get_config(hyperclient_returncode* status);

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

END_HYPERDEX_NAMESPACE

#endif // hyperdex_client_coordinator_link_h_
