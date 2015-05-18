// Copyright (c) 2013-2015, Cornell University
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

// HyperDex
#include "common/coordinator_link.h"

using hyperdex::coordinator_link;

coordinator_link :: coordinator_link(const char* coordinator, uint16_t port)
    : m_repl(replicant_client_create(coordinator, port))
    , m_config()
    , m_id(-1)
    , m_status(REPLICANT_GARBAGE)
    , m_output(NULL)
    , m_output_sz(0)
{
    if (!m_repl)
    {
        throw std::bad_alloc();
    }
}

coordinator_link :: coordinator_link(const char* conn_str)
    : m_repl(replicant_client_create_conn_str(conn_str))
    , m_config()
    , m_id(-1)
    , m_status(REPLICANT_GARBAGE)
    , m_output(NULL)
    , m_output_sz(0)
{
    if (!m_repl)
    {
        throw std::bad_alloc();
    }
}

coordinator_link :: ~coordinator_link() throw ()
{
    reset();
    replicant_client_destroy(m_repl);
}

bool
coordinator_link :: ensure_configuration(replicant_returncode* status)
{
    if (!prime_state_machine(status))
    {
        return false;
    }

    assert(m_id >= 0);
    int timeout = m_config.cluster() == 0 ? -1 : 0;
    int64_t lid = replicant_client_wait(m_repl, m_id, timeout, status);

    if (lid < 0)
    {
        return *status == REPLICANT_TIMEOUT && timeout == 0;
    }

    assert(lid == m_id);
    return process_new_configuration(status);
}

int64_t
coordinator_link :: rpc(const char* func,
                        const char* data, size_t data_sz,
                        replicant_returncode* status,
                        char** output, size_t* output_sz)
{
    return replicant_client_call(m_repl, "hyperdex", func, data, data_sz,
                                 REPLICANT_CALL_ROBUST, status, output, output_sz);
}

int64_t
coordinator_link :: backup(replicant_returncode* status,
                           char** output, size_t* output_sz)
{
    return replicant_client_backup_object(m_repl, "hyperdex", status, output, output_sz);
}

int64_t
coordinator_link :: wait(const char* cond, uint64_t state,
                         replicant_returncode* status)
{
    return replicant_client_cond_wait(m_repl, "hyperdex", cond, state, status, NULL, NULL);
}

int64_t
coordinator_link :: loop(int timeout, replicant_returncode* status)
{
    if (!prime_state_machine(status))
    {
        return -1;
    }

    int64_t lid = replicant_client_loop(m_repl, timeout, status);

    if (lid == m_id)
    {
        return process_new_configuration(status) ? INT64_MAX : -1;
    }
    else
    {
        return lid;
    }
}

int64_t
coordinator_link :: wait(int64_t id, int timeout, replicant_returncode* status)
{
    if (!prime_state_machine(status))
    {
        return -1;
    }

    if (id == INT64_MAX)
    {
        id = m_id;
    }

    int64_t lid = replicant_client_wait(m_repl, id, timeout, status);

    if (lid == m_id)
    {
        return process_new_configuration(status) ? INT64_MAX : -1;
    }
    else
    {
        return lid;
    }
}

bool
coordinator_link :: prime_state_machine(replicant_returncode* status)
{
    if (m_id >= 0)
    {
        return true;
    }

    m_id = replicant_client_cond_wait(m_repl, "hyperdex", "config", m_config.version() + 1, &m_status, &m_output, &m_output_sz);
    *status = m_status;

    if (m_id >= 0)
    {
        return true;
    }
    else
    {
        reset();
        return false;
    }
}

bool
coordinator_link :: process_new_configuration(replicant_returncode* status)
{
    m_id = -1;

    if (m_status != REPLICANT_SUCCESS)
    {
        *status = m_status;
        reset();
        return false;
    }

    e::unpacker up(m_output, m_output_sz);
    configuration new_config;
    up = up >> new_config;
    reset();

    if (up.error())
    {
        *status = REPLICANT_SERVER_ERROR;
        return false;
    }

    m_config = new_config;
    return true;
}

void
coordinator_link :: reset()
{
    if (m_output)
    {
        free(m_output);
    }

    m_id = -1;
    m_status = REPLICANT_GARBAGE;
    m_output = NULL;
    m_output_sz = 0;
}
