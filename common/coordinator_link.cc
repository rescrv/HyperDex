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

// HyperDex
#include "common/coordinator_link.h"

using hyperdex::coordinator_link;

coordinator_link :: coordinator_link(const char* coordinator, uint16_t port)
    : m_repl(coordinator, port)
    , m_config()
    , m_state(NOTHING)
    , m_id(-1)
    , m_status(REPLICANT_GARBAGE)
    , m_output(NULL)
    , m_output_sz(0)
    , m_pending_ids()
{
}

coordinator_link :: ~coordinator_link() throw ()
{
    reset();
}

bool
coordinator_link :: force_configuration_fetch(replicant_returncode* status)
{
    if (m_id >= 0)
    {
        m_repl.kill(m_id);
    }

    if (m_output)
    {
        replicant_destroy_output(m_output, m_output_sz);
    }

    m_id = -1;
    m_state = WAITING_ON_BROADCAST;
    m_status = REPLICANT_SUCCESS;
    m_output = NULL;
    m_output_sz = 0;
    return begin_fetching_config(status);
}

bool
coordinator_link :: ensure_configuration(replicant_returncode* status)
{
    while (true)
    {
        if (!prime_state_machine(status))
        {
            return false;
        }

        assert(m_id >= 0);
        assert(m_state > NOTHING);
        int timeout = 0;

        switch (m_state)
        {
            case WAITING_ON_BROADCAST:
                timeout = 0;
                break;
            case FETCHING_CONFIG:
                timeout = -1;
                break;
            case NOTHING:
            default:
                abort();
        }

        int64_t lid = m_repl.loop(timeout, status);

        if (lid < 0 && *status == REPLICANT_TIMEOUT && timeout >= 0)
        {
            return true;
        }
        else if (lid < 0)
        {
            return false;
        }

        if (lid == m_id)
        {
            bool failed = false;

            if (handle_internal_callback(status, &failed))
            {
                return true;
            }

            if (failed)
            {
                return false;
            }
        }
        else
        {
            m_pending_ids.push_back(lid);
        }
    }
}

int64_t
coordinator_link :: rpc(const char* func,
                        const char* data, size_t data_sz,
                        replicant_returncode* status,
                        const char** output, size_t* output_sz)
{
    return m_repl.send("hyperdex", func, data, data_sz, status, output, output_sz);
}

int64_t
coordinator_link :: wait(const char* cond, uint64_t state,
                         replicant_returncode* status)
{
    return m_repl.wait("hyperdex", cond, state, status);
}

int64_t
coordinator_link :: loop(int timeout, replicant_returncode* status)
{
    if (!m_pending_ids.empty())
    {
        int64_t ret = m_pending_ids.front();
        m_pending_ids.pop_front();
        return ret;
    }

    while (true)
    {
        if (!prime_state_machine(status))
        {
            return -1;
        }

        int64_t lid = m_repl.loop(timeout, status);

        if (lid == m_id)
        {
            bool failed = false;

            if (handle_internal_callback(status, &failed))
            {
                return INT64_MAX;
            }

            if (failed)
            {
                return -1;
            }
        }
        else
        {
            return lid;
        }
    }
}

bool
coordinator_link :: prime_state_machine(replicant_returncode* status)
{
    if (m_id >= 0)
    {
        return true;
    }

    if (m_state == NOTHING)
    {
        if (m_config.version() == 0)
        {
            m_state = WAITING_ON_BROADCAST;
            m_status = REPLICANT_SUCCESS;
            return begin_fetching_config(status);
        }
        else
        {
            return begin_waiting_on_broadcast(status);
        }
    }

    return true;
}

bool
coordinator_link :: handle_internal_callback(replicant_returncode* status, bool* failed)
{
    m_id = -1;

    if (m_status != REPLICANT_SUCCESS)
    {
        *status = m_status;
        *failed = true;
        reset();
        return false;
    }

    assert(m_state == WAITING_ON_BROADCAST ||
           m_state == FETCHING_CONFIG);

    if (m_state == WAITING_ON_BROADCAST)
    {
        if (!begin_fetching_config(status))
        {
            *failed = true;
            return false;
        }

        *failed = false;
        return false;
    }

    e::unpacker up(m_output, m_output_sz);
    configuration new_config;
    up = up >> new_config;
    reset();

    if (up.error())
    {
        *status = REPLICANT_MISBEHAVING_SERVER;
        *failed = true;
        return false;
    }

    if (m_config.cluster() != 0 &&
        m_config.cluster() != new_config.cluster())
    {
        *status = REPLICANT_MISBEHAVING_SERVER;
        *failed = true;
        return false;
    }

    m_config = new_config;
    return true;
}

bool
coordinator_link :: begin_waiting_on_broadcast(replicant_returncode* status)
{
    assert(m_id == -1);
    assert(m_state == NOTHING);
    assert(m_status == REPLICANT_GARBAGE);
    assert(m_output == NULL);
    assert(m_output_sz == 0);
    m_id = m_repl.wait("hyperdex", "config",
                       m_config.version() + 1, &m_status);
    m_state = WAITING_ON_BROADCAST;
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
coordinator_link :: begin_fetching_config(replicant_returncode* status)
{
    assert(m_id == -1);
    assert(m_state == WAITING_ON_BROADCAST);
    assert(m_status == REPLICANT_SUCCESS);
    assert(m_output == NULL);
    assert(m_output_sz == 0);
    m_id = m_repl.send("hyperdex", "config_get", "", 0,
                       &m_status, &m_output, &m_output_sz);
    m_state = FETCHING_CONFIG;
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

void
coordinator_link :: reset()
{
    if (m_output)
    {
        replicant_destroy_output(m_output, m_output_sz);
    }

    m_id = -1;
    m_state = NOTHING;
    m_status = REPLICANT_GARBAGE;
    m_output = NULL;
    m_output_sz = 0;
}
