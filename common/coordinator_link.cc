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
    , m_id(-1)
    , m_status(REPLICANT_GARBAGE)
    , m_state()
    , m_output(NULL)
    , m_output_sz(0)
    , m_pending_ids()
{
}

coordinator_link :: ~coordinator_link() throw ()
{
}

bool
coordinator_link :: ensure_configuration(replicant_returncode* status)
{
    while (true)
    {
        if (m_id < 0)
        {
            if (m_config.version() > 0)
            {
                if (!begin_waiting_on_broadcast(status))
                {
                    return false;
                }
            }
            else
            {
                if (!begin_fetching_config(status))
                {
                    return false;
                }
            }
        }

        assert(m_id >= 0);
        int timeout = 0;

        switch (m_state)
        {
            case WAITING_ON_BROADCAST:
                timeout = 0;
                break;
            case FETCHING_CONFIG:
                timeout = -1;
                break;
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

        if (lid != m_id)
        {
            m_pending_ids.push_back(lid);
            continue;
        }
        else
        {
            bool ensured = false;

            if (handle_internal_callback(status, &ensured))
            {
                return ensured;
            }
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
        int64_t lid = m_repl.loop(timeout, status);

        if (lid < 0 || lid != m_id)
        {
            return lid;
        }
        else
        {
            assert(lid == m_id);
            bool ensured = false;

            if (handle_internal_callback(status, &ensured) && !ensured)
            {
                return INT64_MAX;
            }
        }
    }
}

bool
coordinator_link :: handle_internal_callback(replicant_returncode* status, bool* ensured)
{
    m_id = -1;

    if (m_status != REPLICANT_SUCCESS)
    {
        if (m_output)
        {
            replicant_destroy_output(m_output, m_output_sz);
            m_output = NULL;
            m_output_sz = 0;
        }

        *status = m_status;
        *ensured = false;
        return true;
    }

    if (m_state == WAITING_ON_BROADCAST)
    {
        if (!begin_fetching_config(status))
        {
            *ensured = false;
            return true;
        }

        return false;
    }

    e::unpacker up(m_output, m_output_sz);
    configuration new_config;
    up = up >> new_config;
    replicant_destroy_output(m_output, m_output_sz);
    m_output = NULL;
    m_output_sz = 0;

    if (up.error())
    {
        *status = REPLICANT_MISBEHAVING_SERVER;
        *ensured = false;
        return true;
    }

    if (m_config.cluster() != 0 &&
        m_config.cluster() != new_config.cluster())
    {
        *status = REPLICANT_MISBEHAVING_SERVER;
        *ensured = false;
        return true;
    }

    m_config = new_config;
    *ensured = true;
    return true;
}

bool
coordinator_link :: begin_waiting_on_broadcast(replicant_returncode* status)
{
    assert(m_id == -1);
    assert(m_output == NULL);
    assert(m_output_sz == 0);
    m_id = m_repl.wait("hyperdex", "config",
                       m_config.version(), &m_status);
    m_state = WAITING_ON_BROADCAST;
    *status = m_status;
    return m_id >= 0;
}

bool
coordinator_link :: begin_fetching_config(replicant_returncode* status)
{
    assert(m_id == -1);
    assert(m_output == NULL);
    assert(m_output_sz == 0);
    m_id = m_repl.send("hyperdex", "get-config", "", 0,
                       &m_status, &m_output, &m_output_sz);
    m_state = FETCHING_CONFIG;
    *status = m_status;
    return m_id >= 0;
}
