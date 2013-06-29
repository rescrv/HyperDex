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

        int64_t lid = m_repl.loop(m_id, timeout, status);

        if (lid < 0 && *status == REPLICANT_TIMEOUT && timeout >= 0)
        {
            return true;
        }
        else if (lid < 0)
        {
            return false;
        }

        assert(lid == m_id);
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
            return false;
        }

        if (m_state == WAITING_ON_BROADCAST)
        {
            if (!begin_fetching_config(status))
            {
                return false;
            }

            continue;
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
            return false;
        }

        if (m_config.cluster() != 0 &&
            m_config.cluster() != new_config.cluster())
        {
            *status = REPLICANT_MISBEHAVING_SERVER;
            return false;
        }

        m_config = new_config;
        return true;
    }
}

coordinator_link::poll_fd_t
coordinator_link :: poll_fd()
{
    return m_repl.poll_fd();
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
