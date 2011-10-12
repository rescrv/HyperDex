// Copyright (c) 2011, Cornell University
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

// POSIX
#include <poll.h>

// HyperDex
#include "hyperdex/coordinatorlink.h"

hyperdex :: coordinatorlink :: coordinatorlink(const po6::net::location& coordinator)
    : m_lock()
    , m_coordinator(coordinator)
    , m_announce()
    , m_shutdown(false)
    , m_acknowledged(true)
    , m_config_valid(true)
    , m_config()
    , m_sock()
    , m_pfd()
    , m_buffer()
    , m_reported_failures()
    , m_warnings_issued()
{
    reset();
}

hyperdex :: coordinatorlink :: ~coordinatorlink() throw ()
{
    shutdown();
}

bool
hyperdex :: coordinatorlink :: unacknowledged()
{
    po6::threads::mutex::hold hold(&m_lock);
    return !m_acknowledged;
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: acknowledge()
{
    po6::threads::mutex::hold hold(&m_lock);

    if (m_shutdown)
    {
        return SHUTDOWN;
    }

    returncode ret = send_to_coordinator("ACK\n", 4);

    if (ret == SUCCESS)
    {
        reset_config();
    }

    return ret;
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: warn_location(const po6::net::location& loc)
{
    po6::threads::mutex::hold hold(&m_lock);
    ++m_warnings_issued[loc];

    if (m_warnings_issued[loc] > 5)
    {
        return send_failure(loc);
    }

    return SUCCESS;
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: fail_location(const po6::net::location& loc)
{
    po6::threads::mutex::hold hold(&m_lock);
    return send_failure(loc);
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: fail_transfer(uint16_t xfer_id)
{
    po6::threads::mutex::hold hold(&m_lock);

    if (m_shutdown)
    {
        return SHUTDOWN;
    }

    std::ostringstream ostr;
    ostr << "fail_transfer\t" << xfer_id << "\n";
    return send_to_coordinator(ostr.str().c_str(), ostr.str().size());
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: connect()
{
    po6::threads::mutex::hold hold(&m_lock);

    if (m_sock.get() >= 0)
    {
        return SUCCESS;
    }

    reset_config();
    m_sock.reset(m_coordinator.address.family(), SOCK_STREAM, IPPROTO_TCP);

    try
    {
        m_sock.connect(m_coordinator);
        m_pfd.fd = m_sock.get();
        m_pfd.events = POLLIN;
        return send_to_coordinator(m_announce.c_str(), m_announce.size());
    }
    catch (po6::error& e)
    {
        reset();
        return CONNECTFAIL;
    }
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: loop(size_t iterations, int timeout)
{
    size_t iter = 0;

    while (m_acknowledged && iter < iterations)
    {
        po6::threads::mutex::hold hold(&m_lock);
        if (m_shutdown)
        {
            return SHUTDOWN;
        }

        if (m_sock.get() < 0)
        {
            reset();
            return DISCONNECT;
        }

        m_lock.unlock();
        int polled = poll(&m_pfd, 1, timeout);
        m_lock.lock();

        if (polled < 0)
        {
            if (errno == EINTR)
            {
                return SUCCESS;
            }
            else
            {
                reset();
                return DISCONNECT;
            }
        }

        size_t ret;

        try
        {
            ret = e::read(&m_sock, &m_buffer, 2048);
        }
        catch (po6::error& e)
        {
            reset();
            return DISCONNECT;
        }

        if (ret == 0)
        {
            reset();
            return DISCONNECT;
        }

        size_t index;

        while ((index = m_buffer.index('\n')) < m_buffer.size())
        {
            std::string line(static_cast<const char*>(m_buffer.get()), index);
            m_buffer.trim_prefix(index + 1);

            if (line == "end\tof\tline")
            {
                if (m_config_valid)
                {
                    m_acknowledged = false;
                    return SUCCESS;
                }
                else
                {
                    reset_config();
                    returncode code = send_to_coordinator("BAD\n", 4);

                    if (code != SUCCESS)
                    {
                        return code;
                    }
                }
            }
            else
            {
                m_config_valid &= m_config.add_line(line);
            }
        }

        ++ iter;
    }

    return SUCCESS;
}

void
hyperdex :: coordinatorlink :: shutdown()
{
    po6::threads::mutex::hold hold(&m_lock);

    if (!m_shutdown)
    {
        m_shutdown = true;
    }
}

pollfd
hyperdex :: coordinatorlink :: pfd()
{
    po6::threads::mutex::hold hold(&m_lock);
    return m_pfd;
}

bool
hyperdex :: coordinatorlink :: connected()
{
    po6::threads::mutex::hold hold(&m_lock);
    return m_sock.get() >= 0;
}

hyperdex::configuration
hyperdex :: coordinatorlink :: config()
{
    po6::threads::mutex::hold hold(&m_lock);
    return m_config;
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: send_to_coordinator(const char* msg, size_t len)
{
    try
    {
        if (m_sock.xwrite(msg, len) != len)
        {
            reset();
            return DISCONNECT;
        }
    }
    catch (po6::error& e)
    {
        reset();
        return DISCONNECT;
    }

    return SUCCESS;
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: send_failure(const po6::net::location& loc)
{
    if (m_shutdown)
    {
        return SHUTDOWN;
    }

    std::ostringstream ostr;
    ostr << "fail_location\t" << loc << "\n";
    returncode ret = send_to_coordinator(ostr.str().c_str(), ostr.str().size());

    if (ret == SUCCESS)
    {
        m_reported_failures.insert(loc);
    }

    return ret;
}

void
hyperdex :: coordinatorlink :: reset()
{
    reset_config();
    m_sock.close();
    m_pfd.fd = -1;
    m_pfd.events = 0;
    m_buffer.clear();
}

void
hyperdex :: coordinatorlink :: reset_config()
{
    m_acknowledged = true;
    m_config_valid = true;
    m_config = configuration();
    m_reported_failures.clear();
    m_warnings_issued.clear();
}
