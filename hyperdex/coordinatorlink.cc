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

// C++
#include <sstream>

// e
#include <e/bufferio.h>

// HyperDex
#include "hyperdex/hyperdex/coordinatorlink.h"
#include "hyperdex/configuration_parser.h"

hyperdex :: coordinatorlink :: coordinatorlink(const po6::net::location& coordinator)
    : m_lock()
    , m_coordinator(coordinator)
    , m_announce()
    , m_acknowledged(true)
    , m_config()
    , m_sock()
    , m_buffer()
    , m_reported_failures()
    , m_warnings_issued()
{
    reset();
}

hyperdex :: coordinatorlink :: ~coordinatorlink() throw ()
{
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
    returncode ret = send_to_coordinator("ACK\n", 4);

    if (ret == SUCCESS)
    {
        reset_config();
    }

    return ret;
}

hyperdex::configuration
hyperdex :: coordinatorlink :: config()
{
    po6::threads::mutex::hold hold(&m_lock);
    return m_config;
}

void
hyperdex :: coordinatorlink :: set_announce(const std::string& announce)
{
    po6::threads::mutex::hold hold(&m_lock);
    m_announce = announce + "\n";
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: warn_location(const po6::net::location& loc)
{
    po6::threads::mutex::hold hold(&m_lock);
    ++m_warnings_issued[loc];

    if (m_warnings_issued[loc] > 3)
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
hyperdex :: coordinatorlink :: transfer_fail(uint16_t xfer_id)
{
    po6::threads::mutex::hold hold(&m_lock);
    std::ostringstream ostr;
    ostr << "transfer_fail\t" << xfer_id << "\n";
    return send_to_coordinator(ostr.str().c_str(), ostr.str().size());
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: transfer_golive(uint16_t xfer_id)
{
    po6::threads::mutex::hold hold(&m_lock);
    std::ostringstream ostr;
    ostr << "transfer_golive\t" << xfer_id << "\n";
    return send_to_coordinator(ostr.str().c_str(), ostr.str().size());
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: transfer_complete(uint16_t xfer_id)
{
    po6::threads::mutex::hold hold(&m_lock);
    std::ostringstream ostr;
    ostr << "transfer_complete\t" << xfer_id << "\n";
    return send_to_coordinator(ostr.str().c_str(), ostr.str().size());
}

int
hyperdex :: coordinatorlink :: poll_on()
{
    return m_sock.get();
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: quiesced(const std::string& quiesce_state_id)
{
    po6::threads::mutex::hold hold(&m_lock);
    std::ostringstream ostr;
    ostr << "quiesced\t" << quiesce_state_id << "\n";
    return send_to_coordinator(ostr.str().c_str(), ostr.str().size());
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: poll(int connect_attempts, int timeout)
{
    int attempt_num = 0;

    while (true)
    {
        if (connect_attempts >= 0 && attempt_num == connect_attempts)
        {
            return CONNECTFAIL;
        }

        if (m_sock.get() < 0)
        {
            po6::threads::mutex::hold hold(&m_lock);
            reset_config();
            m_sock.reset(m_coordinator.address.family(), SOCK_STREAM, IPPROTO_TCP);
            ++attempt_num;

            try
            {
                m_sock.connect(m_coordinator);

                switch (send_to_coordinator(m_announce.c_str(), m_announce.size()))
                {
                    case SUCCESS:
                        break;
                    case CONNECTFAIL:
                        reset();
                        continue;
                    default:
                        abort();
                }
            }
            catch (po6::error& e)
            {
                reset();
                continue;
            }
        }

        pollfd pfd;
        pfd.fd = m_sock.get();
        pfd.events = POLLIN;
        int polled = ::poll(&pfd, 1, timeout);
        po6::threads::mutex::hold hold(&m_lock);

        if (polled < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
        {
            reset();
            continue;
        }

        if (polled <= 0 && m_buffer.empty())
        {
            return SUCCESS;
        }

        if ((pfd.revents & POLLHUP) || (pfd.revents & POLLERR))
        {
            reset();
            continue;
        }

        char buf[4096];
        ssize_t ret = m_sock.read(buf, 4096);

        if (ret <= 0)
        {
            reset();
            continue;
        }

        m_buffer += std::string(buf, ret);
        size_t index = m_buffer.find("end of line\n");

        if (index != std::string::npos)
        {
            std::string configtext = m_buffer.substr(0, index);
            m_buffer = m_buffer.substr(index + 12);

            // Parse the config
            configuration_parser cp;
            configuration_parser::error e;
            e = cp.parse(configtext);

            if (e == configuration_parser::CP_SUCCESS)
            {
                m_acknowledged = false;
                m_config = cp.generate();

                if (!m_config.shutdown())
                {
                    return SUCCESS;
                }
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
    }
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: send_to_coordinator(const char* msg, size_t len)
{
    if (m_sock.xwrite(msg, len) != static_cast<ssize_t>(len))
    {
        reset();
        return CONNECTFAIL;
    }

    return SUCCESS;
}

hyperdex::coordinatorlink::returncode
hyperdex :: coordinatorlink :: send_failure(const po6::net::location& loc)
{
    if (m_reported_failures.find(loc) != m_reported_failures.end())
    {
        return SUCCESS;
    }

    std::ostringstream ostr;
    ostr << "fail_host\t" << loc << "\n";
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
    shutdown(m_sock.get(), SHUT_RDWR);
    m_sock.close();
    m_buffer = "";
}

void
hyperdex :: coordinatorlink :: reset_config()
{
    m_acknowledged = true;
    m_config = configuration();
    m_reported_failures.clear();
    m_warnings_issued.clear();
}
