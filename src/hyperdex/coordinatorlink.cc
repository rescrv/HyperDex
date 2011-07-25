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

// Google Log
#include <glog/logging.h>

// e
#include <e/timer.h>

// HyperDex
#include <hyperdex/coordinatorlink.h>

hyperdex :: coordinatorlink :: coordinatorlink(const po6::net::location& coordinator,
                                               const std::string& announce)
    : m_coordinator(coordinator)
    , m_announce(announce + "\n")
    , m_shutdown(false)
    , m_sent_ack(true)
    , m_config_valid(true)
    , m_config()
    , m_sock()
    , m_partial()
{
}

hyperdex :: coordinatorlink :: ~coordinatorlink() throw ()
{
    shutdown();
}

bool
hyperdex :: coordinatorlink :: unacknowledged() const
{
    return !m_sent_ack;
}

void
hyperdex :: coordinatorlink :: acknowledge()
{
    if (!m_shutdown)
    {
        m_sent_ack = true;
        send_to_coordinator("ACK\n", 4);
        m_config = hyperdex::configuration();
        m_config_valid = true;
    }
}

void
hyperdex :: coordinatorlink :: loop()
{
    while (!m_shutdown && m_sent_ack)
    {
        if (m_sock.get() < 0)
        {
            m_config = hyperdex::configuration();
            m_config_valid = true;
            m_partial.clear();
            m_sock.reset(m_coordinator.address.family(), SOCK_STREAM, IPPROTO_TCP);

            try
            {
                m_sock.connect(m_coordinator);

                if (m_sock.xwrite(m_announce.c_str(), m_announce.size()) != m_announce.size())
                {
                    PLOG(WARNING) << "could not send whole annouce string to coordinator";
                    m_sock.close();
                    e::sleep_ms(1, 0);
                    return;
                }

                LOG(INFO) << "connected to coordinator";
            }
            catch (po6::error& e)
            {
                int saved_errno = errno;
                errno = e;
                PLOG(WARNING) << "could not connect to coordinator";
                errno = saved_errno;
                m_sock.close();
                e::sleep_ms(1, 0);
                return;
            }
        }

        pollfd pfd;
        pfd.fd = m_sock.get();
        pfd.events = POLLIN;
        int polled = poll(&pfd, 1, 100);

        if (polled < 0 && errno != EINTR)
        {
            PLOG(WARNING) << "could not poll coordinator";
            m_sock.close();
            return;
        }

        if (polled <= 0)
        {
            return;
        }

        size_t ret;

        try
        {
            ret = e::read(&m_sock, &m_partial, 2048);
        }
        catch (po6::error& e)
        {
            PLOG(WARNING) << "could not read from coordinator";
            m_sock.close();
            continue;
        }

        if (ret == 0)
        {
            m_sock.close();
            continue;
        }

        size_t index;

        while ((index = m_partial.index('\n')) < m_partial.size())
        {
            std::string s(static_cast<const char*>(m_partial.get()), index);
            m_partial.trim_prefix(index + 1);

            if (s == "end\tof\tline")
            {
                if (m_config_valid)
                {
                    LOG(INFO) << "Installing new configuration file.";
                    m_sent_ack = false;
                    break;
                }
                else
                {
                    LOG(WARNING) << "Coordinator sent us a bad configuration file.";
                    send_to_coordinator("BAD\n", 4);
                    m_config = hyperdex::configuration();
                    m_config_valid = true;
                }
            }
            else
            {
                m_config_valid &= m_config.add_line(s);
            }
        }
    }
}

void
hyperdex :: coordinatorlink :: shutdown()
{
    if (!m_shutdown)
    {
        m_shutdown = true;
    }
}

const hyperdex::configuration&
hyperdex :: coordinatorlink :: config() const
{
    return m_config;
}

bool
hyperdex :: coordinatorlink :: send_to_coordinator(const char* msg, size_t len)
{
    try
    {
        if (m_sock.xwrite(msg, len) != len)
        {
            PLOG(WARNING) << "could not communicate with coordinator; dropping connection";
            m_sent_ack = true;
            m_sock.close();
            return false;
        }
    }
    catch (po6::error& e)
    {
        int saved_errno = errno;
        errno = e;
        PLOG(WARNING) << "could not communicate with coordinator; dropping connection";
        errno = saved_errno;
        m_sent_ack = true;
        m_sock.close();
        return false;
    }

    return true;
}
