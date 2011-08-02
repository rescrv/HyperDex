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

#ifndef hyperdex_coordinatorlink_h_
#define hyperdex_coordinatorlink_h_

// POSIX
#include <poll.h>

// STL
#include <string>

// po6
#include <po6/net/location.h>
#include <po6/net/socket.h>

// HyperDex
#include <hyperdex/configuration.h>

namespace hyperdex
{

class coordinatorlink
{
    public:
        enum returncode
        {
            SHUTDOWN    = 0,
            SUCCESS     = 1,
            CONNECTFAIL = 2,
            DISCONNECT  = 3,
            LOGICERROR  = 4
        };

    public:
        coordinatorlink(const po6::net::location& coordinator);
        ~coordinatorlink() throw ();

    // Unacknowledged is true if the current configuration has not been
    // acknowledged.  Once acknowledge is called, it flips the state of
    // "unacknowledged" to false until a new config is received.
    public:
        bool unacknowledged() const;
        returncode acknowledge();

    public:
        void set_announce(const std::string& announce) { m_announce = announce + "\n"; }
        returncode connect();
        returncode loop(size_t iterations, int timeout);
        void shutdown();

    public:
        const pollfd& pfd() const { return m_pfd; }
        bool connected() { return m_sock.get() >= 0; }
        const hyperdex::configuration& config() const;

    private:
        coordinatorlink(const coordinatorlink&);

    private:
        returncode send_to_coordinator(const char* msg, size_t len);
        void reset(); // Reset the config and socket (calls reset_config).
        void reset_config(); // Prepare a new config.

    private:
        coordinatorlink& operator = (const coordinatorlink&);

    private:
        const po6::net::location m_coordinator;
        std::string m_announce;
        bool m_shutdown;
        bool m_acknowledged;
        bool m_config_valid;
        configuration m_config;
        po6::net::socket m_sock;
        pollfd m_pfd;
        e::buffer m_buffer;
};

} // namespace hyperdex

#endif // hyperdex_coordinatorlink_h_
