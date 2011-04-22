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

#ifndef hyperdex_physical_h_
#define hyperdex_physical_h_

// STL
#include <map>
#include <queue>
#include <tr1/memory>
#include <vector>

// libev
#include <ev++.h>

// po6
#include <po6/net/location.h>
#include <po6/net/socket.h>
#include <po6/threads/mutex.h>
#include <po6/threads/rwlock.h>

// HyperDex
#include <hyperdex/lockingq.h>

namespace hyperdex
{

class physical
{
    public:
        physical(ev::loop_ref lr, const po6::net::location& us);
        ~physical();

    // Send and recv messages.
    public:
        void send(const po6::net::location& to, const std::vector<char>& msg);
        bool recv(po6::net::location* from, std::vector<char>* msg);

    private:
        struct channel
        {
            channel(ev::loop_ref lr,
                    const po6::net::location& from,
                    const po6::net::location& to);
            ~channel();
            void io_cb(ev::io& w, int revents);
            void read_cb(ev::io& w);
            void write_cb(ev::io& w);

            po6::threads::mutex mtx; // Anyone touching a channel should hold this.
            po6::net::socket soc; // The socket over which we are communicating.
            po6::net::location loc; // Cached soc.getpeername().
            ev::io io; // The libev watcher.
            std::vector<char> inprogress; // When reading from the network, we buffer partial reads here.
            std::vector<char> outprogress; // When writing to the network, we buffer partial writes here.
            std::queue<std::vector<char> > outgoing; // Messages buffered for writing.
        };

        struct message
        {
            message();
            ~message();

            po6::net::location loc;
            std::vector<char> buf;
        };

    private:
        physical(const physical&);

    private:
        std::tr1::shared_ptr<channel> create_connection(const po6::net::location& to);
        void refresh(ev::async& a, int revent);

    private:
        physical& operator = (const physical&);

    private:
        ev::loop_ref m_lr;
        ev::async m_wakeup;
        po6::net::location m_us;
        po6::threads::rwlock m_lock; // Hold this when changing the location map.
        std::vector<std::tr1::shared_ptr<channel> > m_channels; // The channels we have.
        std::map<po6::net::location, size_t> m_location_map; // A mapping from locations to indices in m_channels.
        hyperdex::lockingq<message> m_incoming; // Messages buffered for reading.
};

} // namespace hyperdex

#endif // hyperdex_physical_h_
