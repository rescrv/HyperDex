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

// libev
#include <ev++.h>

// po6
#include <po6/net/ipaddr.h>
#include <po6/net/socket.h>
#include <po6/threads/mutex.h>
#include <po6/threads/rwlock.h>

// e
#include <e/buffer.h>

// HyperDex
#include <hyperdex/lockingq.h>

namespace hyperdex
{

class physical
{
    public:
        physical(ev::loop_ref lr, const po6::net::ipaddr& ip, bool listen = true);
        ~physical();

    // Send and recv messages.
    public:
        void send(const po6::net::location& to, e::buffer* msg); // Consumes msg.
        bool recv(po6::net::location* from, e::buffer* msg);
        // Deliver a message (put it on the queue) as if it came from "from".
        void deliver(const po6::net::location& from, const e::buffer& msg);
        void shutdown();
        bool pending() { return m_incoming.size() > 0; }
        po6::net::location inbound() { return m_listen.getsockname(); }
        po6::net::location outbound() { return m_bindto; }

    private:
        struct message
        {
            message();
            ~message();

            po6::net::location loc;
            e::buffer buf;
        };

        struct channel
        {
            channel(ev::loop_ref lr,
                    physical* m,
                    po6::net::socket* accept_from);
            channel(ev::loop_ref lr,
                    physical* m,
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
            e::buffer inprogress; // When reading from the network, we buffer partial reads here.
            e::buffer outprogress; // When writing to the network, we buffer partial writes here.
            std::queue<e::buffer> outgoing; // Messages buffered for writing.
            physical* manager; // The outer manager class.

            private:
                channel(const channel&);

            private:
                channel& operator = (const channel&);
        };

    private:
        physical(const physical&);

    private:
        std::tr1::shared_ptr<channel> create_connection(const po6::net::location& to);
        void remove_connection(channel* to_remove);
        void refresh(ev::async& a, int revent);
        void accept_connection(ev::io& i, int revent);
        void place_channel(std::tr1::shared_ptr<channel> chan);

    private:
        physical& operator = (const physical&);

    private:
        ev::loop_ref m_lr;
        ev::async m_wakeup;
        po6::net::socket m_listen;
        ev::io m_listen_event;
        po6::net::location m_bindto;
        po6::threads::rwlock m_lock; // Hold this when changing the location map.
        std::vector<std::tr1::shared_ptr<channel> > m_channels; // The channels we have.
        std::map<po6::net::location, size_t> m_location_map; // A mapping from locations to indices in m_channels.
        hyperdex::lockingq<message> m_incoming; // Messages buffered for reading.
};

} // namespace hyperdex

#endif // hyperdex_physical_h_
