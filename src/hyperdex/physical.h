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
#include <e/intrusive_ptr.h>
#include <e/lockfree_hash_map.h>

// HyperDex
#include <hyperdex/fifo_work_queue.h>

namespace hyperdex
{

class physical
{
    public:
        physical(ev::loop_ref lr, const po6::net::ipaddr& ip, bool listen = true);
        ~physical();

    // Pause/unpause or completely stop recv of messages.  Paused threads will
    // not hold locks, and therefore will not pose risk of deadlock.
    public:
        void pause() { m_incoming.pause(); }
        void unpause() { m_incoming.unpause(); }
        size_t num_paused() { return m_incoming.num_paused(); }
        void shutdown();

    // Send and recv messages.
    public:
        void send(const po6::net::location& to, e::buffer* msg); // Consumes msg.
        bool recv(po6::net::location* from, e::buffer* msg);
        // Deliver a message (put it on the queue) as if it came from "from".
        void deliver(const po6::net::location& from, const e::buffer& msg);
        // This is lossy.  The only time you can rely upon this is when there is
        // only one thread touching the object, and that same thread is calling
        // pending.
        bool pending() { return !m_incoming.optimistically_empty(); }

    // Figure out our own socket info.
    public:
        po6::net::location inbound() { return m_listen.getsockname(); }
        po6::net::location outbound() { return m_bindto; }

    private:
        struct message
        {
            message() : loc(), buf() {}
            ~message() throw () {}

            po6::net::location loc;
            e::buffer buf;
        };

        struct channel
        {
            public:
                channel(ev::loop_ref lr,
                        po6::net::socket* conn,
                        physical* manager);
                ~channel();

            public:
                void io_cb(ev::io& w, int revents);
                void read_cb(ev::io& w);
                void write_cb(ev::io& w);
                void write_step(); // mtx must be held by caller.
                void shutdown();

            public:
                po6::threads::mutex mtx; // Anyone touching a channel should hold this.
                po6::net::socket soc; // The socket over which we are communicating.
                po6::net::location loc; // A cached soc.getpeername.
                e::lockfree_fifo<e::buffer> outgoing; // Messages buffered for writing.
                e::buffer outprogress; // When writing to the network, we buffer partial writes here.
                e::buffer inprogress; // When reading from the network, we buffer partial reads here.
                ev::io io; // The libev watcher.
                physical* manager; // The outer manager class.

            private:
                friend class e::intrusive_ptr<channel>;

            private:
                channel(const channel&);

            private:
                channel& operator = (const channel&);

            private:
                size_t m_ref;
                bool m_shutdown;
        };

    private:
        physical(const physical&);

    private:
        // get_channel creates a new channel, or finds an existing one matching
        // the specific parameter.
        e::intrusive_ptr<channel> get_channel(const po6::net::location& to);
        e::intrusive_ptr<channel> get_channel(po6::net::socket* soc);
        void drop_channel(e::intrusive_ptr<channel> chan);
        void refresh(ev::async& a, int revent);
        void accept_connection(ev::io& i, int revent);

    private:
        physical& operator = (const physical&);

    private:
        ev::loop_ref m_lr;
        ev::async m_wakeup;
        po6::net::socket m_listen;
        ev::io m_listen_event;
        po6::net::location m_bindto;
        hyperdex::fifo_work_queue<message> m_incoming; // Messages buffered for reading.
        e::lockfree_hash_map<po6::net::location, e::intrusive_ptr<channel>, po6::net::location::hash> m_channels;
        po6::threads::mutex m_location_set_lock;
        std::set<po6::net::location> m_location_set;
};

} // namespace hyperdex

#endif // hyperdex_physical_h_
