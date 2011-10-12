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

#ifndef hyperdaemon_physical_h_
#define hyperdaemon_physical_h_

// STL
#include <map>
#include <queue>
#include <tr1/memory>

// po6
#include <po6/net/ipaddr.h>
#include <po6/net/socket.h>
#include <po6/threads/cond.h>
#include <po6/threads/mutex.h>
#include <po6/threads/rwlock.h>

// e
#include <e/buffer.h>
#include <e/hazard_ptrs.h>
#include <e/lockfree_fifo.h>
#include <e/lockfree_hash_map.h>
#include <e/worker_barrier.h>

namespace hyperdaemon
{

class physical
{
    public:
        enum returncode
        {
            SHUTDOWN    = 0,
            SUCCESS     = 1,
            QUEUED      = 2,
            CONNECTFAIL = 3,
            DISCONNECT  = 4,
            LOGICERROR  = 5
        };

    public:
        physical(const po6::net::ipaddr& ip,
                 in_port_t incoming,
                 in_port_t outgoing,
                 bool listen,
                 size_t num_threads);
        ~physical() throw ();

    // Pause/unpause or completely stop recv of messages.  Paused threads will
    // not hold locks, and therefore will not pose risk of deadlock.
    public:
        void pause();
        void unpause();
        void shutdown();

    // Send and recv messages.
    public:
        returncode send(const po6::net::location& to, e::buffer* msg);
        returncode recv(po6::net::location* from, e::buffer* msg);
        // Deliver a message (put it on the queue) as if it came from "from".
        void deliver(const po6::net::location& from, const e::buffer& msg);

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

        class channel
        {
            public:
                channel(po6::net::socket* conn);
                ~channel() throw ();

            public:
                po6::threads::mutex mtx; // Anyone touching the socket should hold this.
                po6::net::socket soc; // The socket over which we are communicating.
                po6::net::location loc; // A cached soc.getpeername.
                e::lockfree_fifo<e::buffer> outgoing; // Messages buffered for writing.
                e::buffer outprogress; // When writing to the network, we buffer partial writes here.
                e::buffer inprogress; // When reading from the network, we buffer partial reads here.

            private:
                channel(const channel&);

            private:
                channel& operator = (const channel&);
        };

        typedef std::auto_ptr<e::hazard_ptrs<channel, 1, size_t>::hazard_ptr> hazard_ptr;

    private:
        physical(const physical&);

    private:
        // get_channel creates a new channel, or finds an existing one matching
        // the specific parameter.
        returncode get_channel(const hazard_ptr& hptr, const po6::net::location& to, channel** chan);
        returncode get_channel(const hazard_ptr& hptr, po6::net::socket* to, channel** chan);
        // worker functions.
        int work_accept(const hazard_ptr& hptr);
        // work_close must be called without holding chan->mtx.
        void work_close(const hazard_ptr& hptr, channel* chan);
        // work_read must be called without holding chan->mtx.
        bool work_read(const hazard_ptr& hptr, channel* chan, po6::net::location* from, e::buffer* msg, returncode* res);
        // work_write must be called while holding chan->mtx.
        bool work_write(channel* chan);

    private:
        physical& operator = (const physical&);

    private:
        const long m_max_fds;
        volatile bool m_shutdown;
        po6::net::socket m_listen;
        po6::net::location m_bindto;
        e::lockfree_fifo<message> m_incoming;
        e::lockfree_hash_map<po6::net::location, int, po6::net::location::hash> m_locations;
        e::hazard_ptrs<channel, 1, size_t> m_hazard_ptrs;
        std::vector<channel*> m_channels;
        std::vector<int> m_channels_mask;
        e::worker_barrier m_pause_barrier;
};

} // namespace hyperdaemon

#endif // hyperdaemon_physical_h_
