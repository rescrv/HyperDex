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
#include <e/striped_lock.h>
#include <e/worker_barrier.h>

// Forward declarations
struct epoll_event;

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
        // Every message has a header of this size.  Senders must allocate this
        // space.  Receivers may ignore this space.
        size_t header_size() const { return sizeof(uint32_t); }
        returncode send(const po6::net::location& to, std::auto_ptr<e::buffer> msg);
        returncode recv(po6::net::location* from, std::auto_ptr<e::buffer>* msg);
        // Deliver a message (put it on the queue) as if it came from "from".
        // This will *not* wake up threads.  This is intentional so the thread
        // calling deliver will possibly pull the delivered item from the queue.
        void deliver(const po6::net::location& from, std::auto_ptr<e::buffer> msg);

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
            std::auto_ptr<e::buffer> buf;
        };

        struct pending
        {
            pending() : fd(), events() {}
            pending(int f, uint32_t e) : fd(f), events(e) {}
            ~pending() throw () {}

            int fd;
            uint32_t events;
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
                e::lockfree_fifo<std::auto_ptr<e::buffer> > outgoing; // Messages buffered for writing.
                std::auto_ptr<e::buffer> outnow; // The current message we are writing to the network.
                e::slice outprogress; // A pointer into what we've written so far.
                std::auto_ptr<e::buffer> inprogress; // When reading from the network, we buffer partial reads here.
                size_t inoffset; // How much we've buffered in inbuffer.
                char inbuffer[sizeof(uint32_t)]; // We buffer reads here when we haven't read enough to set the size of inprogress.

            private:
                channel(const channel&);

            private:
                channel& operator = (const channel&);
        };

        typedef std::auto_ptr<e::hazard_ptrs<channel, 1>::hazard_ptr> hazard_ptr;

    private:
        physical(const physical&);

    private:
        // Receive an event to handle.
        int receive_event(int*fd, uint32_t* events);
        // Postpone an event to handle later.
        void postpone_event(int fd, uint32_t events);
        // Add a new file descriptor to epoll.
        int add_descriptor(int fd);
        // Get a reference to an existing channel
        returncode get_channel(const hazard_ptr& hptr,
                               const po6::net::location& to,
                               channel** chan);
        // Create a new channel using the socket pointed to by 'to'.
        returncode get_channel(const hazard_ptr& hptr,
                               po6::net::socket* to,
                               channel** chan);
        // worker functions.
        int work_accept(const hazard_ptr& hptr);
        // work_close must be called without holding chan->mtx.
        void work_close(const hazard_ptr& hptr, channel* chan);

        // work_read must be called without holding chan->mtx.
        // It will return true if *from, *msg, *res should be returned to the
        // client and false otherwise.
        bool work_read(const hazard_ptr& hptr, channel* chan,
                       po6::net::location* from, std::auto_ptr<e::buffer>* msg,
                       returncode* res);
        // work_write must be called while holding chan->mtx.
        // It will return true if progress was made, and false if there is an
        // error to report (using *res).  If there is an error to report, the
        // caller must call work_close.
        bool work_write(channel* chan);

    private:
        physical& operator = (const physical&);

    private:
        const long m_max_fds;
        volatile bool m_shutdown;
        po6::io::fd m_epoll;
        po6::net::socket m_listen;
        po6::net::location m_bindto;
        e::worker_barrier m_pause_barrier;
        e::striped_lock<po6::threads::mutex> m_connectlocks;
        e::lockfree_fifo<message> m_incoming;
        e::lockfree_hash_map<po6::net::location, int, po6::net::location::hash> m_locations;
        e::hazard_ptrs<channel, 1> m_hazard_ptrs;
        std::vector<channel*> m_channels;
        e::lockfree_fifo<pending> m_postponed;
};

} // namespace hyperdaemon

#endif // hyperdaemon_physical_h_
