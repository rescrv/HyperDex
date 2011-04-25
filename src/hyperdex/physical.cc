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

// Google Log
#include <glog/logging.h>

// HyperDex
#include <hyperdex/physical.h>

hyperdex :: physical :: physical(ev::loop_ref lr, const po6::net::ipaddr& ip)
    : m_lr(lr)
    , m_wakeup(lr)
    , m_listen(ip.family(), SOCK_STREAM, IPPROTO_TCP)
    , m_listen_event(lr)
    , m_bindto()
    , m_lock()
    , m_channels()
    , m_location_map()
    , m_incoming()
{
    // Enable other threads to wake us from the event loop.
    m_wakeup.set<physical, &physical::refresh>(this);
    m_wakeup.start();

    // Enable other hosts to connect to us.
    m_listen.bind(po6::net::location(ip, 0));
    m_listen.listen(4);
    m_listen_event.set<physical, &physical::accept_connection>(this);
    m_listen_event.set(m_listen.get(), ev::READ);
    m_listen_event.start();

    // Setup a channel which connects to ourself.  The purpose of this channel
    // is two-fold:  first, it gives a really cheap way of doing self-messaging
    // without the queue at the higher layer (at the expense of a bigger
    // performance hit); second, it gives us a way to get a port to which we
    // may bind repeatedly when establishing connections.
    //
    // TODO:  Make establishing this channel only serve the second purpose.
    std::tr1::shared_ptr<channel> chan;
    chan.reset(new channel(m_lr, this, po6::net::location(ip, 0), m_listen.getsockname()));
    m_location_map[chan->loc] = m_channels.size();
    m_channels.push_back(chan);
    m_bindto = chan->soc.getsockname();
}

hyperdex :: physical :: ~physical()
{
}

void
hyperdex :: physical :: send(const po6::net::location& to,
                             const std::vector<char>& msg)
{
    // Get a reference to the channel for "to" with mtx held.
    m_lock.rdlock();
    std::map<po6::net::location, size_t>::iterator position;
    std::tr1::shared_ptr<channel> chan;

    if ((position = m_location_map.find(to)) == m_location_map.end())
    {
        m_lock.unlock();
        m_lock.wrlock();
        chan = create_connection(to);

        if (!chan)
        {
            m_lock.unlock();
            return;
        }
    }
    else
    {
        chan = m_channels[position->second];
    }

    chan->mtx.lock();
    m_lock.unlock();

    // Add msg to the outgoing buffer.
    bool notify = chan->outgoing.empty() && chan->outprogress.empty();
    chan->outgoing.push(msg);

    // Notify the event loop thread.
    if (notify)
    {
        m_wakeup.send();
    }

    chan->mtx.unlock();
}

bool
hyperdex :: physical :: recv(po6::net::location* from,
                             std::vector<char>* msg)
{
    message m;
    bool ret = m_incoming.pop(&m);
    *from = m.loc;
    msg->swap(m.buf);
    return ret;
}

void
hyperdex :: physical :: shutdown()
{
    m_incoming.shutdown();
}

// Invariant:  m_lock.wrlock must be held.
std::tr1::shared_ptr<hyperdex :: physical :: channel>
hyperdex :: physical :: create_connection(const po6::net::location& to)
{
    std::map<po6::net::location, size_t>::iterator position;

    if ((position = m_location_map.find(to)) != m_location_map.end())
    {
        return m_channels[position->second];
    }

    try
    {
        std::tr1::shared_ptr<channel> chan;
        chan.reset(new channel(m_lr, this, m_bindto, to));
        place_channel(chan);
        return chan;
    }
    catch (...)
    {
        return std::tr1::shared_ptr<channel>();
    }
}

// Invariant:  no locks may be held.
void
hyperdex :: physical :: remove_connection(channel* to_remove)
{
    m_lock.wrlock();
    po6::net::location loc = to_remove->loc;
    std::map<po6::net::location, size_t>::iterator iter = m_location_map.find(loc);

    if (iter == m_location_map.end())
    {
        LOG(FATAL) << "The physical layer's address map got out of sync with its open channels.";
    }

    size_t index = iter->second;
    m_location_map.erase(iter);
    // This lock/unlock pair is necessary to ensure the channel is no longer in
    // use.  As we know that we are the only one who can use m_location_map and
    // m_channels, the only other possibility is that a send is in-progress
    // (after the rdlock is released, but before the chan lock is released).
    // Once this succeeds, we know that no one else may possibly use this
    // channel.
    m_channels[index]->mtx.lock();
    m_channels[index]->mtx.unlock();
    m_channels[index] = std::tr1::shared_ptr<channel>();
    m_lock.unlock();
}

void
hyperdex :: physical :: refresh(ev::async&, int)
{
    m_lock.rdlock();

    for (std::map<po6::net::location, size_t>::iterator i = m_location_map.begin();
            i != m_location_map.end(); ++ i)
    {
        if (m_channels[i->second])
        {
            m_channels[i->second]->mtx.lock();
            int flags = ev::READ;

            // If there is data to write.
            if (m_channels[i->second]->outprogress.size() > 0 ||
                m_channels[i->second]->outgoing.size() > 0)
            {
                flags |= ev::WRITE;
            }

            m_channels[i->second]->io.set(flags);
            m_channels[i->second]->mtx.unlock();
        }
    }

    m_lock.unlock();
}

void
hyperdex :: physical :: accept_connection(ev::io&, int)
{
    std::tr1::shared_ptr<channel> chan;

    try
    {
        chan.reset(new channel(m_lr, this, &m_listen));
    }
    catch (po6::error& e)
    {
        return;
    }

    m_lock.wrlock();

    try
    {
        place_channel(chan);
    }
    catch (...)
    {
        m_lock.unlock();
        throw;
    }

    m_lock.unlock();
}

// Invariant:  m_lock.wrlock must be held.
void
hyperdex :: physical :: place_channel(std::tr1::shared_ptr<channel> chan)
{
    // Find an empty slot.
    for (size_t i = 0; i < m_channels.size(); ++i)
    {
        if (!m_channels[i])
        {
            m_channels[i] = chan;
            m_location_map[chan->loc] = i;
            return;
        }
    }

    // Resize to create new slots.
    m_location_map[chan->loc] = m_channels.size();
    m_channels.push_back(chan);
}

hyperdex :: physical :: channel :: channel(ev::loop_ref lr,
                                           physical* m,
                                           po6::net::socket* accept_from)
    : mtx()
    , soc()
    , loc()
    , io(lr)
    , inprogress()
    , outprogress()
    , outgoing()
    , manager(m)
{
    accept_from->accept(&soc);
    io.set<channel, &channel::io_cb>(this);
    io.set(soc.get(), ev::READ);
    io.start();
}

hyperdex :: physical :: channel :: channel(ev::loop_ref lr,
                                           physical* m,
                                           const po6::net::location& from,
                                           const po6::net::location& to)
    : mtx()
    , soc(to.address.family(), SOCK_STREAM, IPPROTO_TCP)
    , loc(to)
    , io(lr)
    , inprogress()
    , outprogress()
    , outgoing()
    , manager(m)
{
    io.set<channel, &channel::io_cb>(this);
    io.set(soc.get(), ev::READ);
    io.start();

    soc.reuseaddr(true);
    soc.bind(from);
    soc.connect(to);
}

hyperdex :: physical :: channel :: ~channel()
{
    io.stop();
}

#define IO_BLOCKSIZE 65536

void
hyperdex :: physical :: channel :: io_cb(ev::io& w, int revents)
{
    if (revents & ev::READ)
    {
        read_cb(w);
    }
    else if (revents & ev::WRITE)
    {
        write_cb(w);
    }
}

void
hyperdex :: physical :: channel :: read_cb(ev::io&)
{
    if (!mtx.trylock())
    {
        return;
    }

    size_t startpos = inprogress.size();
    inprogress.resize(startpos + IO_BLOCKSIZE);

    try
    {
        size_t ret = soc.read(&inprogress.front() + startpos, IO_BLOCKSIZE);
        inprogress.resize(startpos + ret);

        if (ret == 0)
        {
            mtx.unlock();
            manager->remove_connection(this);
            return;
        }

        while (inprogress.size() >= sizeof(uint32_t))
        {
            uint32_t message_size;
            memmove(&message_size, &inprogress.front(), sizeof(uint32_t));
            message_size = ntohl(message_size);
            size_t end = message_size + sizeof(uint32_t);

            if (inprogress.size() < end)
            {
                break;
            }

            std::vector<char> buf(inprogress.front() + sizeof(uint32_t),
                                  inprogress.front() + end);
            message m;
            m.loc = loc;
            m.buf.swap(buf);
            manager->m_incoming.push(m);
            memmove(&inprogress.front(), &inprogress.front() + end, inprogress.size() - end);
        }
    }
    catch (po6::error& e)
    {
        if (e != EAGAIN && e != EINTR && e != EWOULDBLOCK)
        {
            LOG(ERROR) << "could not read from " << loc << "; closing";
            mtx.unlock();
            manager->remove_connection(this);
            return;
        }
    }

    mtx.unlock();
}

void
hyperdex :: physical :: channel :: write_cb(ev::io&)
{
    if (!mtx.trylock())
    {
        return;
    }

    if (outgoing.size() == 0 && outprogress.size() == 0)
    {
        io.set(ev::READ);
    }

    // Pop one message to flush to network.
    if (outprogress.size() == 0)
    {
        uint32_t message_size = htonl(outgoing.front().size());
        outprogress.resize(sizeof(uint32_t) + outgoing.front().size());
        memmove(&outprogress.front(), &message_size, sizeof(uint32_t));
        memmove(&outprogress.front() + sizeof(uint32_t),
                &outgoing.front().front(), outgoing.front().size());
        outgoing.pop();
    }

    try
    {
        size_t ret = soc.write(&outprogress.front(), outprogress.size());
        memmove(&outprogress.front(), &outprogress.front() + ret, outprogress.size() - ret);
        outprogress.resize(outprogress.size() - ret);
    }
    catch (po6::error& e)
    {
        if (e != EAGAIN && e != EINTR && e != EWOULDBLOCK)
        {
            LOG(ERROR) << "could not write to " << loc << "; closing";
            mtx.unlock();
            manager->remove_connection(this);
            return;
        }
    }

    mtx.unlock();
}

hyperdex :: physical :: message :: message()
    : loc()
    , buf()
{
}

hyperdex :: physical :: message :: ~message()
{
}
