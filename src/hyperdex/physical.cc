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

// e
#include <e/guard.h>

// HyperDex
#include <hyperdex/physical.h>

// XXX This code is likely to break if clients or other nodes suddenly
// disconnect.  This can be fixed by making sure that only the evloop thread
// touches channel::io.

hyperdex :: physical :: physical(ev::loop_ref lr, const po6::net::ipaddr& ip, bool listen)
    : m_lr(lr)
    , m_wakeup(lr)
    , m_listen(ip.family(), SOCK_STREAM, IPPROTO_TCP)
    , m_listen_event(lr)
    , m_bindto(ip, 0)
    , m_incoming()
    , m_channels(10)
    , m_location_set_lock()
    , m_location_set()
{
    // Enable other threads to wake us from the event loop.
    m_wakeup.set<physical, &physical::refresh>(this);
    m_wakeup.start();

    if (listen)
    {
        // Enable other hosts to connect to us.
        m_listen.bind(po6::net::location(ip, 0));
        m_listen.listen(4);
        m_listen_event.set<physical, &physical::accept_connection>(this);
        m_listen_event.set(m_listen.get(), ev::READ);
        m_listen_event.start();
        e::intrusive_ptr<channel> chan;
        chan = get_channel(m_listen.getsockname());
        m_bindto = chan->soc.getsockname();
    }
    else
    {
        m_listen.close();
    }
}

hyperdex :: physical :: ~physical()
{
}

void
hyperdex :: physical :: send(const po6::net::location& to,
                             e::buffer* msg)
{
    e::intrusive_ptr<channel> chan = get_channel(to);

    if (!chan)
    {
        return;
    }

    if (chan->mtx.trylock())
    {
        e::guard g = e::makeobjguard(chan->mtx, &po6::threads::mutex::unlock);

        if (chan->outprogress.empty())
        {
            chan->outprogress.pack() << *msg;
        }
        else
        {
            e::buffer buf;
            buf.pack() << *msg;
            chan->outgoing.push(buf);
        }

        chan->write_step();
        m_wakeup.send();
        return;
    }
    else
    {
        e::buffer buf;
        buf.pack() << *msg;
        chan->outgoing.push(buf);
    }

    m_wakeup.send();
}

bool
hyperdex :: physical :: recv(po6::net::location* from,
                             e::buffer* msg)
{
    message m;
    bool ret = m_incoming.pop(&m);
    *from = m.loc;
    msg->swap(m.buf);
    return ret;
}

void
hyperdex :: physical :: deliver(const po6::net::location& from,
                                const e::buffer& msg)
{
    message m;
    m.loc = from;
    m.buf = msg;
    m_incoming.push(m);
}

void
hyperdex :: physical :: shutdown()
{
    m_incoming.shutdown();
}

e::intrusive_ptr<hyperdex::physical::channel>
hyperdex :: physical :: get_channel(const po6::net::location& to)
{
    e::intrusive_ptr<channel> chan;

    if (m_channels.lookup(to, &chan))
    {
        return chan;
    }
    else
    {
        try
        {
            po6::net::socket soc(to.address.family(), SOCK_STREAM, IPPROTO_TCP);
            soc.reuseaddr(true);
            soc.bind(m_bindto);
            soc.connect(to);
            return get_channel(&soc);
        }
        catch (po6::error& e)
        {
            LOG(WARNING) << "Cannot connect to " << to << ":  " << e.what();
        }
        catch (std::bad_alloc& ba)
        {
            LOG(ERROR) << "Cannot allocate memory in get_channel.";
        }
    }

    return NULL;
}

e::intrusive_ptr<hyperdex::physical::channel>
hyperdex :: physical :: get_channel(po6::net::socket* soc)
{
    soc->nonblocking();
    soc->tcp_nodelay(true);
    e::intrusive_ptr<channel> chan = new channel(m_lr, soc, this);

    if (m_channels.insert(chan->loc, chan))
    {
        po6::threads::mutex::hold hold(&m_location_set_lock);
        m_location_set.insert(chan->loc);
        return chan;
    }

    return NULL;
}

void
hyperdex :: physical :: drop_channel(e::intrusive_ptr<channel> chan)
{
    if (m_channels.remove(chan->loc))
    {
        chan->shutdown();
        po6::threads::mutex::hold hold(&m_location_set_lock);
        m_location_set.erase(chan->loc);
    }
}

void
hyperdex :: physical :: refresh(ev::async&, int)
{
    std::set<po6::net::location> locs;

    {
        po6::threads::mutex::hold hold(&m_location_set_lock);
        locs = m_location_set;
    }

    for (std::set<po6::net::location>::iterator i = locs.begin(); i != locs.end(); ++i)
    {
        e::intrusive_ptr<channel> chan;

        if (m_channels.lookup(*i, &chan))
        {
            po6::threads::mutex::hold hold(&chan->mtx);
            int flags = ev::READ;

            if (chan->outprogress.empty())
            {
                if (chan->outgoing.pop(&chan->outprogress))
                {
                    flags |= ev::WRITE;
                }
            }
            else
            {
                flags |= ev::WRITE;
            }

            chan->io.set(flags);
        }
    }
}

void
hyperdex :: physical :: accept_connection(ev::io&, int)
{
    try
    {
        po6::net::socket soc;

        try
        {
            m_listen.accept(&soc);
        }
        catch (po6::error& e)
        {
            LOG(INFO) << "Error accepting connection:  " << e.what();
            return;
        }

        e::intrusive_ptr<channel> chan = get_channel(&soc);
    }
    catch (std::bad_alloc& ba)
    {
        LOG(ERROR) << "Memory allocation failed in accept_connection.";
    }
    catch (std::exception& e)
    {
        LOG(ERROR) << "Uncaught exception in accept_connection:  " << e.what();
    }
}

hyperdex :: physical :: channel :: channel(ev::loop_ref lr,
                                           po6::net::socket* conn,
                                           physical* m)
    : mtx()
    , soc()
    , loc(conn->getpeername())
    , outgoing()
    , outprogress()
    , inprogress()
    , io(lr)
    , manager(m)
    , m_ref(0)
    , m_shutdown(false)
{
    soc.swap(conn);
    io.set(soc.get(), ev::READ);
    io.set<channel, &channel::io_cb>(this);
    io.start();
}

hyperdex :: physical :: channel :: ~channel()
{
    shutdown();
}

#define IO_BLOCKSIZE 65536

void
hyperdex :: physical :: channel :: io_cb(ev::io& w, int revents)
{
    try
    {
        if (revents & ev::READ)
        {
            read_cb(w);
        }

        if (revents & ev::WRITE)
        {
            write_cb(w);
        }
    }
    catch (std::bad_alloc& ba)
    {
        LOG(ERROR) << "Memory allocation failed in io_cb.";
    }
    catch (std::exception& e)
    {
        LOG(ERROR) << "Uncaught exception in io_cb:  " << e.what();
    }

    manager->m_wakeup.send();
}

void
hyperdex :: physical :: channel :: read_cb(ev::io&)
{
    if (!mtx.trylock())
    {
        return;
    }

    e::guard g = e::makeobjguard(mtx, &po6::threads::mutex::unlock);

    try
    {
        size_t ret = read(&soc, &inprogress, IO_BLOCKSIZE);

        if (ret == 0)
        {
            e::intrusive_ptr<channel> ourselves(this);
            manager->drop_channel(ourselves);
            return;
        }

        while (inprogress.size() >= sizeof(uint32_t))
        {
            uint32_t message_size;
            inprogress.unpack() >> message_size;

            if (inprogress.size() < message_size + sizeof(uint32_t))
            {
                break;
            }

            message m;
            inprogress.unpack() >> m.buf; // Different unpacker.
            m.loc = loc;
            manager->m_incoming.push(m);
            inprogress.trim_prefix(message_size + sizeof(uint32_t));
        }
    }
    catch (po6::error& e)
    {
        if (e != EAGAIN && e != EINTR && e != EWOULDBLOCK)
        {
            LOG(ERROR) << "could not read from " << loc << "; closing";
            e::intrusive_ptr<channel> ourselves(this);
            manager->drop_channel(ourselves);
            return;
        }
    }
}

void
hyperdex :: physical :: channel :: write_cb(ev::io&)
{
    if (!mtx.trylock())
    {
        return;
    }

    e::guard g = e::makeobjguard(mtx, &po6::threads::mutex::unlock);
    write_step();
}

void
hyperdex :: physical :: channel :: write_step()
{
    if (outprogress.empty())
    {
        if (!outgoing.pop(&outprogress))
        {
            io.set(ev::READ);
            io.start();
            return;
        }
    }

    try
    {
        size_t ret = soc.write(outprogress.get(), outprogress.size());
        outprogress.trim_prefix(ret);
    }
    catch (po6::error& e)
    {
        if (e != EAGAIN && e != EINTR && e != EWOULDBLOCK)
        {
            LOG(ERROR) << "could not write to " << loc << "; closing";
            e::intrusive_ptr<channel> ourselves(this);
            manager->drop_channel(ourselves);
            return;
        }
    }
}

void
hyperdex :: physical :: channel :: shutdown()
{
    if (!m_shutdown)
    {
        try
        {
            soc.shutdown(SHUT_RDWR);
        }
        catch (po6::error& e)
        {
        }

        io.stop();
        io.set<channel, &channel::io_cb>(NULL);
    }
}
