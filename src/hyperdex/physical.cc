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
#include <e/guard.h>

// HyperDex
#include <hyperdex/physical.h>

hyperdex :: physical :: physical(const po6::net::ipaddr& ip, bool listen)
    : m_max_fds(sysconf(_SC_OPEN_MAX))
    , m_shutdown(false)
    , m_listen(ip.family(), SOCK_STREAM, IPPROTO_TCP)
    , m_bindto(ip, 0)
    , m_incoming()
    , m_locations()
    , m_hazard_ptrs()
    , m_channels(static_cast<size_t>(m_max_fds), NULL)
    , m_paused(false)
    , m_not_paused_lock()
    , m_not_paused(&m_not_paused_lock)
    , m_count_paused(0)
{
    if (listen)
    {
        hazard_ptr hptr = m_hazard_ptrs.get();
        channel* chan;
        // Enable other hosts to connect to us.
        m_listen.bind(po6::net::location(ip, 0));
        m_listen.listen(4);
        m_listen.nonblocking();

        switch (get_channel(hptr, m_listen.getsockname(), &chan))
        {
            case SUCCESS:
                break;
            case SHUTDOWN:
            case QUEUED:
            case CONNECTFAIL:
            case DISCONNECT:
            case LOGICERROR:
            default:
                throw std::runtime_error("Could not create connection to self.");
        }

        m_bindto = chan->soc.getsockname();
    }
    else
    {
        m_listen.close();
    }
}

hyperdex :: physical :: ~physical()
                        throw ()
{
}

void
hyperdex :: physical :: pause()
{
    po6::threads::mutex::hold hold(&m_not_paused_lock);
    m_paused = true;
}

void
hyperdex :: physical :: unpause()
{
    po6::threads::mutex::hold hold(&m_not_paused_lock);
    m_paused = false;
    m_not_paused.broadcast();
}

size_t
hyperdex :: physical :: num_paused()
{
    po6::threads::mutex::hold hold(&m_not_paused_lock);
    return m_count_paused;
}

void
hyperdex :: physical :: shutdown()
{
    m_shutdown = true;
}

hyperdex::physical::result_t
hyperdex :: physical :: send(const po6::net::location& to,
                             e::buffer* msg)
{
    hazard_ptr hptr = m_hazard_ptrs.get();
    channel* chan;

    result_t res = get_channel(hptr, to, &chan);

    if (res != SUCCESS)
    {
        return res;
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

        work_write(chan);
        return SUCCESS;
    }
    else
    {
        e::buffer buf;
        buf.pack() << *msg;
        chan->outgoing.push(buf);
        return QUEUED;
    }
}

hyperdex::physical::result_t
hyperdex :: physical :: recv(po6::net::location* from,
                             e::buffer* msg)
{
    message m;
    bool ret = m_incoming.pop(&m);

    if (ret)
    {
        *from = m.loc;
        msg->swap(m.buf);
        return SUCCESS;
    }

    hazard_ptr hptr = m_hazard_ptrs.get();
    std::vector<pollfd> pfds(m_max_fds);

    for (int i = 0; i < m_max_fds; ++i)
    {
        pfds[i].fd = i;
        pfds[i].events = POLLIN|POLLOUT;
        pfds[i].revents = 0;
    }

    while (true)
    {
        if (m_shutdown)
        {
            return SHUTDOWN;
        }

        for (int i = 0; i < m_max_fds; ++i)
        {
            if (pfds[i].revents & POLLNVAL)
            {
                pfds[i].fd = -1;
                pfds[i].events = 0;
                pfds[i].revents = 0;
            }
        }

        {
            po6::threads::mutex::hold hold(&m_not_paused_lock);

            while (m_paused)
            {
                ++m_count_paused;
                m_not_paused.wait();
                --m_count_paused;
            }
        }

        if (poll(&pfds.front(), m_max_fds, 1000) < 0)
        {
            PLOG(INFO) << "poll failed";
            return LOGICERROR;
        }

        for (int i = 0; i < m_max_fds; ++i)
        {
            channel* chan;

            while (true)
            {
                chan = m_channels[i];
                hptr->set(0, chan);

                if (chan == m_channels[i])
                {
                    break;
                }
            }

            // Protect file descriptors opened elsewhere.
            if (!chan && i != m_listen.get())
            {
                pfds[i].fd = -1;
                continue;
            }

            if (pfds[i].revents & POLLNVAL)
            {
                continue;
            }

            if (pfds[i].revents & POLLERR)
            {
                *from = chan->loc;
                work_close(hptr, chan);
                return DISCONNECT;
            }

            if (pfds[i].revents & POLLHUP)
            {
                *from = chan->loc;
                work_close(hptr, chan);
                return DISCONNECT;
            }

            if (pfds[i].revents & POLLIN)
            {
                if (i == m_listen.get())
                {
                    int fd = work_accept(hptr);

                    if (fd >= 0)
                    {
                        pfds[fd].fd = fd;
                        pfds[fd].events |= POLLIN;
                        pfds[fd].revents = 0;
                    }
                }
                else
                {
                    result_t res;

                    if (work_read(hptr, chan, from, msg, &res))
                    {
                        return res;
                    }
                }
            }

            if (pfds[i].revents & POLLOUT)
            {
                if (!work_write(chan))
                {
                    pfds[i].events &= ~POLLOUT;
                }
            }
        }
    }
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

hyperdex::physical::result_t
hyperdex :: physical :: get_channel(const hazard_ptr& hptr, const po6::net::location& to, channel** chan)
{
    int fd;

    while (true)
    {
        if (m_locations.lookup(to, &fd))
        {
            *chan = m_channels[fd];
            hptr->set(0, *chan);

            if (*chan != m_channels[fd])
            {
                continue;
            }

            if (!*chan)
            {
                continue;
            }

            return SUCCESS;
        }
        else
        {
            try
            {
                po6::net::socket soc(to.address.family(), SOCK_STREAM, IPPROTO_TCP);
                soc.reuseaddr(true);
                soc.bind(m_bindto);
                soc.connect(to);
                return get_channel(hptr, &soc, chan);
            }
            catch (po6::error& e)
            {
                return CONNECTFAIL;
            }
        }
    }
}

hyperdex::physical::result_t
hyperdex :: physical :: get_channel(const hazard_ptr& hptr, po6::net::socket* soc, channel** ret)
{
    soc->nonblocking();
    soc->tcp_nodelay(true);
    std::auto_ptr<channel> chan(new channel(soc));
    hptr->set(0, chan.get());
    *ret = chan.get();

    if (m_locations.insert(chan->loc, chan->soc.get()))
    {
        assert(m_channels[chan->soc.get()] == NULL);
        m_channels[chan->soc.get()] = chan.get();
        chan.release();
        return SUCCESS;
    }

    LOG(INFO) << "Logic error in get_channel.";
    return LOGICERROR;
}

int
hyperdex :: physical :: work_accept(const hazard_ptr& hptr)
{
    try
    {
        po6::net::socket soc;
        m_listen.accept(&soc);
        channel* chan;
        get_channel(hptr, &soc, &chan);
        return chan->soc.get();
    }
    catch (po6::error& e)
    {
        if (e != EAGAIN && e != EINTR && e != EWOULDBLOCK)
        {
            LOG(INFO) << "Error accepting connection:  " << e.what();
        }
    }

    return -1;
}

void
hyperdex :: physical :: work_close(const hazard_ptr& hptr, channel* chan)
{
    if (chan)
    {
        int fd = chan->soc.get();
        m_channels[fd] = NULL;
        m_locations.remove(chan->loc);
        chan->soc.close();
        hptr->retire(chan);
    }
}

#define IO_BLOCKSIZE 65536

bool
hyperdex :: physical :: work_read(const hazard_ptr& hptr, channel* chan, po6::net::location* from, e::buffer* msg, result_t* res)
{
    if (!chan->mtx.trylock())
    {
        return false;
    }

    e::guard g = e::makeobjguard(chan->mtx, &po6::threads::mutex::unlock);

    try
    {
        size_t ret = read(&chan->soc, &chan->inprogress, IO_BLOCKSIZE);

        if (ret == 0)
        {
            *from = chan->loc;
            *res = DISCONNECT;
            work_close(hptr, chan);
            return true;
        }

        std::vector<message> ms;

        while (chan->inprogress.size() >= sizeof(uint32_t))
        {
            uint32_t message_size;
            chan->inprogress.unpack() >> message_size;

            if (chan->inprogress.size() < message_size + sizeof(uint32_t))
            {
                break;
            }

            ms.push_back(message());
            message& m(ms.back());
            chan->inprogress.unpack() >> m.buf; // Different unpacker.
            m.loc = chan->loc;
            chan->inprogress.trim_prefix(message_size + sizeof(uint32_t));
        }

        if (ms.empty())
        {
            return false;
        }

        for (size_t i = 1; i < ms.size(); ++i)
        {
            m_incoming.push(ms[i]);
        }

        *from = ms[0].loc;
        *res = SUCCESS;
        msg->swap(ms[0].buf);
        return true;
    }
    catch (po6::error& e)
    {
        if (e != EAGAIN && e != EINTR && e != EWOULDBLOCK)
        {
            LOG(ERROR) << "could not read from " << chan->loc << "; closing";
            *from = chan->loc;
            *res = DISCONNECT;
            work_close(hptr, chan);
            return true;
        }
    }

    return false;
}

bool
hyperdex :: physical :: work_write(channel* chan)
{
    if (chan->outprogress.empty())
    {
        if (!chan->outgoing.pop(&chan->outprogress))
        {
            return false;
        }
    }

    try
    {
        size_t ret = chan->soc.write(chan->outprogress.get(), chan->outprogress.size());
        chan->outprogress.trim_prefix(ret);
    }
    catch (po6::error& e)
    {
        if (e != EAGAIN && e != EINTR && e != EWOULDBLOCK)
        {
            LOG(ERROR) << "could not write to " << chan->loc << "; closing";
            return false;
        }
    }

    return true;
}

hyperdex :: physical :: channel :: channel(po6::net::socket* conn)
    : mtx()
    , soc()
    , loc(conn->getpeername())
    , outgoing()
    , outprogress()
    , inprogress()
{
    soc.swap(conn);
}

hyperdex :: physical :: channel :: ~channel()
                                   throw ()
{
}
