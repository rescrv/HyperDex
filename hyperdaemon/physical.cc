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
#include <e/bufferio.h>
#include <e/guard.h>

// HyperDaemon
#include "hyperdaemon/physical.h"

using e::bufferio::read;

hyperdaemon :: physical :: physical(const po6::net::ipaddr& ip,
                                    in_port_t incoming,
                                    in_port_t outgoing,
                                    bool listen,
                                    size_t num_threads)
    : m_max_fds(sysconf(_SC_OPEN_MAX))
    , m_shutdown(false)
    , m_listen(ip.family(), SOCK_STREAM, IPPROTO_TCP)
    , m_bindto(ip, outgoing)
    , m_incoming()
    , m_locations()
    , m_hazard_ptrs()
    , m_channels(static_cast<size_t>(m_max_fds), NULL)
    , m_channels_mask(m_max_fds, 0)
    , m_pause_barrier(num_threads)
{
    if (listen)
    {
        hazard_ptr hptr = m_hazard_ptrs.get();
        channel* chan;
        // Enable other hosts to connect to us.
        m_listen.set_reuseaddr();
        m_listen.bind(po6::net::location(ip, incoming));
        m_listen.listen(sysconf(_SC_OPEN_MAX));
        m_listen.set_nonblocking();

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

hyperdaemon :: physical :: ~physical()
                        throw ()
{
}

void
hyperdaemon :: physical :: pause()
{
    m_pause_barrier.pause();
}

void
hyperdaemon :: physical :: unpause()
{
    m_pause_barrier.unpause();
}

void
hyperdaemon :: physical :: shutdown()
{
    m_shutdown = true;
    m_pause_barrier.shutdown();
}

hyperdaemon::physical::returncode
hyperdaemon :: physical :: send(const po6::net::location& to,
                                std::auto_ptr<e::buffer> msg)
{
    assert(msg->capacity() >= sizeof(uint32_t));
    hazard_ptr hptr = m_hazard_ptrs.get();
    channel* chan;

    returncode res = get_channel(hptr, to, &chan);

    if (res != SUCCESS)
    {
        return res;
    }

    *msg << static_cast<uint32_t>(msg->size());

    if (chan->mtx.trylock())
    {
        e::guard g = e::makeobjguard(chan->mtx, &po6::threads::mutex::unlock);

        if (chan->outprogress.empty())
        {
            chan->outnow = msg;
            chan->outprogress = chan->outnow->as_slice();
        }
        else
        {
            chan->outgoing.push(msg);
        }

        work_write(chan);
        return SUCCESS;
    }
    else
    {
        chan->outgoing.push(msg);
        return QUEUED;
    }
}

hyperdaemon::physical::returncode
hyperdaemon :: physical :: recv(po6::net::location* from,
                                std::auto_ptr<e::buffer>* msg)
{
    message m;
    bool ret = m_incoming.pop(&m);

    if (ret)
    {
        *from = m.loc;
        *msg = m.buf;
        return SUCCESS;
    }

    hazard_ptr hptr = m_hazard_ptrs.get();
    std::vector<pollfd> pfds;

    for (int i = 0; i < m_max_fds; ++i)
    {
        if (m_channels_mask[i])
        {
            pfds.push_back(pollfd());
            pfds.back().fd = i;
            pfds.back().events = POLLIN|POLLOUT;
            pfds.back().revents = 0;
        }
    }

    pfds.push_back(pollfd());
    pfds.back().fd = m_listen.get();
    pfds.back().events = POLLIN;
    pfds.back().revents = 0;

    while (true)
    {
        m_pause_barrier.pausepoint();

        if (m_shutdown)
        {
            return SHUTDOWN;
        }

        if (poll(&pfds.front(), pfds.size(), 100) < 0)
        {
            if (errno != EAGAIN && errno != EINTR && errno != EWOULDBLOCK)
            {
                PLOG(INFO) << "poll failed";
                return LOGICERROR;
            }
        }

        size_t starting = hptr->state();

        for (size_t i = 0; i < pfds.size(); ++i)
        {
            size_t pfd_idx = hptr->state() = (starting + i) % pfds.size();

            if (pfds[pfd_idx].fd == -1)
            {
                continue;
            }

            channel* chan;

            while (true)
            {
                chan = m_channels[pfds[pfd_idx].fd];
                hptr->set(0, chan);

                if (chan == m_channels[pfds[pfd_idx].fd])
                {
                    break;
                }
            }

            // Protect file descriptors opened elsewhere.
            if (!chan && pfds[pfd_idx].fd != m_listen.get())
            {
                pfds[pfd_idx].fd = -1;
                continue;
            }

            if (pfds[pfd_idx].revents & POLLNVAL)
            {
                pfds[pfd_idx].fd = -1;
                continue;
            }

            if (pfds[pfd_idx].revents & POLLERR)
            {
                *from = chan->loc;
                work_close(hptr, chan);
                return DISCONNECT;
            }

            if (pfds[pfd_idx].revents & POLLHUP)
            {
                *from = chan->loc;
                work_close(hptr, chan);
                return DISCONNECT;
            }

            if (pfds[pfd_idx].revents & POLLIN)
            {
                if (pfds[pfd_idx].fd == m_listen.get())
                {
                    int fd = work_accept(hptr);

                    if (fd >= 0)
                    {
                        pfds.push_back(pollfd());
                        pfds.back().fd = fd;
                        pfds.back().events |= POLLIN;
                        pfds.back().revents = 0;
                    }
                }
                else
                {
                    returncode res;

                    if (work_read(hptr, chan, from, msg, &res))
                    {
                        return res;
                    }
                }
            }

            if (pfds[pfd_idx].revents & POLLOUT)
            {
                po6::threads::mutex::hold hold(&chan->mtx);

                if (!work_write(chan))
                {
                    pfds[pfd_idx].events &= ~POLLOUT;
                }
            }
        }
    }
}

void
hyperdaemon :: physical :: deliver(const po6::net::location& from,
                                   std::auto_ptr<e::buffer> msg)
{
    message m;
    m.loc = from;
    m.buf = msg;
    m_incoming.push(m);
}

hyperdaemon::physical::returncode
hyperdaemon :: physical :: get_channel(const hazard_ptr& hptr,
                                       const po6::net::location& to,
                                       channel** chan)
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
                soc.set_reuseaddr();
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

hyperdaemon::physical::returncode
hyperdaemon :: physical :: get_channel(const hazard_ptr& hptr,
                                       po6::net::socket* soc,
                                       channel** ret)
{
    soc->set_nonblocking();
    soc->set_tcp_nodelay();
    std::auto_ptr<channel> chan(new channel(soc));
    hptr->set(0, chan.get());
    *ret = chan.get();

    if (m_locations.insert(chan->loc, chan->soc.get()))
    {
        assert(m_channels[chan->soc.get()] == NULL);
        __sync_or_and_fetch(&m_channels_mask[chan->soc.get()], 1);
        m_channels[chan->soc.get()] = chan.get();
        chan.release();
        return SUCCESS;
    }

    LOG(INFO) << "Logic error in get_channel.";
    return LOGICERROR;
}

int
hyperdaemon :: physical :: work_accept(const hazard_ptr& hptr)
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
hyperdaemon :: physical :: work_close(const hazard_ptr& hptr, channel* chan)
{
    if (chan)
    {
        {
            po6::threads::mutex::hold hold(&chan->mtx);
            int fd = chan->soc.get();

            if (fd < 0)
            {
                return;
            }

            m_channels_mask[chan->soc.get()] = 0;
            m_channels[fd] = NULL;
            m_locations.remove(chan->loc);
            chan->soc.close();
        }

        hptr->retire(chan);
    }
}

#define IO_BLOCKSIZE 4096

bool
hyperdaemon :: physical :: work_read(const hazard_ptr& hptr,
                                     channel* chan,
                                     po6::net::location* from,
                                     std::auto_ptr<e::buffer>* msg,
                                     returncode* res)
{
    if (!chan->mtx.trylock())
    {
        return false;
    }

    e::guard g = e::makeobjguard(chan->mtx, &po6::threads::mutex::unlock);

    if (chan->soc.get() < 0)
    {
        return false;
    }

    char buffer[IO_BLOCKSIZE];
    ssize_t rem;

    // Restore leftovers from last time.
    if (chan->inoffset)
    {
        memmove(buffer, chan->inbuffer, chan->inoffset);
    }

    // Read into our temporary local buffer.
    rem = chan->soc.read(buffer + chan->inoffset, IO_BLOCKSIZE - chan->inoffset);

    // If it's an error case, we should short-circuit.
    if ((rem < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
            || rem == 0)
    {
        LOG(ERROR) << "could not read from " << chan->loc << "; closing";
        *from = chan->loc;
        *res = DISCONNECT;
        chan->mtx.unlock();
        g.dismiss();
        work_close(hptr, chan);
        return true;
    }
    else if (rem < 0)
    {
        return false;
    }

    // We know rem is >= 0, so add the amount of preexisting data.
    rem += chan->inoffset;
    chan->inoffset = 0;
    char* data = buffer;
    bool ret = false;

    // XXX If this fails to allocate memory at any time, we need to just close
    // the channel.
    while (rem > 0)
    {
        if (!chan->inprogress.get())
        {
            if (rem < static_cast<ssize_t>(sizeof(uint32_t)))
            {
                memmove(chan->inbuffer, data, rem);
                chan->inoffset = rem;
                rem = 0;
            }
            else
            {
                uint32_t sz;
                memmove(&sz, data, sizeof(uint32_t));
                sz = be32toh(sz);
                // XXX sanity check sz to prevent memory exhaustion.
                chan->inprogress.reset(e::buffer::create(sz));
                memmove(chan->inprogress->data(), data, sizeof(uint32_t));
                chan->inprogress->resize(sizeof(uint32_t));
                rem -= sizeof(uint32_t);
                data += sizeof(uint32_t);
            }
        }
        else
        {
            uint32_t sz = chan->inprogress->capacity() - chan->inprogress->size();
            sz = std::min(static_cast<uint32_t>(rem), sz);
            rem -= sz;
            memmove(chan->inprogress->data() + chan->inprogress->size(),
                    data, sz);
            chan->inprogress->resize(chan->inprogress->size() + sz);

            if (chan->inprogress->size() == chan->inprogress->capacity())
            {
                if (ret)
                {
                    message m;
                    m.loc = chan->loc;
                    m.buf = chan->inprogress;
                    m_incoming.push(m);
                }
                else
                {
                    *from = chan->loc;
                    *msg = chan->inprogress;
                    *res = SUCCESS;
                    ret = true;
                }
            }
        }
    }

    return ret;
}

bool
hyperdaemon :: physical :: work_write(channel* chan)
{
    if (chan->soc.get() < 0)
    {
        return false;
    }

    if (chan->outprogress.empty())
    {
        if (!chan->outgoing.pop(&chan->outnow))
        {
            return false;
        }

        chan->outprogress = chan->outnow->as_slice();
    }

    ssize_t ret = chan->soc.write(chan->outprogress.data(), chan->outprogress.size());

    if (ret < 0 && errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK)
    {
        PLOG(ERROR) << "could not write to " << chan->loc << "(fd:"  << chan->soc.get() << ")";
        return false;
    }

    chan->outprogress.advance(ret);
    return true;
}

hyperdaemon :: physical :: channel :: channel(po6::net::socket* conn)
    : mtx()
    , soc()
    , loc(conn->getpeername())
    , outgoing()
    , outnow()
    , outprogress()
    , inprogress()
    , inoffset(0)
    , inbuffer()
{
    soc.swap(conn);
}

hyperdaemon :: physical :: channel :: ~channel()
                                   throw ()
{
}
