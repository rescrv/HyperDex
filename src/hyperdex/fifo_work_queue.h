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

#ifndef hyperdex_fifo_work_queue_h_
#define hyperdex_fifo_work_queue_h_

// C includes
#include <cassert>
#include <cstring>

// STL includes
#include <queue>
#include <stdexcept>

// po6
#include <po6/threads/cond.h>

// e
#include <e/lockfree_fifo.h>

namespace hyperdex
{

template<typename T>
class fifo_work_queue
{
    public:
        fifo_work_queue();
        ~fifo_work_queue() throw ();

    public:
        void pause();
        void unpause();
        size_t num_paused();
        void shutdown();
        bool is_shutdown();
        bool push(const T& t);
        bool pop(T* t);
        bool optimistically_empty() { return m_queue.optimistically_empty(); }

    private:
        e::lockfree_fifo<T> m_queue;
        po6::threads::mutex m_lock;
        po6::threads::cond m_may_pop;
        bool m_paused;
        size_t m_num_blocked;
        bool m_shutdown;
};

template<typename T>
fifo_work_queue<T> :: fifo_work_queue()
    : m_queue()
    , m_lock()
    , m_may_pop(&m_lock)
    , m_paused(false)
    , m_num_blocked(0)
    , m_shutdown(false)
{
}

template<typename T>
fifo_work_queue<T> :: ~fifo_work_queue()
               throw ()
{
}

template<typename T>
void
fifo_work_queue<T> :: pause()
{
    po6::threads::mutex::hold hold(&m_lock);
    m_paused = true;
    __sync_synchronize();
}

template<typename T>
void
fifo_work_queue<T> :: unpause()
{
    po6::threads::mutex::hold hold(&m_lock);
    m_paused = false;
    m_may_pop.broadcast();
    __sync_synchronize();
}

template<typename T>
size_t
fifo_work_queue<T> :: num_paused()
{
    po6::threads::mutex::hold hold(&m_lock);
    return m_num_blocked;
}

template<typename T>
void
fifo_work_queue<T> :: shutdown()
{
    po6::threads::mutex::hold hold(&m_lock);
    m_shutdown = true;
    m_may_pop.broadcast();
    __sync_synchronize();
}

template<typename T>
bool
fifo_work_queue<T> :: is_shutdown()
{
    po6::threads::mutex::hold hold(&m_lock);
    return m_shutdown;
}

template<typename T>
bool
fifo_work_queue<T> :: push(const T& t)
{
    if (__sync_and_and_fetch(&m_shutdown, true))
    {
        return false;
    }

    m_queue.push(t);

    if (__sync_add_and_fetch(&m_num_blocked, 0) > 0)
    {
        po6::threads::mutex::hold hold(&m_lock);
        m_may_pop.signal();
    }

    return true;
}

template<typename T>
bool
fifo_work_queue<T> :: pop(T* t)
{
    while (true)
    {
        if (__sync_and_and_fetch(&m_paused, true))
        {
            po6::threads::mutex::hold hold(&m_lock);
            __sync_add_and_fetch(&m_num_blocked, 1);
            m_may_pop.wait();
            __sync_sub_and_fetch(&m_num_blocked, 1);
        }

        bool popped = m_queue.pop(t);

        if (popped)
        {
            return true;
        }

        if (__sync_and_and_fetch(&m_shutdown, true))
        {
            return false;
        }

        po6::threads::mutex::hold hold(&m_lock);
        __sync_add_and_fetch(&m_num_blocked, 1);
        popped = m_queue.pop(t);

        if (popped)
        {
            return true;
        }

        m_may_pop.wait();
        __sync_sub_and_fetch(&m_num_blocked, 1);
    }
}

} // namespace hyperdex

#endif // hyperdex_fifo_work_queue_h_
