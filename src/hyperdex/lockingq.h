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

#ifndef hyperdex_lockingq_h_
#define hyperdex_lockingq_h_

// C includes
#include <cassert>
#include <cstring>

// STL includes
#include <queue>
#include <stdexcept>

// po6
#include <po6/threads/cond.h>

namespace hyperdex
{

template<typename T>
class lockingq
{
    public:
        lockingq();
        ~lockingq() throw ();

    public:
        void shutdown();
        bool is_shutdown();
        size_t size();
        bool push(const T& t);
        bool pop(T* t);

    private:
        std::queue<T> m_queue;
        po6::threads::mutex m_lock;
        po6::threads::cond m_cond;
        bool m_shutdown;
};

template<typename T>
lockingq<T> :: lockingq()
    : m_queue()
    , m_lock()
    , m_cond(&m_lock)
    , m_shutdown(false)
{
}

template<typename T>
lockingq<T> :: ~lockingq()
               throw ()
{
}

template<typename T>
void
lockingq<T> :: shutdown()
{
    m_lock.lock();
    m_shutdown = true;
    m_cond.broadcast();
    m_lock.unlock();
}

template<typename T>
bool
lockingq<T> :: is_shutdown()
{
    bool ret;
    m_lock.lock();
    ret = m_shutdown;
    m_lock.unlock();
    return ret;
}

template<typename T>
size_t
lockingq<T> :: size()
{
    size_t ret;
    m_lock.lock();
    ret = m_queue.size();
    m_lock.unlock();
    return ret;
}

template<typename T>
bool
lockingq<T> :: push(const T& t)
{
    m_lock.lock();

    if (m_shutdown)
    {
        m_lock.unlock();
        return false;
    }

    m_queue.push(t);
    m_cond.signal();
    m_lock.unlock();
    return true;
}

template<typename T>
bool
lockingq<T> :: pop(T* t)
{
    m_lock.lock();

    while (m_queue.empty() && !m_shutdown)
    {
        m_cond.wait();
    }

    if (m_queue.empty() && m_shutdown)
    {
        m_lock.unlock();
        return false;
    }

    *t = m_queue.front();
    m_queue.pop();
    m_lock.unlock();
    return true;
}

} // namespace hyperdex

#endif // hyperdex_lockingq_h_
