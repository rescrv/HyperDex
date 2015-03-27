// Copyright (c) 2013, Cornell University
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

#ifndef hyperdex_daemon_state_hash_table_h_
#define hyperdex_daemon_state_hash_table_h_

// po6
#include <po6/threads/mutex.h>

// e
#include <e/nwf_hash_map.h>

// HyperDex
#include "namespace.h"

// This provides a hash table to map from a key to a piece of state.  It
// differs from an ordinary hash table in that the piece of state is assumed to
// be a state machine that multiple threads may concurrently manipulate (or not,
// your choice, but this module won't enforce synchronization on the object).
// The same instance will be returned until the state machine's "finished"
// method returns true *and* no more threads have acquired references to the
// state.  Note that this means your state machine must "do the right thing"
// when its methods are called after it returns "true" for finished (starting
// with returning "false" for subsequent calls to "finished").
//
// A newly constructed T(K) must return "true" for finished();

BEGIN_HYPERDEX_NAMESPACE

template <typename T>
uint64_t
default_state_hash(const T& t)
{
    e::compat::hash<T> h;
    return h(t);
}

template <typename K, typename T, uint64_t (*H)(const K& k) = default_state_hash>
class state_hash_table
{
    public:
        class state_reference;
        class iterator;

    public:
        state_hash_table(e::garbage_collector* gc);
        ~state_hash_table() throw ();

    public:
        T* create_state(const K& key, state_reference* sr);
        T* get_state(const K& key, state_reference* sr);
        T* get_or_create_state(const K& key, state_reference* sr);

    private:
        class state;
        typedef typename e::nwf_hash_map<K, e::intrusive_ptr<state>, H> state_map_t;
        state_map_t m_table;
};

template <typename K, typename T, uint64_t (*H)(const K& k)>
class state_hash_table<K, T, H>::state_reference
{
    public:
        state_reference();
        ~state_reference() throw ();

    public:
        void release();
        T* get();

    private:
        friend class state_hash_table;
        void acquire(state_hash_table* sht, e::intrusive_ptr<state> s);
        void unlock(); // still acquired, but state unlocked

    private:
        state_hash_table* m_sht;
        e::intrusive_ptr<state> m_state;
        bool m_locked;

    private:
        state_reference(state_reference&);
        state_reference& operator = (state_reference&);
};

template <typename K, typename T, uint64_t (*H)(const K& k)>
class state_hash_table<K, T, H>::iterator
{
    public:
        iterator(state_hash_table* sht);
        ~iterator() throw ();

    public:
        bool valid();
        T* operator * ();
        T* operator -> ();
        iterator& operator ++ ();

    private:
        void prime();

    private:
        state_hash_table* m_sht;
        typename state_map_t::iterator m_iter;
        state_reference m_sr;
        bool m_primed;

    private:
        iterator(iterator&);
        iterator& operator = (iterator&);
};

template <typename K, typename T, uint64_t (*H)(const K& k)>
class state_hash_table<K, T, H>::state
{
    public:
        state(const K& k);
        ~state() throw ();

    public:
        T t;
        po6::threads::mutex mtx;
        uint64_t acquires;
        bool garbage;

    public:
        size_t ref;
        void inc() { __sync_add_and_fetch(&ref, 1); }
        void dec() { if (__sync_sub_and_fetch(&ref, 1) == 0) delete this; }
};

template <typename K, typename T, uint64_t (*H)(const K& k)>
state_hash_table<K, T, H> :: state_hash_table(e::garbage_collector* gc)
    : m_table(gc)
{
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
state_hash_table<K, T, H> :: ~state_hash_table() throw ()
{
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
T*
state_hash_table<K, T, H> :: create_state(const K& key, state_reference* sr)
{
    sr->release();

    e::intrusive_ptr<state> s = new state(key);
    sr->acquire(this, s);

    if (m_table.put_ine(key, s))
    {
        sr->unlock();
        return sr->get();
    }

    sr->release();
    return NULL;
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
T*
state_hash_table<K, T, H> :: get_state(const K& key, state_reference* sr)
{
    sr->release();

    while (true)
    {
        e::intrusive_ptr<state> s;

        if (!m_table.get(key, &s))
        {
            return NULL;
        }

        sr->acquire(this, s);

        if (s->garbage)
        {
            sr->release();
            continue;
        }

        sr->unlock();
        return sr->get();
    }
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
T*
state_hash_table<K, T, H> :: get_or_create_state(const K& key, state_reference* sr)
{
    sr->release();

    while (true)
    {
        e::intrusive_ptr<state> s;

        if (m_table.get(key, &s))
        {
            sr->acquire(this, s);
        }
        else
        {
            assert(!s);
            s = new state(key);
            sr->acquire(this, s);

            if (!m_table.put_ine(key, s))
            {
                sr->release();
                continue;
            }
        }

        assert(s);

        if (s->garbage)
        {
            sr->release();
            continue;
        }

        sr->unlock();
        return sr->get();
    }
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
state_hash_table<K, T, H> :: state_reference :: state_reference()
    : m_sht(NULL)
    , m_state()
    , m_locked(false)
{
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
state_hash_table<K, T, H> :: state_reference :: ~state_reference() throw ()
{
    release();
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
void
state_hash_table<K, T, H> :: state_reference :: release()
{
    if (!m_sht || !m_state)
    {
        assert(!m_sht);
        assert(!m_state);
        assert(!m_locked);
        return;
    }

    assert(m_sht);
    assert(m_state);

    if (!m_locked)
    {
        m_state->mtx.lock();
        m_locked = true;
    }

    assert(m_state->acquires > 0);
    --m_state->acquires;

    if (m_state->acquires == 0 &&
        !m_state->garbage &&
        m_state->t.finished())
    {
        m_state->garbage = true;
        m_sht->m_table.del_if(m_state->t.state_key(), m_state);
    }

    m_state->mtx.unlock();
    m_sht = NULL;
    m_state = NULL;
    m_locked = false;
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
T*
state_hash_table<K, T, H> :: state_reference :: get()
{
    return &m_state->t;
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
void
state_hash_table<K, T, H> :: state_reference :: acquire(state_hash_table* sht, e::intrusive_ptr<state> s)
{
    if (m_sht || m_state || m_locked)
    {
        release();
    }

    m_sht = sht;
    m_state = s;
    m_state->mtx.lock();
    m_locked = true;
    ++m_state->acquires;
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
void
state_hash_table<K, T, H> :: state_reference :: unlock()
{
    if (m_locked)
    {
        m_locked = false;
        m_state->mtx.unlock();
    }
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
state_hash_table<K, T, H> :: iterator :: iterator(state_hash_table* sht)
    : m_sht(sht)
    , m_iter(m_sht->m_table.begin())
    , m_sr()
    , m_primed(false)
{
    prime();
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
state_hash_table<K, T, H> :: iterator :: ~iterator() throw ()
{
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
bool
state_hash_table<K, T, H> :: iterator :: valid()
{
    prime();
    return m_iter != m_sht->m_table.end();
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
T*
state_hash_table<K, T, H> :: iterator :: operator * ()
{
    assert(valid());
    return m_sr.get();
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
T*
state_hash_table<K, T, H> :: iterator :: operator -> ()
{
    assert(valid());
    return m_sr.get();
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
typename state_hash_table<K, T, H>::iterator&
state_hash_table<K, T, H> :: iterator :: operator ++ ()
{
    m_primed = false;
    ++m_iter;
    prime();
    return *this;
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
void
state_hash_table<K, T, H> :: iterator :: prime()
{
    if (m_primed)
    {
        return;
    }

    while (true)
    {
        m_sr.release();

        if (m_iter == m_sht->m_table.end() ||
            m_sht->get_state(m_iter->first, &m_sr))
        {
            break;
        }

        ++m_iter;
    }

    m_primed = true;
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
state_hash_table<K, T, H> :: state :: state(const K& k)
    : t(k)
    , mtx()
    , acquires(0)
    , garbage(false)
    , ref(0)
{
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
state_hash_table<K, T, H> :: state :: ~state() throw ()
{
    assert(acquires == 0);
}

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_state_hash_table_h_
