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

// STL
#include <list>

// po6
#include <po6/threads/mutex.h>

// e
#include <e/nwf_hash_map.h>

// HyperDex
#include "namespace.h"

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
        typedef typename e::nwf_hash_map<K, e::intrusive_ptr<T>, H> state_map_t;
        state_map_t m_table;
};

template <typename K, typename T, uint64_t (*H)(const K& k)>
class state_hash_table<K, T, H>::state_reference
{
    public:
        state_reference();
        ~state_reference() throw ();

    public:
        void lock(state_hash_table* sht, e::intrusive_ptr<T> state);
        void unlock();
        bool locked();

    private:
        bool m_locked;
        state_hash_table* m_sht;
        e::intrusive_ptr<T> m_state;

    private:
        state_reference(state_reference&);
        state_reference& operator = (state_reference&);
};

template <typename K, typename T, uint64_t (*H)(const K& k)>
class state_hash_table<K, T, H>::iterator
{
    public:
        iterator(state_hash_table* sht);

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
    while (true)
    {
        e::intrusive_ptr<T> t = new T(key);
        sr->lock(this, t);

        if (m_table.put_ine(key, t))
        {
            return t.get();
        }

        sr->unlock();
        return NULL;
    }
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
T*
state_hash_table<K, T, H> :: get_state(const K& key, state_reference* sr)
{
    while (true)
    {
        e::intrusive_ptr<T> t;

        if (!m_table.get(key, &t))
        {
            return NULL;
        }

        sr->lock(this, t);

        if (t->marked_garbage())
        {
            sr->unlock();
            continue;
        }

        return t.get();
    }
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
T*
state_hash_table<K, T, H> :: get_or_create_state(const K& key, state_reference* sr)
{
    while (true)
    {
        e::intrusive_ptr<T> t;

        if (m_table.get(key, &t))
        {
            sr->lock(this, t);
        }
        else
        {
            assert(!t);
            t = new T(key);
            sr->lock(this, t);

            if (!m_table.put_ine(key, t))
            {
                sr->unlock();
                continue;
            }
        }

        assert(t);
        assert(sr->locked());

        if (t->marked_garbage())
        {
            sr->unlock();
            continue;
        }

        return t.get();
    }
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
state_hash_table<K, T, H> :: state_reference :: state_reference()
    : m_locked(false)
    , m_sht(NULL)
    , m_state()
{
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
state_hash_table<K, T, H> :: state_reference :: ~state_reference() throw ()
{
    if (m_locked)
    {
        unlock();
    }
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
void
state_hash_table<K, T, H> :: state_reference :: lock(state_hash_table* sht,
                                                     e::intrusive_ptr<T> state)
{
    assert(!m_locked);
    m_locked = true;
    m_sht = sht;
    m_state = state;
    m_state->lock();
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
void
state_hash_table<K, T, H> :: state_reference :: unlock()
{
    assert(m_locked);
    // so we need to prevent a deadlock with cycle
    // state_hash_table::m_mtx -> m_state->lock ->
    //
    // To do this, we mark garbage on key_state, release the lock, grab
    // replication_manager::m_key_states_lock and destroy the object.
    // Everyone else will spin without holding a lock on the key state.
    bool we_collect = m_state->finished() && !m_state->marked_garbage();

    if (we_collect)
    {
        m_state->mark_garbage();
        m_sht->m_table.del_if(m_state->state_key(), m_state);
    }

    m_state->unlock();
    m_sht = NULL;
    m_state = NULL;
    m_locked = false;
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
bool
state_hash_table<K, T, H> :: state_reference :: locked()
{
    return m_locked;
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
    assert(m_sr.locked());
    return m_iter->second.get();
}

template <typename K, typename T, uint64_t (*H)(const K& k)>
T*
state_hash_table<K, T, H> :: iterator :: operator -> ()
{
    assert(valid());
    assert(m_sr.locked());
    return m_iter->second.get();
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
        if (m_sr.locked())
        {
            m_sr.unlock();
        }

        if (m_iter == m_sht->m_table.end() ||
            m_sht->get_state(m_iter->first, &m_sr))
        {
            break;
        }

        ++m_iter;
    }

    m_primed = true;
}

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_state_hash_table_h_
