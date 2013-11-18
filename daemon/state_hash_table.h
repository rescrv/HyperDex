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

// Google
#include <google/dense_hash_map>

// po6
#include <po6/threads/mutex.h>

// HyperDex
#include "namespace.h"

BEGIN_HYPERDEX_NAMESPACE

// Only one iterator may be used at a time

template <typename K, typename T>
class state_hash_table
{
    public:
        class state_reference;
        class iterator;

    public:
        state_hash_table();
        ~state_hash_table() throw ();

    public:
        void set_empty_key(const K& k)
        { m_state_map->set_empty_key(k); }
        void set_deleted_key(const K& k)
        { m_state_map->set_deleted_key(k); }

    public:
        T* create_state(const K& key, state_reference* sr);
        T* get_state(const K& key, state_reference* sr);
        T* get_or_create_state(const K& key, state_reference* sr);

    private:
        typedef std::list<e::intrusive_ptr<T> > state_list_t;
        typedef google::dense_hash_map<K, typename state_list_t::iterator> state_map_t;

    private:
        po6::threads::mutex m_mtx;
        const std::auto_ptr<state_map_t> m_state_map;
        const std::auto_ptr<state_list_t> m_state_list;
        typename state_list_t::iterator m_itl;
        bool m_itl_erased;
        bool m_iterating;
};

template <typename K, typename T>
class state_hash_table<K, T>::state_reference
{
    public:
        state_reference();
        ~state_reference() throw ();

    public:
        void lock(state_hash_table* sht, e::intrusive_ptr<T> state);
        void unlock();

    private:
        state_reference(state_reference&);
        state_reference& operator = (state_reference&);

    private:
        bool m_locked;
        state_hash_table* m_sht;
        e::intrusive_ptr<T> m_state;
};

template <typename K, typename T>
class state_hash_table<K, T>::iterator
{
    public:
        iterator(state_hash_table* sht);
        ~iterator() throw ();

    public:
        T* get();
        bool valid();
        void next();

    private:
        void prime(bool inc);

    private:
        state_hash_table* m_sht;
        state_reference m_sr;
        T* m_ptr;
        bool m_primed;
        bool m_valid;

    private:
        iterator(iterator&);
        iterator& operator = (iterator&);
};

template <typename K, typename T>
state_hash_table<K, T> :: state_hash_table()
    : m_mtx()
    , m_state_map(new state_map_t())
    , m_state_list(new state_list_t())
    , m_itl(m_state_list->begin())
    , m_itl_erased(false)
    , m_iterating(false)
{
}

template <typename K, typename T>
state_hash_table<K, T> :: ~state_hash_table() throw ()
{
    po6::threads::mutex::hold hold(&m_mtx);
}

template <typename K, typename T>
T*
state_hash_table<K, T> :: create_state(const K& key, state_reference* sr)
{
    while (true)
    {
        e::intrusive_ptr<T> t = new T(key);
        sr->lock(this, t);
        std::pair<typename state_map_t::iterator, bool> inserted;

        {
            po6::threads::mutex::hold hold(&m_mtx);
            typename state_list_t::iterator it;
            it = m_state_list->insert(m_state_list->end(), t);
            inserted = m_state_map->insert(std::make_pair(t->state_key(), it));
        }

        if (!inserted.second)
        {
            sr->unlock();
            return NULL;
        }

        return t.get();
    }
}

template <typename K, typename T>
T*
state_hash_table<K, T> :: get_state(const K& key, state_reference* sr)
{
    while (true)
    {
        e::intrusive_ptr<T> t;

        {
            po6::threads::mutex::hold hold(&m_mtx);
            typename state_map_t::iterator it = m_state_map->find(key);

            if (it == m_state_map->end())
            {
                return NULL;
            }

            t = *it->second;
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

template <typename K, typename T>
T*
state_hash_table<K, T> :: get_or_create_state(const K& key, state_reference* sr)
{
    while (true)
    {
        e::intrusive_ptr<T> t;

        {
            po6::threads::mutex::hold hold(&m_mtx);
            typename state_map_t::iterator it = m_state_map->find(key);

            if (it == m_state_map->end())
            {
                t = new T(key);
                typename state_list_t::iterator itl;
                itl = m_state_list->insert(m_state_list->end(), t);
                std::pair<typename state_map_t::iterator, bool> inserted;
                inserted = m_state_map->insert(std::make_pair(t->state_key(), itl));
                assert(inserted.second);
            }
            else
            {
                t = *it->second;
            }
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

template <typename K, typename T>
state_hash_table<K, T> :: state_reference :: state_reference()
    : m_locked(false)
    , m_sht(NULL)
    , m_state()
{
}

template <typename K, typename T>
state_hash_table<K, T> :: state_reference :: ~state_reference() throw ()
{
    if (m_locked)
    {
        unlock();
    }
}

template <typename K, typename T>
void
state_hash_table<K, T> :: state_reference :: lock(state_hash_table* sht,
                                                  e::intrusive_ptr<T> state)
{
    assert(!m_locked);
    m_locked = true;
    m_sht = sht;
    m_state = state;
    m_state->lock();
}

template <typename K, typename T>
void
state_hash_table<K, T> :: state_reference :: unlock()
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
    }

    m_state->unlock();

    if (we_collect)
    {
        po6::threads::mutex::hold hold(&m_sht->m_mtx);
        typename state_map_t::iterator itm;
        itm = m_sht->m_state_map->find(m_state->state_key());
        typename state_list_t::iterator itl;
        bool erase_iterator = itm->second == m_sht->m_itl;
        itl = m_sht->m_state_list->erase(itm->second);

        if (erase_iterator)
        {
            m_sht->m_itl = itl;
            m_sht->m_itl_erased = true;
        }

        m_sht->m_state_map->erase(itm);
    }

    m_sht = NULL;
    m_state = NULL;
    m_locked = false;
}

template <typename K, typename T>
state_hash_table<K, T> :: iterator :: iterator(state_hash_table* sht)
    : m_sht(sht)
    , m_sr()
    , m_ptr(NULL)
    , m_primed(false)
    , m_valid(false)
{
    po6::threads::mutex::hold hold(&m_sht->m_mtx);
    assert(!m_sht->m_iterating);
    m_sht->m_iterating = true;
    m_sht->m_itl = m_sht->m_state_list->begin();
    m_sht->m_itl_erased = false;
}

template <typename K, typename T>
state_hash_table<K, T> :: iterator :: ~iterator() throw ()
{
    if (m_ptr)
    {
        m_sr.unlock();
    }

    po6::threads::mutex::hold hold(&m_sht->m_mtx);
    assert(m_sht->m_iterating);
    m_sht->m_iterating = false;
}

template <typename K, typename T>
T*
state_hash_table<K, T> :: iterator :: get()
{
    return m_ptr;
}

template <typename K, typename T>
bool
state_hash_table<K, T> :: iterator :: valid()
{
    if (!m_primed)
    {
        prime(false);
    }

    return m_valid;
}

template <typename K, typename T>
void
state_hash_table<K, T> :: iterator :: next()
{
    prime(true);
}

template <typename K, typename T>
void
state_hash_table<K, T> :: iterator :: prime(bool inc)
{
    m_primed = true;
    m_valid = false;

    if (m_ptr)
    {
        m_sr.unlock();
        m_ptr = NULL;
    }

    while (true)
    {
        e::intrusive_ptr<T> t;

        {
            po6::threads::mutex::hold hold(&m_sht->m_mtx);

            if (m_sht->m_itl == m_sht->m_state_list->end())
            {
                return;
            }

            if (inc && !m_sht->m_itl_erased)
            {
                ++(m_sht->m_itl);

                if (m_sht->m_itl == m_sht->m_state_list->end())
                {
                    return;
                }
            }

            inc = false;
            m_sht->m_itl_erased = false;
            t = *m_sht->m_itl;
        }

        m_sr.lock(m_sht, t);

        if (t->marked_garbage())
        {
            m_sr.unlock();
            inc = true;
            continue;
        }

        m_ptr = t.get();
        m_valid = true;
        return;
    }
}

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_state_hash_table_h_
