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

#ifndef hyperdex_daemon_leveldb_h_
#define hyperdex_daemon_leveldb_h_

// e
#include <e/compat.h>

// LevelDB
#include <hyperleveldb/db.h>

// HyperDex
#include "namespace.h"

// Wrappers for leveldb's objects that we use off-the-stack.
//
// Motivation:  objects like snapshots and transfers keep persistent objects.
//      On shutdown, these objects need to be destroyed before the leveldb::DB
//      object.  This is easy to do in the normal case, but if anything goes
//      wrong, it'll very likely fail and cause a crash.
//
// Solution:  wrap each persistent object (iterator, snapshot, db) in a smart
//      pointer that also refers to all its dependencies.

BEGIN_HYPERDEX_NAMESPACE

typedef e::compat::shared_ptr<leveldb::DB> leveldb_db_ptr;

template<class T>
struct leveldb_dtor
{
    typedef void (leveldb::DB::*func)(T* t);
    static func get_func();
};

template <>
inline leveldb_dtor<const leveldb::Snapshot>::func
leveldb_dtor<const leveldb::Snapshot> :: get_func()
{
    return &leveldb::DB::ReleaseSnapshot;
}

template <>
inline leveldb_dtor<leveldb::ReplayIterator>::func
leveldb_dtor<leveldb::ReplayIterator> :: get_func()
{
    return &leveldb::DB::ReleaseReplayIterator;
}

template<class T>
class leveldb_release_ptr
{
    public:
        leveldb_release_ptr()
            : m_db(), m_resource() {}
        leveldb_release_ptr(leveldb_db_ptr d, T* t)
            : m_db(), m_resource() { reset(d, t); }
        leveldb_release_ptr(const leveldb_release_ptr& other)
            : m_db(other.m_db), m_resource(other.m_resource) {}
        ~leveldb_release_ptr() throw () {}

    public:
        T* get() const { return m_resource->ptr; }
        leveldb::DB* db() const { return m_db.get(); }
        void reset(leveldb_db_ptr d, T* t)
        { m_db = d; m_resource.reset(new wrapper(d, t)); }

    public:
        T* operator * () const throw () { return get(); }
        T* operator -> () const throw () { return get(); }
        leveldb_release_ptr& operator = (const leveldb_release_ptr& rhs)
        {
            if (this != &rhs)
            {
                m_resource = rhs.m_resource;
                m_db = rhs.m_db;
            }

            return *this;
        }

    private:
        struct wrapper
        {
            wrapper(leveldb_db_ptr d, T* t) : db(d), ptr(t) {}
            ~wrapper() throw ()
            {
                if (ptr)
                {
                    (*db.*leveldb_dtor<T>::get_func())(ptr);
                }
            }

            leveldb_db_ptr db;
            T* ptr;

            private:
                wrapper(const wrapper&);
                wrapper& operator = (const wrapper&);
        };

    private:
        leveldb_db_ptr m_db;
        e::compat::shared_ptr<wrapper> m_resource;
};

typedef leveldb_release_ptr<const leveldb::Snapshot> leveldb_snapshot_ptr;
typedef leveldb_release_ptr<leveldb::ReplayIterator> leveldb_replay_iterator_ptr;

class leveldb_iterator_ptr
{
    public:
        leveldb_iterator_ptr() : m_snap(), m_iter() {}
        ~leveldb_iterator_ptr() {}

    public:
        void reset(leveldb_snapshot_ptr s, leveldb::Iterator* iter)
        { m_iter.reset(iter); m_snap = s; }
        leveldb::Iterator* get() const { return m_iter.get(); }
        leveldb_snapshot_ptr snap() const { return m_snap; }

    public:
        leveldb::Iterator* operator -> () const throw () { return get(); }

    private:
        leveldb_snapshot_ptr m_snap;
        e::compat::shared_ptr<leveldb::Iterator> m_iter;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_leveldb_h_
