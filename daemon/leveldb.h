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

// STL
#include <tr1/functional>
#include <tr1/memory>

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

using std::tr1::placeholders::_1;

typedef std::tr1::shared_ptr<leveldb::DB> leveldb_db_ptr;

class leveldb_snapshot_ptr
{
    public:
        leveldb_snapshot_ptr() : m_db(), m_snap() {}
        leveldb_snapshot_ptr(leveldb_db_ptr d, const leveldb::Snapshot* snap)
            : m_db(d), m_snap()
        {
            std::tr1::function<void (const leveldb::Snapshot*)> dtor;
            dtor = std::tr1::bind(&leveldb::DB::ReleaseSnapshot, m_db, _1);
            std::tr1::shared_ptr<const leveldb::Snapshot> tmp(snap, dtor);
            m_snap = tmp;
        }
        leveldb_snapshot_ptr(const leveldb_snapshot_ptr& other)
            : m_db(other.m_db), m_snap(other.m_snap) {}
        ~leveldb_snapshot_ptr() throw () {}

    public:
        void reset(leveldb_db_ptr d, const leveldb::Snapshot* snap)
        {
            m_db = d;
            // keep m_db init above and m_snap init below
            std::tr1::function<void (const leveldb::Snapshot*)> dtor;
            dtor = std::tr1::bind(&leveldb::DB::ReleaseSnapshot, m_db.get(), _1);
            std::tr1::shared_ptr<const leveldb::Snapshot> tmp(snap, dtor);
            m_snap = tmp;
        }
        const leveldb::Snapshot* get() const { return m_snap.get(); }
        leveldb::DB* db() const { return m_db.get(); }

    public:
        leveldb_snapshot_ptr& operator = (const leveldb_snapshot_ptr& rhs)
        {
            if (this != &rhs)
            {
                m_db = rhs.m_db;
                m_snap = rhs.m_snap;
            }
            return *this;
        }

    private:
        leveldb_db_ptr m_db;
        std::tr1::shared_ptr<const leveldb::Snapshot> m_snap;
};

class leveldb_iterator_ptr
{
    public:
        leveldb_iterator_ptr() : m_snap(), m_iter() {}
        ~leveldb_iterator_ptr() throw () {}

    public:
        void reset(leveldb_snapshot_ptr s, leveldb::Iterator* iter)
        { m_snap = s; m_iter.reset(iter); }
        const leveldb::Iterator* get() const { return m_iter.get(); }
        leveldb_snapshot_ptr snap() const { return m_snap; }

    public:
        leveldb::Iterator* operator -> () const throw () { return m_iter.get(); }

    private:
        leveldb_snapshot_ptr m_snap;
        std::tr1::shared_ptr<leveldb::Iterator> m_iter;
};

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_leveldb_h_
