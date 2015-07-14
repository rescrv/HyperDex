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

#ifndef hyperdex_daemon_datalayer_iterator_h_
#define hyperdex_daemon_datalayer_iterator_h_

// e
#include <e/intrusive_ptr.h>

// HyperDex
#include "namespace.h"
#include "daemon/datalayer.h"
#include "daemon/index_info.h"

BEGIN_HYPERDEX_NAMESPACE

class datalayer::iterator
{
    public:
        iterator(leveldb_snapshot_ptr snap);

    public:
        virtual bool valid() = 0;
        // REQUIRES: valid
        virtual void next() = 0;
        virtual uint64_t cost(leveldb::DB*) = 0;
        // REQUIRES: valid
        virtual e::slice key() = 0;
        virtual std::ostream& describe(std::ostream&) const = 0;

    public:
        leveldb_snapshot_ptr snap();

    protected:
        friend class e::intrusive_ptr<iterator>;
        virtual ~iterator() throw ();
        void inc() { ++m_ref; }
        void dec() { --m_ref; if (m_ref == 0) delete this; }
        size_t m_ref;

    private:
        leveldb_snapshot_ptr m_snap;
};

class datalayer::replay_iterator
{
    public:
        replay_iterator(const region_id& ri, leveldb_replay_iterator_ptr ptr, const index_encoding* ie);

    public:
        bool valid();
        void next();
        bool has_value();
        e::slice key();
        returncode unpack_value(std::vector<e::slice>* value,
                                uint64_t* version,
                                reference* ref);
        leveldb::Status status();

    private:
        region_id m_ri;
        leveldb::ReplayIterator* m_iter;
        leveldb_replay_iterator_ptr m_ptr;
        std::vector<char> m_decoded;
        const index_encoding *const m_ie;

    private:
        replay_iterator(const replay_iterator&);
        replay_iterator& operator = (const replay_iterator&);
};

class datalayer::dummy_iterator : public iterator
{
    public:
        dummy_iterator();

    public:
        virtual bool valid();
        virtual void next();
        virtual uint64_t cost(leveldb::DB*);
        virtual e::slice key();
        virtual std::ostream& describe(std::ostream&) const;

    protected:
        virtual ~dummy_iterator() throw ();
};

class datalayer::region_iterator : public iterator
{
    public:
        region_iterator(leveldb_iterator_ptr iter,
                        const region_id& ri,
                        const index_encoding* ie);
        virtual ~region_iterator() throw ();

    public:
        virtual bool valid();
        virtual void next();
        virtual uint64_t cost(leveldb::DB*);
        virtual e::slice key();
        virtual std::ostream& describe(std::ostream&) const;

    private:
        region_iterator(const region_iterator&);
        region_iterator& operator = (const region_iterator&);

    private:
        leveldb_iterator_ptr m_iter;
        region_id m_ri;
        std::vector<char> m_decoded;
        const index_encoding *const m_ie;
};

class datalayer::index_iterator : public iterator
{
    public:
        index_iterator(leveldb_snapshot_ptr snap);
        virtual ~index_iterator() throw ();

    public:
        virtual e::slice internal_key() = 0;
        virtual bool sorted() = 0;
        virtual void seek(const e::slice& internal_key) = 0;

    protected:
        friend class e::intrusive_ptr<index_iterator>;
};

class datalayer::range_index_iterator : public index_iterator
{
    public:
        range_index_iterator(leveldb_snapshot_ptr snap,
                             size_t range_prefix_sz,
                             const e::slice& range_lower,
                             const e::slice& range_upper,
                             bool has_value_lower,
                             bool has_value_upper,
                             const index_encoding* val_ie,
                             const index_encoding* key_ie);
        virtual ~range_index_iterator() throw ();

    public:
        virtual bool valid();
        virtual void next();
        virtual uint64_t cost(leveldb::DB*);
        virtual e::slice key();
        virtual std::ostream& describe(std::ostream&) const;
        virtual e::slice internal_key();
        virtual bool sorted();
        virtual void seek(const e::slice& internal_key);

    private:
        bool decode_entry(const e::slice& in, e::slice* val, e::slice* key);
        bool decode_entry_keyless(const e::slice& in, e::slice* val);
        void encode_entry(const e::slice& val,
                          const e::slice& key,
                          std::vector<char>* scratch,
                          e::slice* slice);

    private:
        range_index_iterator(const range_index_iterator&);
        range_index_iterator& operator = (const range_index_iterator&);

    private:
        leveldb_iterator_ptr m_iter;
        const index_encoding *const m_val_ie;
        const index_encoding *const m_key_ie;
        e::slice m_prefix;
        e::slice m_range_lower;
        e::slice m_range_upper;
        e::slice m_value_lower;
        e::slice m_value_upper;
        std::vector<char> m_range_buf;
        std::vector<char> m_scratch;
        bool m_has_lower;
        bool m_has_upper;
        bool m_invalid;
};

class datalayer::intersect_iterator : public index_iterator
{
    public:
        intersect_iterator(leveldb_snapshot_ptr snap,
                           const std::vector<e::intrusive_ptr<index_iterator> >& iterators);
        virtual ~intersect_iterator() throw ();

    public:
        virtual bool valid();
        virtual void next();
        virtual uint64_t cost(leveldb::DB*);
        virtual e::slice key();
        virtual std::ostream& describe(std::ostream&) const;
        virtual e::slice internal_key();
        virtual bool sorted();
        virtual void seek(const e::slice& internal_key);

    private:
        std::vector<e::intrusive_ptr<index_iterator> > m_iters;
        uint64_t m_cost;
        bool m_invalid;
};

class datalayer::search_iterator : public iterator
{
    public:
        search_iterator(datalayer* dl,
                        const region_id& ri,
                        e::intrusive_ptr<index_iterator> iter,
                        std::ostringstream* ostr,
                        const std::vector<attribute_check>* checks);
        virtual ~search_iterator() throw ();

    public:
        virtual bool valid();
        virtual void next();
        virtual uint64_t cost(leveldb::DB*);
        virtual e::slice key();
        virtual std::ostream& describe(std::ostream&) const;

    private:
        search_iterator(const search_iterator&);
        search_iterator& operator = (const search_iterator&);

    private:
        datalayer* m_dl;
        region_id m_ri;
        e::intrusive_ptr<index_iterator> m_iter;
        returncode m_error;
        std::ostringstream* m_ostr;
        uint64_t m_num_gets;
        const std::vector<attribute_check>* m_checks;
};

inline std::ostream&
operator << (std::ostream& lhs, const datalayer::iterator& rhs)
{
    return rhs.describe(lhs);
}

END_HYPERDEX_NAMESPACE

#endif // hyperdex_daemon_datalayer_iterator_h_
