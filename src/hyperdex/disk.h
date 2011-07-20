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

#ifndef hyperdex_disk_h_
#define hyperdex_disk_h_

// po6
#include <po6/pathname.h>

// e
#include <e/buffer.h>
#include <e/intrusive_ptr.h>

// HyperDex
#include <hyperdex/result_t.h>
#include <hyperdex/op_t.h>

namespace hyperdex
{

// This allocates a zero-filled 256MB + 5MB file at "filename".  The initial 5MB
// is used for a hash table and a log-style index which index the disk.  The
// first hash table indexes solely by the key, while the second is an
// append-only log used for answering searches in a history-preserving way.
//
// Synchronization:  Any number of threads may be in the "GET" method
// simulatenously, but only one may use PUT or DEL at a time (it may be
// concurrent with GET).

class disk
{
    public:
        class snapshot
        {
            public:
                bool valid();
                void next();

            public:
                uint32_t secondary_point();
                uint64_t version();
                e::buffer key();
                std::vector<e::buffer> value();

            private:
                friend class e::intrusive_ptr<snapshot>;
                friend class disk;

            private:
                snapshot(e::intrusive_ptr<disk> d);

            private:
                size_t m_ref;
                bool m_valid;
                e::intrusive_ptr<disk> m_disk;
                const uint32_t m_limit;
                uint32_t m_entry;
        };

    public:
        // Create will create a newly initialized disk at the given filename,
        // even if it already exists.  That is, it will overwrite the existing
        // disk (or other file) at "filename".
        static e::intrusive_ptr<disk> create(const po6::pathname& filename);

    public:
        result_t get(const e::buffer& key, uint64_t key_hash,
                     std::vector<e::buffer>* value, uint64_t* version);
        result_t put(const e::buffer& key, uint64_t key_hash,
                     const std::vector<e::buffer>& value,
                     const std::vector<uint64_t>& value_hashes,
                     uint64_t version);
        result_t del(const e::buffer& key, uint64_t key_hash);
        void async();
        void sync();
        void drop();
        e::intrusive_ptr<snapshot> make_snapshot();

    private:
        static const size_t HASH_TABLE_ENTRIES = 262144;
        static const size_t HASH_TABLE_SIZE = HASH_TABLE_ENTRIES * 8;
        static const size_t SEARCH_INDEX_ENTRIES = HASH_TABLE_ENTRIES;
        static const size_t SEARCH_INDEX_SIZE = SEARCH_INDEX_ENTRIES * 12;
        static const size_t INDEX_SEGMENT_SIZE = HASH_TABLE_SIZE + SEARCH_INDEX_SIZE;
        static const size_t DATA_SEGMENT_SIZE = HASH_TABLE_ENTRIES * 1024;
        static const size_t TOTAL_FILE_SIZE = INDEX_SEGMENT_SIZE + DATA_SEGMENT_SIZE;

    private:
        friend class e::intrusive_ptr<disk>;
        friend class snapshot;

    private:
        disk(po6::io::fd* fd, const po6::pathname& filename);
        disk(const disk&);
        ~disk() throw ();

    private:
        uint32_t* hashtable_base(size_t entry) const
        {
            uint32_t offset = entry * 8;
            assert(offset < HASH_TABLE_SIZE);
            return reinterpret_cast<uint32_t*>(m_base + offset);
        }

        uint32_t hashtable_hash(size_t entry) const
        { return be32toh(hashtable_base(entry)[0]); }
        uint32_t hashtable_offset(size_t entry) const
        { return be32toh(hashtable_base(entry)[1]); }

        uint32_t* searchindex_base(size_t entry) const
        {
            uint32_t offset = HASH_TABLE_SIZE + entry * 12;
            assert(HASH_TABLE_SIZE <= offset && offset < INDEX_SEGMENT_SIZE);
            return reinterpret_cast<uint32_t*>(m_base + offset);
        }
        uint32_t searchindex_hash(size_t entry) const
        { return be32toh(searchindex_base(entry)[0]); }
        uint32_t searchindex_offset(size_t entry) const
        { return be32toh(searchindex_base(entry)[1]); }
        uint32_t searchindex_invalid(size_t entry) const
        { return be32toh(searchindex_base(entry)[2]); }

    private:
        void hashtable_hash(size_t entry, uint32_t value)
        { hashtable_base(entry)[0] = htobe32(value); }
        void hashtable_offset(size_t entry, uint32_t value)
        { hashtable_base(entry)[1] = htobe32(value); }
        void searchindex_hash(size_t entry, uint32_t value)
        { searchindex_base(entry)[0] = htobe32(value); }
        void searchindex_offset(size_t entry, uint32_t value)
        { searchindex_base(entry)[1] = htobe32(value); }
        void searchindex_invalid(size_t entry, uint32_t value)
        { searchindex_base(entry)[2] = htobe32(value); }

    private:
        size_t data_size(const e::buffer& key, const std::vector<e::buffer>& value) const;
        uint64_t data_version(uint32_t offset) const;
        size_t data_key_size(uint32_t offset) const;
        size_t data_key_offset(uint32_t offset) const
        { return offset + sizeof(uint64_t) + sizeof(uint32_t); }
        void data_key(uint32_t offset, size_t keysize, e::buffer* key) const;
        void data_value(uint32_t offset, size_t keysize, std::vector<e::buffer>* value) const;

    private:
        // Find the bucket for the given key.  If the key is already in the
        // table, then bucket will be stored in entry (and hence, the hash and
        // offset of that bucket will be from the older version of the key).  If
        // the key is not in the table, then a dead (deleted) or empty (never
        // used) bucket will be stored in entry.  If no bucket is available,
        // HASH_TABLE_ENTRIES will be stored in entry.  If a non-dead bucket was
        // found (as in, old/new keys match), true is returned; else, false is
        // returned.
        bool find_bucket(const e::buffer& key, uint64_t key_hash, size_t* entry);
        // This will invalidate any entry in the search index which references
        // the specified offset.
        void invalidate_search_index(uint32_t to_invalidate, uint32_t invalidate_with);

    private:
        disk& operator = (const disk&);

    private:
        size_t m_ref;
        char* m_base;
        uint32_t m_offset;
        uint32_t m_search;
        po6::pathname m_filename;
};

} // namespace hyperdex

#endif // hyperdex_disk_h_
