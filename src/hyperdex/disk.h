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

// HyperDex
#include <hyperdex/result_t.h>

namespace hyperdex
{

// This allocates a zero-filled 1GB + 4MB file at "filename".  The initial 4MB
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
        disk(const char* filename);
        ~disk() throw ();

    public:
        result_t get(const e::buffer& key, uint64_t key_hash,
                     std::vector<e::buffer>* value, uint64_t* version);
        result_t put(const e::buffer& key, uint64_t key_hash,
                     const std::vector<e::buffer>& value,
                     const std::vector<uint64_t>& value_hashes,
                     uint64_t version);
        result_t del(const e::buffer& key, uint64_t key_hash);
        void sync();

    private:
        disk(const disk&);

    private:
        // Find the bucket for the given key.  If the key is not already in the
        // hash table, then return the first dead bucket.  If no dead bucket was
        // found, return the empty bucket which was found.  Returns false if
        // there is no bucket available.
        bool find_bucket_for_key(const e::buffer& key, uint64_t key_hash,
                                 uint32_t** hash, uint32_t** offset);
        // This will invalidate any entry in the search index which references
        // the specified offset.
        void invalidate_search_index(uint32_t to_invalidate);

    private:
        disk& operator = (const disk&);

    private:
        char* m_base;
        uint32_t m_offset;
        uint32_t m_search;
};

void
zero_fill(const char* filename);

} // namespace hyperdex

#endif // hyperdex_disk_h_
