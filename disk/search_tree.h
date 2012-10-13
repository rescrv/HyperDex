// Copyright (c) 2012, Cornell University
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

#ifndef hyperdex_disk_search_tree_h_
#define hyperdex_disk_search_tree_h_

// C
#include <cstdlib>
#include <stdint.h>

// po6
#include <po6/threads/mutex.h>
#include <po6/threads/spinlock.h>

// e
#include <e/striped_lock.h>

// HyperDex
#include "disk/heap.h"
#include "disk/search_returncode.h"
#include "disk/search_snapshot.h"

namespace hyperdex
{

class search_tree
{
    public:
        class iterator;

    public:
        search_tree(size_t attrs);
        ~search_tree() throw ();

    public:
        search_returncode open(const char* prefix);
        search_returncode close();

    public:
        search_returncode insert(uint64_t log_id,
                                 const uint64_t* hashes);
        search_returncode remove(uint64_t create_id,
                                 const uint64_t* hashes,
                                 uint64_t remove_id);
        search_returncode make_iterator(const uint64_t* hashes,
                                        iterator* it);

    private:
        class frame;
        enum block_t { BLOCK_LEAF, BLOCK_INTERNAL, BLOCK_LIST };

    private:
        uint64_t* get_block(uint64_t block);
        uint16_t get_index(const uint64_t* hashes, uint64_t level);
        block_t get_type(uint64_t block);
        void store_entry(uint64_t* ptr, uint64_t log_id, const uint64_t* hashes);
        void copy_entry(uint64_t* to, uint64_t* from);
        uint64_t* copy_live_entries(uint64_t* new_block, uint64_t* old_block, uint64_t* old_end);
        // Returns true if src must be returned.  This happens either when the
        // root is replaced or when a fatal error occurs.
        bool zipper_tree(const std::vector<frame>& fs,
                         heap::recycler* r,
                         uint64_t old_block_id,
                         uint64_t new_block_id,
                         search_returncode* src);
        // Returns true if src must be returned.  This happens *only* when a
        // fatal error occurs.
        bool expand(const std::vector<frame>& fs,
                    heap::recycler* r,
                    uint64_t block_id,
                    uint64_t* block,
                    search_returncode* src);
        // Swap from one to the other.
        bool cas_root(uint64_t old_root, uint64_t new_root);

    private:
        po6::threads::spinlock m_protect;
        e::striped_lock<po6::threads::mutex> m_block_protect;
        size_t m_attrs;
        uint64_t m_root;
        heap m_heap;
};

class search_tree::iterator
{
    public:
        iterator();
        ~iterator() throw ();

    public:
        bool has_next();
        void next();

    private:
        iterator(const iterator&);
        iterator& operator = (const iterator&);
};

} // namespace hyperdex

#endif // hyperdex_disk_search_tree_h_
