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

#define __STDC_LIMIT_MACROS

// C++
#include <iostream>

// STL
#include <vector>

// e
#include <e/atomic.h>

// HyperDex
#include "disk/search_tree.h"

using hyperdex::search_returncode;
using hyperdex::search_tree;

#define BLOCK_SIZE 4096
#define UINT64S_PER_BLOCK (BLOCK_SIZE / sizeof(uint64_t))
#define UINT64S_PER_ENTRY(A) (2 + A)
#define MASK_LEAF (1ULL << 63)
#define MASK_LIST (1ULL << 62)
#define MASK_BLOCK ((1ULL << 62) - 1)
#define MASK_TYPE (MASK_LEAF | MASK_LIST)

#ifdef STREEDEBUG
#define DEBUG std::cerr << __FILE__ << ":" << __LINE__ << " search_tree: "
#else
#define DEBUG if (0) std::cerr << __FILE__ << ":" << __LINE__ << " search_tree: "
#endif

struct search_tree::frame
{
    frame(uint64_t l, uint16_t i, uint64_t b) : level(l), idx(i), block_id(b) {}
    uint64_t level;
    uint16_t idx;
    uint64_t block_id;
};

search_tree :: search_tree(size_t attrs)
    : m_protect()
    , m_block_protect(1024)
    , m_attrs(attrs)
    , m_root()
    , m_heap(BLOCK_SIZE)
{
}

search_tree :: ~search_tree() throw ()
{
}

search_returncode
search_tree :: open(const char* /*prefix*/)
{
    // XXX
    po6::threads::spinlock::hold hold(&m_protect);
    void* data;
    heap_returncode hrc = m_heap.create(&m_root, &data);
    memset(data, 0xff, BLOCK_SIZE);

    switch (hrc)
    {
        case HEAP_SUCCESS:
            break;
        case HEAP_ALLOC_FAIL:
            return SEARCH_HEAP_OPEN_FAIL;
        default:
            abort();
    }

    m_root |= MASK_LEAF; // XXX
    return SEARCH_SUCCESS;
}

search_returncode
search_tree :: close()
{
    // XXX
    return SEARCH_SUCCESS;
}

search_returncode
search_tree :: insert(uint64_t log_id,
                      const uint64_t* hashes)
{
    DEBUG << "entering \"insert\"" << std::endl;

    while (true)
    {
        DEBUG << "top of the loop" << std::endl;
        uint64_t level = 0;
        uint64_t block_id = m_root;
        uint64_t* block = get_block(m_root);
        std::vector<frame> fs;

        while (get_type(block_id) == BLOCK_INTERNAL)
        {
            DEBUG << "internal node " << block_id << std::endl;
            uint16_t idx = get_index(hashes, level);
            fs.push_back(frame(level, idx, block_id));
            ++level;
            block_id = block[idx];

            if (block_id == UINT64_MAX)
            {
                break;
            }

            block = get_block(block_id);
        }

        heap::recycler r(&m_heap);
        heap_returncode hrc;

        DEBUG << "traversed " << level << " levels deep" << std::endl;

        if (block_id == UINT64_MAX)
        {
            uint64_t new_block_id;
            void* new_data;

            hrc = r.create(&new_block_id, &new_data);

            switch (hrc)
            {
                case HEAP_SUCCESS:
                    break;
                case HEAP_ALLOC_FAIL:
                    return SEARCH_HEAP_ALLOC_FAIL;
                default:
                    abort();
            }

            new_block_id |= MASK_LEAF;
            uint64_t* new_block = static_cast<uint64_t*>(new_data);
            memset(new_block, 0xff, BLOCK_SIZE);
            store_entry(new_block, log_id, hashes);
            search_returncode src;

            if (zipper_tree(fs, &r, UINT64_MAX, new_block_id, &src))
            {
                // zipper_tree will dismiss r if need be
                return src;
            }

            // otherwise try again
        }
        else
        {
            e::striped_lock<po6::threads::mutex>::hold hold(&m_block_protect, block_id);
            block_t type = get_type(block_id);
            assert(type == BLOCK_LEAF || type == BLOCK_LIST);

            size_t dead_entries = 0;
            uint64_t* ptr = block;
            uint64_t* end = block + UINT64S_PER_BLOCK - (type == BLOCK_LIST ? 1 : 0);

            for (; ptr + UINT64S_PER_ENTRY(m_attrs) <= end;
                    ptr += UINT64S_PER_ENTRY(m_attrs))
            {
                if (ptr[0] == UINT64_MAX)
                {
                    DEBUG << "adding entry " << log_id << " to block " << block_id
                          << " at offset " << ptr - block << std::endl;
                    store_entry(ptr, log_id, hashes);
                    r.dismiss();
                    return SEARCH_SUCCESS;
                }
                else if (ptr[1] != UINT64_MAX)
                {
                    ++dead_entries;
                }
            }

            if (dead_entries > 0)
            {
                DEBUG << "block " << block_id << " has dead entries" << std::endl;
                uint64_t new_block_id;
                void* new_data;

                hrc = r.create(&new_block_id, &new_data);

                switch (hrc)
                {
                    case HEAP_SUCCESS:
                        break;
                    case HEAP_ALLOC_FAIL:
                        return SEARCH_HEAP_ALLOC_FAIL;
                    default:
                        abort();
                }

                new_block_id |= block_id & MASK_TYPE;
                uint64_t* new_block = static_cast<uint64_t*>(new_data);
                memset(new_block, 0xff, BLOCK_SIZE);
                ptr = copy_live_entries(new_block, block, end);
                store_entry(ptr, log_id, hashes);

                if (type == BLOCK_LIST)
                {
                    new_block[UINT64S_PER_BLOCK - 1] = block[UINT64S_PER_BLOCK - 1];
                }

                search_returncode src;

                if (zipper_tree(fs, &r, block_id, new_block_id, &src))
                {
                    // zipper_tree will dismiss r if need be
                    return src;
                }
            }
            else
            {
                search_returncode src;

                if (expand(fs, &r, block_id, block, &src))
                {
                    // expand will dismiss r if need be
                    return src;
                }
            }
        }
    }

    DEBUG << "reached unreachable point; mind blown!" << std::endl;
    abort();
}

uint64_t*
search_tree :: get_block(uint64_t block)
{
    void* data;
    block = block & MASK_BLOCK;

    switch (m_heap.get(block, &data))
    {
        case HEAP_SUCCESS:
            return static_cast<uint64_t*>(data);
        case HEAP_ALLOC_FAIL:
        default:
            abort();
    }
}

uint16_t
search_tree :: get_index(const uint64_t* hashes, uint64_t level)
{
    return 0; // XXX
}

search_tree::block_t
search_tree :: get_type(uint64_t block)
{
    if ((block & MASK_LEAF))
    {
        return BLOCK_LEAF;
    }

    if ((block & MASK_LIST))
    {
        return BLOCK_LIST;
    }

    return BLOCK_INTERNAL;
}

void
search_tree :: store_entry(uint64_t* ptr,
                           uint64_t log_id,
                           const uint64_t* hashes)
{
    for (size_t i = 0; i < m_attrs; ++i)
    {
        ptr[2 + i] = hashes[i];
    }

    ptr[1] = UINT64_MAX;
    e::atomic::store_64_release(ptr, log_id);
}

void
search_tree :: copy_entry(uint64_t* to, uint64_t* from)
{
    for (size_t i = 0; i < m_attrs; ++i)
    {
        to[2 + i] = from[2 + i];
    }

    to[1] = from[1];
    e::atomic::store_64_release(to, *from);
}

uint64_t*
search_tree :: copy_live_entries(uint64_t* new_block,
                                 uint64_t* old_block,
                                 uint64_t* old_end)
{
    uint64_t* old_ptr = old_block;
    uint64_t* new_ptr = new_block;

    for (; old_ptr + UINT64S_PER_ENTRY(m_attrs) <= old_end;
            old_ptr += UINT64S_PER_ENTRY(m_attrs))
    {
        if (old_ptr[0] != UINT64_MAX && old_ptr[1] == UINT64_MAX)
        {
            copy_entry(new_ptr, old_ptr);
            new_ptr += UINT64S_PER_ENTRY(m_attrs);
        }
    }

    return new_ptr;
}

bool
search_tree :: zipper_tree(const std::vector<frame>& fs,
                           heap::recycler* r,
                           uint64_t old_block_id,
                           uint64_t new_block_id,
                           search_returncode* src)
{
    DEBUG << "zippering the tree to integrate " << new_block_id << std::endl;
    std::vector<std::pair<uint64_t, uint64_t*> > path(fs.size(), std::make_pair<uint64_t, uint64_t*>(UINT64_MAX, NULL));

    // Allocate a new path to the root
    for (size_t i = 0; i < fs.size(); ++i)
    {
        uint64_t bid;
        void* d;
        heap_returncode hrc = r->create(&bid, &d);
        DEBUG << "allocating new block " << bid << std::endl;

        switch (hrc)
        {
            case HEAP_SUCCESS:
                break;
            case HEAP_ALLOC_FAIL:
                *src = SEARCH_HEAP_ALLOC_FAIL;
                return true;
            default:
                abort();
        }

        uint64_t* b = static_cast<uint64_t*>(d);
        memset(b, 0xff, BLOCK_SIZE);
        path[i] = std::make_pair(bid, b);
    }

    if (fs.empty())
    {
        DEBUG << "replacing the root immediately" << std::endl;
        *src = SEARCH_SUCCESS;

        if (cas_root(old_block_id, new_block_id))
        {
            r->dismiss();
            return true;
        }

        return false;
    }

    while (true)
    {
        uint64_t old_root = m_root;
        uint64_t block_id = old_root;
        uint64_t* block = get_block(block_id);
        DEBUG << "attempting to zipper the tree" << std::endl;

        for (size_t level = 0; level < fs.size(); ++level)
        {
            if (block_id == UINT64_MAX)
            {
                return false;
            }

            if (get_type(block_id) != BLOCK_INTERNAL)
            {
                return false;
            }

            DEBUG << "considering replacing " << block_id << " with "
                  << path[level].first << std::endl;
            block = get_block(block_id);
            memmove(path[level].second, block, BLOCK_SIZE);
            block_id = block[fs[level].idx];
        }

        if (block[fs.back().idx] != old_block_id)
        {
            DEBUG << "did not zipper because the tree height changed" << std::endl;
            return false;
        }

        for (size_t i = 0; i < fs.size() - 1; ++i)
        {
            path[i].second[fs[i].idx] = path[i + 1].first;
        }

        path.back().second[fs.back().idx] = new_block_id;

        DEBUG << "trying to CAS the root" << std::endl;

        if (cas_root(old_root, path[0].first))
        {
            *src = SEARCH_SUCCESS;
            r->dismiss();
            return true;
        }

        DEBUG << "the CAS failed" << std::endl;
    }
}

bool
search_tree :: expand(const std::vector<frame>& fs,
                      heap::recycler* r,
                      uint64_t block_id,
                      uint64_t* block,
                      search_returncode* src)
{
    DEBUG << "expanding " << block_id << std::endl;
    uint64_t new_block_id;
    void* new_data;
    heap_returncode hrc = r->create(&new_block_id, &new_data);

    switch (hrc)
    {
        case HEAP_SUCCESS:
            break;
        case HEAP_ALLOC_FAIL:
            return SEARCH_HEAP_ALLOC_FAIL;
        default:
            abort();
    }

    uint64_t* new_block = static_cast<uint64_t*>(new_data);
    memset(new_block, 0xff, BLOCK_SIZE);

    // We need to start making lists so we just point to the existing node
    if (fs.size() * 14 > m_attrs * 64)
    {
        new_block_id |= MASK_LIST;
        new_block[UINT64S_PER_BLOCK - 1] = block_id;
        DEBUG << "creating list node " << new_block_id << std::endl;
    }
    // We need to make another internal node
    else
    {
        uint64_t* ptr = block;
        uint64_t* end = block + UINT64S_PER_BLOCK - (get_type(block_id) == BLOCK_LIST ? 1 : 0);
        std::vector<uint64_t*> ptrs(UINT64S_PER_BLOCK);
        DEBUG << "creating internal node " << new_block_id << std::endl;

        for (; ptr + UINT64S_PER_ENTRY(m_attrs) <= end;
                ptr += UINT64S_PER_ENTRY(m_attrs))
        {
            uint16_t idx = get_index(ptr + 2, fs.size());
            uint64_t child_block_id = new_block[idx];
            uint64_t* child_block;

            if (child_block_id == UINT64_MAX)
            {
                void* child_data;
                hrc = r->create(&child_block_id, &child_data);

                switch (hrc)
                {
                    case HEAP_SUCCESS:
                        break;
                    case HEAP_ALLOC_FAIL:
                        return SEARCH_HEAP_ALLOC_FAIL;
                    default:
                        abort();
                }

                child_block_id |= MASK_LEAF;
                new_block[idx] = child_block_id;
                child_block = static_cast<uint64_t*>(child_data);
                memset(child_block, 0xff, BLOCK_SIZE);
                ptrs[idx] = child_block;
            }
            else
            {
                child_block = get_block(child_block_id);
            }

            uint64_t* child_ptr = ptrs[idx];
            uint64_t* child_end = child_block + UINT64S_PER_BLOCK;

            for (; child_ptr + UINT64S_PER_ENTRY(m_attrs) <= child_end;
                    child_ptr += UINT64S_PER_ENTRY(m_attrs))
            {
                if (child_ptr[0] == UINT64_MAX)
                {
                    DEBUG << "copying entry " << ptr[0] << " to block " << child_block_id
                          << " at offset " << child_ptr - child_block << std::endl;
                    copy_entry(child_ptr, ptr);
                    break;
                }
            }

            ptrs[idx] = child_ptr;
        }
    }

    // Here we want to zipper the tree, but we only want to return src if the
    // zippering fails.  Otherwise, we silently expand and let it go again.
    bool zippered = zipper_tree(fs, r, block_id, new_block_id, src);

    if (zippered && *src != SEARCH_SUCCESS)
    {
        return true;
    }

    return false;
}

bool
search_tree :: cas_root(uint64_t old_root, uint64_t new_root)
{
    if (m_root == old_root)
    {
        DEBUG << "succeeded in replacing root " << old_root << " with " << new_root << std::endl;
        m_root = new_root;
        return true;
    }

    DEBUG << "failed to replace root " << old_root << " with " << new_root << std::endl;
    return false;
}
