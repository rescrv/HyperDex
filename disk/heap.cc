// Copyright (c) 2012, Robert Escriva
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

// C
#include <cassert>

// POSIX
#include <sys/mman.h>

// HyperDex
#include "disk/heap.h"

using hyperdex::heap;
using hyperdex::heap_returncode;

heap :: heap(size_t block_sz)
    : m_block_sz(block_sz)
{
    size_t page_sz = sysconf(_SC_PAGESIZE);
    assert((m_block_sz / page_sz) * page_sz == m_block_sz);
    assert(sizeof(void*) == sizeof(uint64_t));
}

heap :: ~heap() throw ()
{
}

heap_returncode
heap :: create(uint64_t* block, void** data)
{
    void* x = mmap(NULL, m_block_sz, PROT_READ|PROT_WRITE, MAP_PRIVATE|MAP_ANONYMOUS|MAP_POPULATE, -1, 0);

    if (x == MAP_FAILED)
    {
        return HEAP_ALLOC_FAIL;
    }

    uint64_t b = reinterpret_cast<uint64_t>(x);
    assert(b == (b & ((1ULL << 62) - 1)));
    *block = b;
    *data = x;
    return HEAP_SUCCESS;
}

heap_returncode
heap :: get(uint64_t block, void** data)
{
    *data = reinterpret_cast<void*>(block);
    return HEAP_SUCCESS;
}

heap_returncode
heap :: recycle(uint64_t block)
{
    void* x = reinterpret_cast<void*>(block);
    munmap(x, m_block_sz); // XXX errno
    return HEAP_SUCCESS;
}

heap :: recycler :: recycler(heap* h)
    : m_heap(h)
    , m_blocks()
{
}

heap :: recycler :: ~recycler() throw ()
{
    for (size_t i = 0; i < m_blocks.size(); ++i)
    {
        m_heap->recycle(m_blocks[i]);
    }
}

heap_returncode
heap :: recycler :: create(uint64_t* block, void** data)
{
    heap_returncode rc = m_heap->create(block, data);

    if (rc == HEAP_SUCCESS)
    {
        m_blocks.push_back(*block);
    }

    return rc;
}

heap_returncode
heap :: recycler :: dismiss()
{
    m_blocks.clear();
    return HEAP_SUCCESS;
}
