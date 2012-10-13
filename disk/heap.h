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

#ifndef hyperdex_disk_heap_h_
#define hyperdex_disk_heap_h_

// C
#include <stdint.h>

// STL
#include <vector>

// e
#include <e/intrusive_ptr.h>

// HyperDex
#include "disk/heap_returncode.h"

namespace hyperdex
{

class heap
{
    public:
        class recycler;

    public:
        heap(size_t block_sz);
        ~heap() throw ();

    public:
        heap_returncode open(const char* prefix);
        heap_returncode close();

        heap_returncode create(uint64_t* block, void** data);
        heap_returncode get(uint64_t block, void** data);
        heap_returncode recycle(uint64_t block);

    private:
        size_t m_block_sz;
};

// Recycle all blocks returned by "create" unless dismiss is called.
class heap::recycler
{
    public:
        recycler(heap* h);
        ~recycler() throw ();

    public:
        heap_returncode create(uint64_t* block, void** data);
        heap_returncode dismiss();

    private:
        recycler(const recycler&);
        recycler& operator = (const recycler&);

    private:
        heap* m_heap;
        std::vector<uint64_t> m_blocks;
};

} // namespace hyperdex

#endif // hyperdex_disk_heap_h_
