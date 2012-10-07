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

#ifndef hyperdex_disk_cuckoo_index_h_
#define hyperdex_disk_cuckoo_index_h_

// C
#include <stdint.h>

// STL
#include <vector>

// po6
#include <po6/threads/spinlock.h>

// e
#include <e/intrusive_ptr.h>
#include <e/striped_lock.h>

// HyperDex
#include "disk/cuckoo_returncode.h"

namespace hyperdex
{

class cuckoo_index
{
    public:
        cuckoo_index();
        ~cuckoo_index() throw ();

    public:
        cuckoo_returncode close(const char* path);

    public:
        cuckoo_returncode insert(uint64_t key, uint64_t val);
        cuckoo_returncode lookup(uint64_t key, std::vector<uint64_t>* vals);
        cuckoo_returncode remove(uint64_t key, uint64_t val);

    private:
        class table_list;
        class table_info;
        class table_base;
        typedef e::intrusive_ptr<table_list> table_list_ptr;

    private:
        table_list_ptr m_tables;
        po6::threads::spinlock m_tables_lock;
        e::striped_lock<po6::threads::spinlock> m_table_locks;
};

} // namespace hyperdex

#endif // hyperdex_disk_cuckoo_index_h_
