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

#ifndef hyperdex_disk_cuckoo_table_h_
#define hyperdex_disk_cuckoo_table_h_

// C
#include <stdint.h>

// STL
#include <vector>

namespace hyperdex
{

class cuckoo_table
{
    public:
        enum returncode
        {
            SUCCESS,
            NOT_FOUND,
            FULL
        };

    public:
        cuckoo_table(void* table);
        ~cuckoo_table() throw ();

    public:
        returncode insert(uint64_t key, uint64_t old_val, uint64_t new_val);
        returncode lookup(uint64_t key, std::vector<uint64_t>* vals);
        returncode remove(uint64_t key, uint64_t val);

    private:
        cuckoo_table(const cuckoo_table&);

    private:
        void get_entry1(uint64_t key, uint64_t val, uint32_t* entry);
        void get_entry2(uint64_t key, uint64_t val, uint32_t* entry);
        uint16_t get_index1(uint64_t key);
        uint16_t get_index2(uint64_t key);
        uint32_t* get_cache_line1(uint16_t idx);
        uint32_t* get_cache_line2(uint16_t idx);
        uint64_t get_key1(uint16_t idx, uint32_t* entry);
        uint64_t get_key2(uint16_t idx, uint32_t* entry);
        uint64_t get_val(uint32_t* entry);

    private:
        cuckoo_table& operator = (const cuckoo_table&);

    private:
        uint32_t* m_base;
        bool m_hash_table_full;
        uint32_t m_entry[3];
};

} // namespace hyperdex

#endif // hyperdex_disk_cuckoo_table_h_
