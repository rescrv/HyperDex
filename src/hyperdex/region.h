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

#ifndef hyperdex_region_h_
#define hyperdex_region_h_

// STL
#include <map>
#include <utility>
#include <vector>

// po6
#include <po6/pathname.h>
#include <po6/threads/rwlock.h>

// e
#include <e/intrusive_ptr.h>

// HyperDex
#include <hyperdex/disk.h>
#include <hyperdex/ids.h>
#include <hyperdex/log.h>
#include <hyperdex/result_t.h>

namespace hyperdex
{

class region
{
    public:
        region(const regionid& ri, const po6::pathname& directory, uint16_t nc);

    public:
        result_t get(const e::buffer& key, std::vector<e::buffer>* value,
                     uint64_t* version);
        result_t put(const e::buffer& key, const std::vector<e::buffer>& value,
                     uint64_t version);
        result_t del(const e::buffer& key);
        size_t flush();
        void async();
        void sync();

    private:
        friend class e::intrusive_ptr<region>;

    private:
        void get_value_hashes(const std::vector<e::buffer>& value, std::vector<uint64_t>* value_hashes);
        uint64_t get_point_for(uint64_t key_hash);
        uint64_t get_point_for(uint64_t key_hash, const std::vector<uint64_t>& value_hashes);
        void flush_one(op_t op, uint64_t point, const e::buffer& key,
                       uint64_t key_hash, const std::vector<e::buffer>& value,
                       const std::vector<uint64_t>& value_hashes, uint64_t version);

    private:
        size_t m_ref;
        size_t m_numcolumns;
        uint64_t m_point_mask;
        log m_log;
        po6::threads::rwlock m_rwlock;
        typedef std::vector<std::pair<regionid, e::intrusive_ptr<disk> > > disk_vector;
        disk_vector m_disks;
};

} // namespace hyperdex

#endif // hyperdex_region_h_
